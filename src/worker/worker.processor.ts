import { Injectable, Logger } from '@nestjs/common';
import { Processor, Process, InjectQueue } from '@nestjs/bull';
import { Job, Queue } from 'bull';
import { PlayurlService } from '../playurl/playurl.service';
import { FfmpegService } from '../ffmpeg/ffmpeg.service';
import { JobHistoryService } from '../jobs/job-history.service';
import * as fs from 'fs';
import * as path from 'path';
import resumeDownload, { ResumeDownloadResult } from '../utils/resume-download';
import axios from 'axios';

const DATA_DIR = path.resolve(process.cwd(), 'data');
const PART_SIZE_BYTES = 8 * 1024 * 1024;

@Processor('downloads')
@Injectable()
export class WorkerProcessor {
  private readonly logger = new Logger(WorkerProcessor.name);

  constructor(
    private readonly playurl: PlayurlService,
    private readonly ffmpeg: FfmpegService,
    private readonly history: JobHistoryService,
    @InjectQueue('downloads') private readonly downloadQueue?: Queue,
    @InjectQueue('download-parts') private readonly partsQueue?: Queue,
  ) {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  @Process()
  async handleJob(job: Job) {
    this.logger.log('Handling job ' + job.id);
    const payload = job.data || {};
    const bvid = payload.bvid;
    if (!bvid) throw new Error('missing bvid');
    const cookies = payload.cookies;
    try {
      await job.progress(5);
      await this.history.appendEvent(job.id.toString(), { state: 'queued', progress: 5 });
      const cid = await this.playurl.getCidFromBvid(bvid);
      if (!cid) throw new Error('cannot get cid');
      await job.progress(10);
      await this.history.appendEvent(job.id.toString(), { state: 'resolving', progress: 10 });
      const play = await this.playurl.getPlayurl(bvid, cid, cookies);
      this.logger.log('Playurl response code: ' + play?.code);
      if (play?.code !== 0) throw new Error(`playurl failed: ${play?.message || 'unknown'}`);

      // Fetch and sanitize video title for final output path/manifest.
      const title = (await this.fetchVideoTitle(bvid, cookies)) || bvid;
      const safeTitle = this.sanitizeTitle(title);
      const manifestDir = path.join(DATA_DIR, String(job.id));
      const manifestPath = path.join(manifestDir, 'manifest.json');
      if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true });

      const cancelFile = path.join(DATA_DIR, `${job.id}.cancel`);
      const stopFile = path.join(DATA_DIR, `${job.id}.stop`);
      const stopKey = `job:stop:${job.id}`;
      let stopRequested = false;
      let _poll: any = null;
      let subClient: any = null;
      const redisClient: any = (this.downloadQueue && (this.downloadQueue as any).client) ? (this.downloadQueue as any).client : null;
      if (redisClient) {
        try {
          // Prefer pub/sub: create a dedicated subscriber if possible
          if (typeof redisClient.duplicate === 'function') {
            try {
              subClient = redisClient.duplicate();
              if (typeof subClient.connect === 'function') await subClient.connect();
              // node-redis v4: subscribe with callback
              if (typeof subClient.subscribe === 'function') {
                await subClient.subscribe(stopKey, (message: any) => {
                  if (message) { stopRequested = true; }
                });
              } else if (typeof subClient.on === 'function') {
                subClient.on('message', (ch: any, message: any) => { if (ch === stopKey && message) stopRequested = true; });
                if (typeof subClient.subscribe === 'function') await subClient.subscribe(stopKey);
              }
            } catch (e) {
              subClient = null;
            }
          }
          // fallback to polling if no pub/sub available
          if (!subClient) {
            if (typeof redisClient.get === 'function') {
              _poll = setInterval(async () => {
                try {
                  const v = await redisClient.get(stopKey);
                  if (v) { stopRequested = true; clearInterval(_poll); }
                } catch (e) {
                  // ignore redis errors
                }
              }, 250);
            }
          }
        } catch (e) {
          // ignore redis setup errors
        }
      }

      if (play.data?.dash) {
        const videoUrl = play.data.dash.video?.[0]?.baseUrl;
        const audioUrl = play.data.dash.audio?.[0]?.baseUrl;
        if (!videoUrl || !audioUrl) throw new Error('dash urls missing');
        const videoTmp = path.join(DATA_DIR, `${job.id}-video`);
        const audioTmp = path.join(DATA_DIR, `${job.id}-audio`);
        const tempOutFile = path.join(DATA_DIR, `${bvid}-${job.id}.mp4`);

        // Persist a minimal manifest so that later tools (including partial merge)
        // know basic metadata even for non-partitioned DASH jobs.
        try {
          const dashManifest = {
            strategy: 'dash-single',
            jobId: String(job.id),
            bvid,
            title,
            safeTitle,
            totalExpectedBytes: 0,
            parts: [] as any[],
          };
          fs.writeFileSync(manifestPath, JSON.stringify(dashManifest, null, 2), 'utf8');
        } catch (e) {
          // best-effort only
        }
        if (fs.existsSync(cancelFile)) {
          await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
          this.logger.log(`Job ${job.id} cancelled before download start`);
          return { cancelled: true };
        }
        if (fs.existsSync(stopFile) || stopRequested) {
          await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
          this.logger.log(`Job ${job.id} stopped before download start`);
          try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
          try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) { }
          if (_poll) clearInterval(_poll);
          try {
            if (subClient) {
              if (typeof subClient.unsubscribe === 'function') await subClient.unsubscribe(stopKey);
              if (typeof subClient.disconnect === 'function') await subClient.disconnect();
              if (typeof subClient.quit === 'function') await subClient.quit();
            }
          } catch (e) {}
          return { stopped: true };
        }
        await job.progress(20);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading-video', progress: 20 });
        try {
          await resumeDownload(
            videoUrl,
            videoTmp,
            { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies },
            cancelFile,
            () => stopRequested,
          );
        } catch (err: any) {
          if (String(err?.message || '').toLowerCase().includes('stopped')) {
            await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
            this.logger.log(`Job ${job.id} stopped during video download`);
            try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
            try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) { }
            if (_poll) clearInterval(_poll);
            try {
              if (subClient) {
                if (typeof subClient.unsubscribe === 'function') await subClient.unsubscribe(stopKey);
                if (typeof subClient.disconnect === 'function') await subClient.disconnect();
                if (typeof subClient.quit === 'function') await subClient.quit();
              }
            } catch (e) {}
            return { stopped: true };
          }
          throw err;
        }
        await job.progress(50);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading-audio', progress: 50 });
        try {
          await resumeDownload(
            audioUrl,
            audioTmp,
            { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies },
            cancelFile,
            () => stopRequested,
          );
        } catch (err: any) {
          if (String(err?.message || '').toLowerCase().includes('stopped')) {
            await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
            this.logger.log(`Job ${job.id} stopped during audio download`);
            try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
            try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) { }
            if (_poll) clearInterval(_poll);
            return { stopped: true };
          }
          throw err;
        }
        await job.progress(70);
        await this.history.appendEvent(job.id.toString(), { state: 'merging', progress: 70 });
        await this.ffmpeg.merge(videoTmp, audioTmp, tempOutFile);
        await job.progress(95);
        try { fs.unlinkSync(videoTmp); } catch (e) { }
        try { fs.unlinkSync(audioTmp); } catch (e) { }
          try {
            if (fs.existsSync(stopFile) || stopRequested) {
              await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
              this.logger.log(`Job ${job.id} stopped during merge`);
              try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
              try { fs.unlinkSync(stopFile); } catch (e) { }
              if (_poll) clearInterval(_poll);
              return { stopped: true };
            }
          } catch (e) {}
          try { if (fs.existsSync(cancelFile)) fs.unlinkSync(cancelFile); } catch (e) { }

        // Move final file into /data/bilibili/<title>/<bvid>-<jobId>.mp4
        const finalDir = path.join(DATA_DIR, 'bilibili', safeTitle);
        try {
          if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true });
        } catch (e) {
          // ignore mkdir errors; we will fallback to DATA_DIR if needed
        }
        const finalPath = fs.existsSync(finalDir)
          ? path.join(finalDir, `${bvid}-${job.id}.mp4`)
          : tempOutFile;
        if (finalPath !== tempOutFile) {
          try {
            fs.renameSync(tempOutFile, finalPath);
          } catch (e) {
            // if rename fails, leave file in place but still report tempOutFile
          }
        }

        await job.progress(100);
        await this.history.appendEvent(job.id.toString(), {
          state: 'finished',
          progress: 100,
          result: { path: finalPath, bvid, title },
        });
        this.logger.log('Job finished, output: ' + finalPath);
        return { path: finalPath };
      }

      if (play.data?.durl?.length) {
        const url = play.data.durl[0].url;
        const tempOutFile = path.join(DATA_DIR, `${bvid}-${job.id}.mp4`);
        if (fs.existsSync(cancelFile)) {
          await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
          this.logger.log(`Job ${job.id} cancelled before download start`);
          return { cancelled: true };
        }
        await job.progress(30);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading', progress: 30 });

        // Try to get content-length and Accept-Ranges to decide whether to use parallel parts.
        let useParallel = false;
        let totalSize = 0;
        try {
          const head = await axios.head(url, {
            headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies },
            timeout: 5000,
          });
          const acceptRanges = String(head.headers['accept-ranges'] || '').toLowerCase();
          const contentLength = parseInt(head.headers['content-length'] || '0', 10) || 0;
          if (acceptRanges.includes('bytes') && contentLength > 0) {
            totalSize = contentLength;
            // Simple heuristic: use parallel when at least two parts of ~8MB each.
            if (contentLength >= 2 * PART_SIZE_BYTES && this.partsQueue) {
              useParallel = true;
            }
          }
        } catch (e) {
          // ignore HEAD errors, fall back to single-stream download
        }

        if (!useParallel) {
          await resumeDownload(
            url,
            tempOutFile,
            { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies },
            cancelFile,
          );
        } else {
          const partSize = PART_SIZE_BYTES;
          const partCount = Math.ceil(totalSize / partSize);
          const partsDir = path.join(manifestDir, 'parts');
          if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true });
          if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });

          const parts: any[] = [];
          for (let i = 0; i < partCount; i++) {
            const start = i * partSize;
            const end = Math.min(totalSize - 1, (i + 1) * partSize - 1);
            const expectedBytes = end - start + 1;
            parts.push({
              partIndex: i,
              rangeStart: start,
              rangeEnd: end,
              expectedBytes,
              state: 'pending',
              downloadedBytes: 0,
            });
          }

          const manifest = {
            strategy: 'durl-byte-range',
            jobId: String(job.id),
            bvid,
            title,
            safeTitle,
            url,
            totalExpectedBytes: totalSize,
            parts,
          };

          fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');

          // Initialise Redis state for parts if a client is available.
          const redisClient: any =
            this.downloadQueue && (this.downloadQueue as any).client ? (this.downloadQueue as any).client : null;
          if (redisClient) {
            try {
              const key = `job:parts:${job.id}`;
              await redisClient.hset(key, 'totalExpectedBytes', String(totalSize));
              for (const p of parts) {
                await redisClient.hset(key, `part:${p.partIndex}:expectedBytes`, String(p.expectedBytes));
                await redisClient.hset(key, `part:${p.partIndex}:state`, 'pending');
              }
            } catch (e) {
              // ignore redis errors
            }
          }

          await this.history.appendEvent(job.id.toString(), { state: 'segmenting', progress: 35, manifestPath });

          const headers = { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies || '' };

          const partJobs = [];
          for (const p of parts) {
            const j = await this.partsQueue!.add(
              {
                jobId: String(job.id),
                bvid,
                url,
                partIndex: p.partIndex,
                rangeStart: p.rangeStart,
                rangeEnd: p.rangeEnd,
                expectedBytes: p.expectedBytes,
                headers,
              },
              { attempts: 3, backoff: 5000 },
            );
            partJobs.push(j);
          }

          // Wait for all part jobs to complete with simple stall detection based on Bull progress.
          const stallMs = 15000;
          const pollMs = 5000;
          const lastProgress: Record<string, { value: number; ts: number }> = {};
          let remaining = partJobs.length;

          while (remaining > 0) {
            remaining = 0;
            for (const pj of partJobs) {
              const id = String(pj.id);
              const state = await pj.getState();
              const prog = (await pj.progress()) as number;

              if (state === 'completed') {
                remaining += 0;
                continue;
              }
              if (state === 'failed') {
                throw new Error(`Part job ${id} failed`);
              }

              remaining += 1;

              const now = Date.now();
              const prev = lastProgress[id];
              if (!prev || prog > prev.value) {
                lastProgress[id] = { value: prog, ts: now };
              } else if (now - prev.ts >= stallMs) {
                await this.history.appendEvent(job.id.toString(), {
                  state: 'failed',
                  progress: 0,
                  message: `Part job ${id} stalled at ${prog}%`,
                });
                throw new Error(`Part job ${id} stalled`);
              }
            }

            if (remaining > 0) {
              await new Promise((r) => setTimeout(r, pollMs));
            }
          }

          // Merge parts into final file
          await this.history.appendEvent(job.id.toString(), { state: 'merging-parts', progress: 80 });
          const outStream = fs.createWriteStream(tempOutFile, { flags: 'w' });
          for (let i = 0; i < parts.length; i++) {
            const partPath = path.join(partsDir, `part-${i}.bin`);
            if (!fs.existsSync(partPath)) {
              throw new Error(`Missing part file ${partPath}`);
            }
            await new Promise<void>((resolve, reject) => {
              const rs = fs.createReadStream(partPath);
              rs.on('error', reject);
              rs.on('end', () => resolve());
              rs.pipe(outStream, { end: false });
            });
          }
          outStream.end();

          await job.progress(95);
        }
        await job.progress(100);
        try {
          if (fs.existsSync(stopFile) || stopRequested) {
            await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
            this.logger.log(`Job ${job.id} stopped during download`);
            try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
            try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) { }
            if (_poll) clearInterval(_poll);
            return { stopped: true };
          }
        } catch (e) {}
        try {
          if (fs.existsSync(cancelFile)) {
            await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
            this.logger.log(`Job ${job.id} cancelled during download`);
            return { cancelled: true };
          }
        } catch (e) {}

        // Move final file into /data/bilibili/<title>/<bvid>-<jobId>.mp4
        const finalDir = path.join(DATA_DIR, 'bilibili', safeTitle);
        try {
          if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true });
        } catch (e) {
          // ignore mkdir errors; we will fallback to DATA_DIR if needed
        }
        const finalPath = fs.existsSync(finalDir)
          ? path.join(finalDir, `${bvid}-${job.id}.mp4`)
          : tempOutFile;
        if (finalPath !== tempOutFile) {
          try {
            fs.renameSync(tempOutFile, finalPath);
          } catch (e) {
            // if rename fails, leave file in place but still report tempOutFile
          }
        }

        await this.history.appendEvent(job.id.toString(), {
          state: 'finished',
          progress: 100,
          result: { path: finalPath, bvid, title },
        });
        try {
          if (fs.existsSync(cancelFile)) fs.unlinkSync(cancelFile);
        } catch (e) {}
        this.logger.log('Job finished, output: ' + finalPath);
        return { path: finalPath };
      }

      throw new Error('No downloadable urls found or DRM-protected');
    } catch (err: any) {
      this.logger.error(`Job ${job.id} failed: ${err?.message || err}`);
      try { await job.progress(0); } catch (e) { }
      try { await this.history.appendEvent(job.id.toString(), { state: 'failed', progress: 0, message: err?.message }); } catch (e) { }
      throw err;
    }
  }

  private sanitizeTitle(raw: string): string {
    const trimmed = (raw || '').trim();
    // Remove characters that are invalid in common filesystems.
    const noSpecials = trimmed.replace(/[\/\\:*?"<>|]/g, '');
    // Collapse whitespace and limit length.
    const collapsed = noSpecials.replace(/\s+/g, ' ');
    return collapsed.substring(0, 80) || 'untitled';
  }

  private async fetchVideoTitle(bvid: string, cookies?: string): Promise<string | null> {
    try {
      const headers: Record<string, string> = {};
      if (cookies) headers.Cookie = cookies;
      const resp = await axios.get('https://api.bilibili.com/x/web-interface/view', {
        params: { bvid },
        headers,
        timeout: 5000,
      });
      return resp.data?.data?.title || null;
    } catch (e) {
      this.logger.warn(`Failed to fetch video title for ${bvid}: ${e instanceof Error ? e.message : String(e)}`);
      return null;
    }
  }
}
