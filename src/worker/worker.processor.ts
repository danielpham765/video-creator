import { Injectable, Logger } from '@nestjs/common';
import { Processor, Process, InjectQueue } from '@nestjs/bull';
import { Job, Queue } from 'bull';
import { PlayurlService } from '../playurl/playurl.service';
import { FfmpegService } from '../ffmpeg/ffmpeg.service';
import { JobHistoryService } from '../jobs/job-history.service';
import { ConfigService } from '@nestjs/config';
import * as fs from 'fs';
import * as path from 'path';
import resumeDownload, { ResumeDownloadResult } from '../utils/resume-download';
import axios from 'axios';

// data directory is configurable via `config.download.dataDir` (absolute or relative)
// part size now configured in `config.download.partSizeBytes` (MB) and converted at runtime

@Processor('downloads')
@Injectable()
export class WorkerProcessor {
  private readonly logger = new Logger(WorkerProcessor.name);

  constructor(
    private readonly playurl: PlayurlService,
    private readonly ffmpeg: FfmpegService,
    private readonly history: JobHistoryService,
    private readonly config: ConfigService,
    @InjectQueue('downloads') private readonly downloadQueue?: Queue,
    @InjectQueue('download-parts') private readonly partsQueue?: Queue,
  ) {
    const cfgDataDir = String(this.config.get('download.dataDir') || path.join(process.cwd(), 'data'));
    this['dataDir'] = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    if (!fs.existsSync(this['dataDir'])) fs.mkdirSync(this['dataDir'], { recursive: true });
  }

  @Process()
  async handleJob(job: Job) {
    this.logger.log('Handling job ' + job.id);
    this.logger.debug(`job.payload=${JSON.stringify(job.data || {})}`);
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
      this.logger.debug('Playurl response data keys: ' + JSON.stringify(Object.keys(play?.data || {})));

      // Fetch and sanitize video title for final output path/manifest.
      const title = (await this.fetchVideoTitle(bvid, cookies)) || bvid;
      const safeTitle = this.sanitizeTitle(title);
      const manifestDir = path.join(this['dataDir'], String(job.id));
      const manifestPath = path.join(manifestDir, 'manifest.json');
      if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true });

      // Compute part size (configured as MB in config.download.partSizeBytes)
      const partSizeMB = Number(this.config.get('download.partSizeBytes') ?? 8);
      const partSizeBytes = Math.max(1, Math.floor(partSizeMB)) * 1024 * 1024;
      this.logger.debug(`Using partSizeBytes=${partSizeBytes} (${partSizeMB} MB) for job ${job.id}`);

      // Download options (timeout + proxy) from config
      const downloadTimeoutMs = Number(this.config.get('download.timeoutMs') ?? 30000);
      const proxyUrl = String(this.config.get('proxy.http') || this.config.get('proxy.https') || '');
      const downloadOptions = { timeoutMs: downloadTimeoutMs, proxy: proxyUrl || undefined };
      const retryCount = Number(this.config.get('download.retryCount') ?? 3);
      const retryBackoffMs = Number(this.config.get('download.retryBackoffMs') ?? 5000);
      const resumeEnabled = Boolean(this.config.get('download.resumeEnabled') ?? true);

      const cancelFile = path.join(this['dataDir'], `${job.id}.cancel`);
      const stopFile = path.join(this['dataDir'], `${job.id}.stop`);
      const stopKey = `job:stop:${job.id}`;
      const cancelKey = `job:cancel:${job.id}`;
      let stopRequested = false;
      let cancelRequested = false;
      let _poll: any = null;
      let subClient: any = null;
      const redisClient: any = (this.downloadQueue && (this.downloadQueue as any).client) ? (this.downloadQueue as any).client : null;
      if (redisClient) {
        try {
          if (typeof redisClient.duplicate === 'function') {
            try {
              subClient = redisClient.duplicate();
              if (typeof subClient.connect === 'function') await subClient.connect();
              if (typeof subClient.subscribe === 'function') {
                await subClient.subscribe(stopKey, (message: any) => { if (message) stopRequested = true; });
                await subClient.subscribe(cancelKey, (message: any) => { if (message) cancelRequested = true; });
              } else if (typeof subClient.on === 'function') {
                subClient.on('message', (ch: any, message: any) => { if (ch === stopKey && message) stopRequested = true; if (ch === cancelKey && message) cancelRequested = true; });
                if (typeof subClient.subscribe === 'function') { await subClient.subscribe(stopKey); await subClient.subscribe(cancelKey); }
              }
            } catch (e) { subClient = null; }
          }
          if (!subClient) {
            if (typeof redisClient.get === 'function') {
              _poll = setInterval(async () => {
                try {
                  const v = await redisClient.get(stopKey);
                  const c = await redisClient.get(cancelKey);
                  if (v) { stopRequested = true; clearInterval(_poll); }
                  if (c) { cancelRequested = true; clearInterval(_poll); }
                } catch (e) { }
              }, 250);
            }
          }
        } catch (e) { }
      }

      if (play.data?.dash) {
        const videoUrl = play.data.dash.video?.[0]?.baseUrl;
        const audioUrl = play.data.dash.audio?.[0]?.baseUrl;
        if (!videoUrl || !audioUrl) throw new Error('dash urls missing');
        const videoTmp = path.join(this['dataDir'], `${job.id}-video`);
        const audioTmp = path.join(this['dataDir'], `${job.id}-audio`);
        const tempOutFile = path.join(this['dataDir'], `${bvid}-${job.id}.mp4`);

        // Persist a minimal manifest so that later tools (including partial merge)
        // know basic metadata even for non-partitioned DASH jobs.
        try {
          const dashManifest = { strategy: 'dash-single', jobId: String(job.id), bvid, title, safeTitle, totalExpectedBytes: 0, parts: [] as any[] };
          fs.writeFileSync(manifestPath, JSON.stringify(dashManifest, null, 2), 'utf8');
        } catch (e) {}

        if (fs.existsSync(cancelFile) || cancelRequested) {
          await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
          this.logger.log(`Job ${job.id} cancelled before download start`);
          return { cancelled: true };
        }
        if (fs.existsSync(stopFile) || stopRequested) {
          await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
          this.logger.log(`Job ${job.id} stopped before download start`);
          try { if (redisClient) await redisClient.del(stopKey); } catch (e) {}
          try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) {}
          if (_poll) clearInterval(_poll);
          try { if (subClient) { if (typeof subClient.unsubscribe === 'function') { try { await subClient.unsubscribe(stopKey); } catch {} try { await subClient.unsubscribe(cancelKey); } catch {} } if (typeof subClient.disconnect === 'function') await subClient.disconnect(); if (typeof subClient.quit === 'function') await subClient.quit(); } } catch (e) {}
          return { stopped: true };
        }

        await job.progress(20);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading-video', progress: 20 });

        // Try to detect manifests for video and audio (HLS or MPD)
        let didSegment = false;
        let segmentList: string[] | null = null;
        let audioSegmentList: string[] | null = null;
        try {
          const manifestResp = await axios.get(videoUrl, { headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, timeout: 5000, responseType: 'text', maxContentLength: 128 * 1024, maxBodyLength: 128 * 1024 });
          const body = String(manifestResp.data || '');
          if (body.includes('#EXTM3U')) {
            const lines = body.split(/\r?\n/).map((l) => l.trim());
            const segs: string[] = [];
            for (const line of lines) { if (!line || line.startsWith('#')) continue; try { segs.push(new URL(line, videoUrl).href); } catch (e) {} }
            if (segs.length) {
              segmentList = segs;
              this.logger.debug(`Detected HLS video manifest with ${segs.length} segments`);
            }
          } else if (body.includes('<MPD')) {
            const segs: string[] = []; const segUrlRe = /<SegmentURL[^>]*media=\"([^\"]+)\"/gi; let m: any; while ((m = segUrlRe.exec(body))) { try { segs.push(new URL(m[1], videoUrl).href); } catch (e) {} }
            if (!segs.length) { const baseRe = /<BaseURL>([^<]+)<\/BaseURL>/gi; while ((m = baseRe.exec(body))) { try { segs.push(new URL(m[1].trim(), videoUrl).href); } catch (e) {} } }
            if (segs.length) {
              segmentList = segs;
              this.logger.debug(`Detected MPD video manifest with ${segs.length} segments`);
            }
          }
        } catch (e) { segmentList = null; }

        try {
          const manifestRespA = await axios.get(audioUrl, { headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, timeout: 5000, responseType: 'text', maxContentLength: 128 * 1024, maxBodyLength: 128 * 1024 });
          const bodyA = String(manifestRespA.data || '');
          if (bodyA.includes('#EXTM3U')) {
            const lines = bodyA.split(/\r?\n/).map((l) => l.trim()); const segs: string[] = []; for (const line of lines) { if (!line || line.startsWith('#')) continue; try { segs.push(new URL(line, audioUrl).href); } catch (e) {} }
            if (segs.length) {
              audioSegmentList = segs;
              this.logger.debug(`Detected HLS audio manifest with ${segs.length} segments`);
            }
          } else if (bodyA.includes('<MPD')) {
            const segs: string[] = []; const segUrlRe = /<SegmentURL[^>]*media=\"([^\"]+)\"/gi; let m: any; while ((m = segUrlRe.exec(bodyA))) { try { segs.push(new URL(m[1], audioUrl).href); } catch (e) {} }
            if (!segs.length) { const baseRe = /<BaseURL>([^<]+)<\/BaseURL>/gi; while ((m = baseRe.exec(bodyA))) { try { segs.push(new URL(m[1].trim(), audioUrl).href); } catch (e) {} } }
            if (segs.length) {
              audioSegmentList = segs;
              this.logger.debug(`Detected MPD audio manifest with ${segs.length} segments`);
            }
          }
        } catch (e) { audioSegmentList = null; }

        // If we found segment lists, create parts and enqueue part jobs for both video and audio (when available)
        if (Array.isArray(segmentList) && segmentList.length > 0 && this.partsQueue) {
          const partsDir = path.join(manifestDir, 'parts'); if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true }); if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });

          const segments: Array<{ url: string; approxBytes: number }> = [];
          for (let i = 0; i < segmentList.length; i++) {
            const su = segmentList[i]; let approx = 0; try { const h = await axios.head(su, { headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, timeout: 5000 }); approx = parseInt(h.headers['content-length'] || '0', 10) || 0; } catch (e) { approx = 0; }
            segments.push({ url: su, approxBytes: approx });
          }

          this.logger.debug(`Collected ${segments.length} segments (video)`);

          const audioSegments: Array<{ url: string; approxBytes: number }> = [];
          if (Array.isArray(audioSegmentList) && audioSegmentList.length > 0) {
            for (let i = 0; i < audioSegmentList.length; i++) {
              const su = audioSegmentList[i];
              let approx = 0;
              try {
                const h = await axios.head(su, { headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, timeout: 5000 });
                approx = parseInt(h.headers['content-length'] || '0', 10) || 0;
              } catch (e) {
                approx = 0;
              }
              audioSegments.push({ url: su, approxBytes: approx });
            }
          }

          const parts: any[] = []; let cur: any = null;
          for (let i = 0; i < segments.length; i++) { const s = segments[i]; if (!cur) cur = { partIndex: parts.length, segmentUrls: [], expectedBytes: 0, state: 'pending', downloadedBytes: 0 }; cur.segmentUrls.push(s.url); cur.expectedBytes += s.approxBytes || 0; if (cur.expectedBytes >= partSizeBytes || i === segments.length - 1) { parts.push(cur); cur = null; } }

          const audioParts: any[] = [];
          if (audioSegments.length > 0) {
            let curA: any = null;
            for (let i = 0; i < audioSegments.length; i++) {
              const s = audioSegments[i];
              if (!curA) curA = { partIndex: audioParts.length, segmentUrls: [], expectedBytes: 0, state: 'pending', downloadedBytes: 0 };
              curA.segmentUrls.push(s.url);
              curA.expectedBytes += s.approxBytes || 0;
              if (curA.expectedBytes >= partSizeBytes || i === audioSegments.length - 1) { audioParts.push(curA); curA = null; }
            }
          }

          const videoExpectedBytes = parts.reduce((s, p) => s + (p.expectedBytes || 0), 0);
          const audioExpectedBytes = audioParts.reduce((s, p) => s + (p.expectedBytes || 0), 0);
          const totalExpectedBytes = videoExpectedBytes + audioExpectedBytes;
          const manifest = { strategy: 'dash-segmented', jobId: String(job.id), bvid, title, safeTitle, totalExpectedBytes, parts };
          fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
          await this.setTotalExpectedBytes(redisClient, String(job.id), totalExpectedBytes);

          if (redisClient) {
            try {
              const key = `job:parts:${job.id}`;
              for (const p of parts) {
                await redisClient.hset(key, `part:${p.partIndex}:expectedBytes`, String(p.expectedBytes || 0));
                await redisClient.hset(key, `part:${p.partIndex}:state`, 'pending');
              }
              for (const p of audioParts) {
                await redisClient.hset(key, `part:audio:${p.partIndex}:expectedBytes`, String(p.expectedBytes || 0));
                await redisClient.hset(key, `part:audio:${p.partIndex}:state`, 'pending');
              }
            } catch (e) {}
          }

          await this.history.appendEvent(job.id.toString(), { state: 'segmenting', progress: 30, manifestPath });
          this.logger.log(`Job ${job.id}: segmented manifest written with ${parts.length} parts`);

          const headers = { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies || '' };
          const partJobs: Job[] = [];
          for (const p of parts) {
            this.logger.debug(`Enqueue video part job=${job.id} part=${p.partIndex} expectedBytes=${p.expectedBytes}`);
            const j = await this.partsQueue!.add({ jobId: String(job.id), bvid, segmentUrls: p.segmentUrls, partIndex: p.partIndex, expectedBytes: p.expectedBytes || 0, headers, role: 'video' } as any, { attempts: retryCount, backoff: retryBackoffMs });
            partJobs.push(j);
          }

          const audioPartJobs: Job[] = [];
          if (audioParts.length > 0) {
            for (const p of audioParts) {
              this.logger.debug(`Enqueue audio part job=${job.id} part=${p.partIndex} expectedBytes=${p.expectedBytes}`);
              const j = await this.partsQueue!.add({ jobId: String(job.id), bvid, segmentUrls: p.segmentUrls, partIndex: p.partIndex, expectedBytes: p.expectedBytes || 0, headers, role: 'audio' } as any, { attempts: retryCount, backoff: retryBackoffMs });
              audioPartJobs.push(j);
            }
          }

          // Wait for all part jobs to complete with stall detection.
          // Important: do not treat waiting/delayed jobs as stalled just because they are queued.
          const activeStallMs = 45000;
          const queuedStallMs = 10 * 60 * 1000;
          const pollMs = 5000;
          const lastProgress: Record<string, { value: number; ts: number; state: string }> = {};
          let remaining = partJobs.length + (Array.isArray(audioPartJobs) ? audioPartJobs.length : 0);
          const segmentedProgressMin = 30;
          const segmentedProgressMax = 74;
          if (totalExpectedBytes > 0) {
            await this.reportByteProgress(job, 0, totalExpectedBytes, segmentedProgressMin, segmentedProgressMax);
          }
          while (remaining > 0) {
            remaining = 0;
            for (const pj of [...partJobs, ...audioPartJobs]) {
              const id = String(pj.id);
              const state = await pj.getState();
              const prog = (await pj.progress()) as number;
              this.logger.debug(`poll part job id=${id} state=${state} progress=${prog}`);
              if (state === 'completed') continue;
              if (state === 'failed') throw new Error(`Part job ${id} failed`);
              remaining += 1;
              const now = Date.now();
              const prev = lastProgress[id];
              if (!prev || prog > prev.value || prev.state !== state) {
                lastProgress[id] = { value: prog, ts: now, state };
              } else {
                const timeout = state === 'active' ? activeStallMs : queuedStallMs;
                if (now - prev.ts >= timeout) {
                  await this.history.appendEvent(job.id.toString(), { state: 'failed', progress: 0, message: `Part job ${id} stalled at ${prog}% (state=${state})` });
                  throw new Error(`Part job ${id} stalled (state=${state})`);
                }
              }
            }
            if (totalExpectedBytes > 0) {
              const totals = await this.readAggregatedPartBytes(redisClient, String(job.id));
              if (totals.totalExpectedBytes > 0) {
                await this.reportByteProgress(job, totals.totalDownloadedBytes, totals.totalExpectedBytes, segmentedProgressMin, segmentedProgressMax);
              }
            }
            if (remaining > 0) await new Promise((r) => setTimeout(r, pollMs));
          }
          if (totalExpectedBytes > 0) {
            await this.reportByteProgress(job, totalExpectedBytes, totalExpectedBytes, segmentedProgressMin, segmentedProgressMax);
          }

          // build video parts list for ffmpeg concat
          await this.history.appendEvent(job.id.toString(), { state: 'merging-parts', progress: 75 });
          const partsListPath = path.join(manifestDir, 'parts.txt');
          const partsDirPath = path.join(manifestDir, 'parts');
          const listLines: string[] = [];
          for (let i = 0; i < parts.length; i++) { const partPath = path.join(partsDirPath, `part-${i}.bin`); if (!fs.existsSync(partPath)) throw new Error(`Missing part file ${partPath}`); listLines.push(`file '${partPath.replace(/'/g, "'\\''")}'`); }
          fs.writeFileSync(partsListPath, listLines.join('\n'), 'utf8');
          const videoConcatTmp = path.join(manifestDir, `${job.id}-video-concat.mp4`);
          this.logger.debug(`Merging video parts list ${partsListPath} -> ${videoConcatTmp}`);
          await this.ffmpeg.mergeParts(partsListPath, videoConcatTmp);

          // build audio concat if audio parts present
          let audioConcatTmp: string | null = null;
          if (audioParts && audioParts.length > 0) {
            const audioListPath = path.join(manifestDir, 'audio-parts.txt');
            const audioLines: string[] = [];
            for (let i = 0; i < audioParts.length; i++) { const partPath = path.join(partsDirPath, `audio-part-${i}.bin`); if (!fs.existsSync(partPath)) throw new Error(`Missing audio part file ${partPath}`); audioLines.push(`file '${partPath.replace(/'/g, "'\\''")}'`); }
            fs.writeFileSync(audioListPath, audioLines.join('\n'), 'utf8');
            audioConcatTmp = path.join(manifestDir, `${job.id}-audio-concat.mp4`);
            this.logger.debug(`Merging audio parts list ${audioListPath} -> ${audioConcatTmp}`);
            await this.ffmpeg.mergeParts(audioListPath, audioConcatTmp);
          }

          // if audioConcatTmp not created, fallback to downloading single audio file
          if (!audioConcatTmp) {
            await job.progress(85);
            await this.history.appendEvent(job.id.toString(), { state: 'downloading-audio', progress: 85 });
              try {
                if (!resumeEnabled && fs.existsSync(audioTmp)) {
                  try { fs.unlinkSync(audioTmp); } catch (e) {}
                }
                await resumeDownload(audioUrl, audioTmp, { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, cancelFile, () => stopRequested || cancelRequested, undefined, false, { ...downloadOptions, logger: this.logger });
              } catch (err: any) {
              if (String(err?.message || '').toLowerCase().includes('stopped')) { await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 }); this.logger.log(`Job ${job.id} stopped during audio download`); try { if (redisClient) await redisClient.del(stopKey); } catch (e) {} if (_poll) clearInterval(_poll); return { stopped: true }; }
              throw err;
            }
            audioConcatTmp = audioTmp;
          }

          await job.progress(90);
          await this.history.appendEvent(job.id.toString(), { state: 'merging', progress: 90 });
          await this.ffmpeg.merge(videoConcatTmp, audioConcatTmp!, tempOutFile);
          try { fs.unlinkSync(videoConcatTmp); } catch (e) {}
          try { if (audioConcatTmp && audioConcatTmp !== audioTmp) fs.unlinkSync(audioConcatTmp); } catch (e) {}
          didSegment = true;
        }

        // fallback: if we didn't do segmented path, download whole video+audio as before
        if (!didSegment) {
          if (fs.existsSync(cancelFile) || cancelRequested) { await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 }); this.logger.log(`Job ${job.id} cancelled before download start`); return { cancelled: true }; }
          let dashSingleTotalSize = 0;
          try {
            const vh = await axios.head(videoUrl, { headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, timeout: 5000 });
            dashSingleTotalSize += parseInt(vh.headers['content-length'] || '0', 10) || 0;
          } catch (e) {}
          try {
            const ah = await axios.head(audioUrl, { headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, timeout: 5000 });
            dashSingleTotalSize += parseInt(ah.headers['content-length'] || '0', 10) || 0;
          } catch (e) {}
          if (dashSingleTotalSize > 0) await this.setTotalExpectedBytes(redisClient, String(job.id), dashSingleTotalSize);
          let dashDownloaded = 0;
          try { if (fs.existsSync(videoTmp)) dashDownloaded += fs.statSync(videoTmp).size; } catch (e) {}
          try { if (fs.existsSync(audioTmp)) dashDownloaded += fs.statSync(audioTmp).size; } catch (e) {}
          const onDashProgress = (delta: number) => {
            dashDownloaded += delta;
            if (dashSingleTotalSize > 0) {
              void this.reportByteProgress(job, dashDownloaded, dashSingleTotalSize, 20, 89);
            }
          };
          if (dashSingleTotalSize > 0) {
            await this.reportByteProgress(job, dashDownloaded, dashSingleTotalSize, 20, 89);
          }
          try {
            if (!resumeEnabled && fs.existsSync(videoTmp)) { try { fs.unlinkSync(videoTmp); } catch (e) {} }
            await resumeDownload(videoUrl, videoTmp, { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, cancelFile, () => stopRequested || cancelRequested, onDashProgress, false, { ...downloadOptions, logger: this.logger });
          } catch (err: any) {
            if (String(err?.message || '').toLowerCase().includes('stopped')) { await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 }); this.logger.log(`Job ${job.id} stopped during video download`); try { if (redisClient) await redisClient.del(stopKey); } catch (e) {} if (_poll) clearInterval(_poll); try { if (subClient) { if (typeof subClient.unsubscribe === 'function') { try { await subClient.unsubscribe(stopKey); } catch {} try { await subClient.unsubscribe(cancelKey); } catch {} } if (typeof subClient.disconnect === 'function') await subClient.disconnect(); if (typeof subClient.quit === 'function') await subClient.quit(); } } catch (e) {} return { stopped: true }; }
            throw err;
          }
          await job.progress(50);
          await this.history.appendEvent(job.id.toString(), { state: 'downloading-audio', progress: 50 });
            try { if (!resumeEnabled && fs.existsSync(audioTmp)) { try { fs.unlinkSync(audioTmp); } catch (e) {} } await resumeDownload(audioUrl, audioTmp, { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, cancelFile, () => stopRequested || cancelRequested, onDashProgress, false, { ...downloadOptions, logger: this.logger }); } catch (err: any) { if (String(err?.message || '').toLowerCase().includes('stopped')) { await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 }); this.logger.log(`Job ${job.id} stopped during audio download`); try { if (redisClient) await redisClient.del(stopKey); } catch (e) {} if (_poll) clearInterval(_poll); return { stopped: true }; } throw err; }
          await job.progress(70);
          await this.history.appendEvent(job.id.toString(), { state: 'merging', progress: 70 });
          await this.ffmpeg.merge(videoTmp, audioTmp, tempOutFile);
          await job.progress(95);
          try { fs.unlinkSync(videoTmp); } catch (e) {}
          try { fs.unlinkSync(audioTmp); } catch (e) {}
        }

        // Move final file into <dataDir>/bilibili/<title>/<bvid>-<jobId>.mp4
        const finalDir = path.join(this['dataDir'], 'bilibili', safeTitle);
        try { if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true }); } catch (e) {}
        const finalPath = fs.existsSync(finalDir) ? path.join(finalDir, `${bvid}-${job.id}.mp4`) : tempOutFile;
        if (finalPath !== tempOutFile) { try { fs.renameSync(tempOutFile, finalPath); } catch (e) {} }

        await job.progress(100);
        await this.history.appendEvent(job.id.toString(), { state: 'finished', progress: 100, result: { path: finalPath, bvid, title } });
        try { if (fs.existsSync(cancelFile)) fs.unlinkSync(cancelFile); } catch (e) {}
        this.logger.log('Job finished, output: ' + finalPath);
        return { path: finalPath };
      }

      // existing durl (single url / byte-range) flow remains unchanged
      if (play.data?.durl?.length) {
        const url = play.data.durl[0].url;
        const tempOutFile = path.join(this['dataDir'], `${bvid}-${job.id}.mp4`);
        if (fs.existsSync(cancelFile)) { await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 }); this.logger.log(`Job ${job.id} cancelled before download start`); return { cancelled: true }; }
        await job.progress(30);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading', progress: 30 });

        // Try to get content-length and Accept-Ranges to decide whether to use parallel parts.
        let useParallel = false; let totalSize = 0;
        try {
          const head = await axios.head(url, { headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, timeout: 5000 });
          const acceptRanges = String(head.headers['accept-ranges'] || '').toLowerCase();
          const contentLength = parseInt(head.headers['content-length'] || '0', 10) || 0;
          if (contentLength > 0) {
            totalSize = contentLength;
          }
          if (acceptRanges.includes('bytes') && contentLength > 0) {
            if (contentLength >= 2 * partSizeBytes && this.partsQueue) {
              // Some origins advertise ranges but still respond with full 200 bodies.
              // Probe with a tiny ranged GET and only enable parallel path on a true 206 response.
              let rangeProbeOk = false;
              try {
                const probe = await axios.get(url, {
                  headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies, Range: 'bytes=0-1' },
                  timeout: 5000,
                  responseType: 'stream',
                  validateStatus: (status: number) => status === 200 || status === 206,
                });
                rangeProbeOk = probe.status === 206;
                try { if (probe.data && typeof probe.data.destroy === 'function') probe.data.destroy(); } catch (e) {}
              } catch (e) {
                rangeProbeOk = false;
              }
              if (rangeProbeOk) useParallel = true;
              else this.logger.warn(`Job ${job.id}: origin did not honor range probe (expected 206); falling back to single download`);
            }
          }
        } catch (e) {}
        if (totalSize > 0) await this.setTotalExpectedBytes(redisClient, String(job.id), totalSize);

        if (!useParallel) {
          let downloaded = 0;
          try { if (fs.existsSync(tempOutFile)) downloaded = fs.statSync(tempOutFile).size; } catch (e) {}
          const onProgress = (delta: number) => {
            downloaded += delta;
            if (totalSize > 0) {
              void this.reportByteProgress(job, downloaded, totalSize, 30, 95);
            }
          };
          if (totalSize > 0) await this.reportByteProgress(job, downloaded, totalSize, 30, 95);
          await resumeDownload(url, tempOutFile, { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, cancelFile, undefined, onProgress, false, { ...downloadOptions, logger: this.logger });
        } else {
          const partSize = partSizeBytes; const partCount = Math.ceil(totalSize / partSize); const partsDir = path.join(manifestDir, 'parts'); if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true }); if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });
          const parts: any[] = [];
          for (let i = 0; i < partCount; i++) { const start = i * partSize; const end = Math.min(totalSize - 1, (i + 1) * partSize - 1); const expectedBytes = end - start + 1; parts.push({ partIndex: i, rangeStart: start, rangeEnd: end, expectedBytes, state: 'pending', downloadedBytes: 0 }); }
          const manifest = { strategy: 'durl-byte-range', jobId: String(job.id), bvid, title, safeTitle, url, totalExpectedBytes: totalSize, parts };
          fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
          this.logger.log(`Job ${job.id}: created byte-range manifest with ${parts.length} parts (total ${totalSize} bytes)`);
          if (redisClient) { try { const key = `job:parts:${job.id}`; await redisClient.hset(key, 'totalExpectedBytes', String(totalSize)); for (const p of parts) { await redisClient.hset(key, `part:${p.partIndex}:expectedBytes`, String(p.expectedBytes)); await redisClient.hset(key, `part:${p.partIndex}:state`, 'pending'); } } catch (e) {} }
          await this.history.appendEvent(job.id.toString(), { state: 'segmenting', progress: 35, manifestPath });
          const headers = { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies || '' };
          const partJobs = [];
          for (const p of parts) { this.logger.debug(`Enqueue byte-range part job=${job.id} part=${p.partIndex} range=${p.rangeStart}-${p.rangeEnd} expected=${p.expectedBytes}`); const j = await this.partsQueue!.add({ jobId: String(job.id), bvid, url, partIndex: p.partIndex, rangeStart: p.rangeStart, rangeEnd: p.rangeEnd, expectedBytes: p.expectedBytes, headers } as any, { attempts: retryCount, backoff: retryBackoffMs }); partJobs.push(j); }
          const activeStallMs = 45000;
          const queuedStallMs = 10 * 60 * 1000;
          const pollMs = 5000;
          const lastProgress: Record<string, { value: number; ts: number; state: string }> = {};
          let remaining = partJobs.length;
          const durlProgressMin = 35;
          const durlProgressMax = 79;
          if (totalSize > 0) await this.reportByteProgress(job, 0, totalSize, durlProgressMin, durlProgressMax);
          while (remaining > 0) {
            remaining = 0;
            for (const pj of partJobs) {
              const id = String(pj.id);
              const state = await pj.getState();
              const prog = (await pj.progress()) as number;
              if (state === 'completed') continue;
              if (state === 'failed') throw new Error(`Part job ${id} failed`);
              remaining += 1;
              const now = Date.now();
              const prev = lastProgress[id];
              if (!prev || prog > prev.value || prev.state !== state) {
                lastProgress[id] = { value: prog, ts: now, state };
              } else {
                const timeout = state === 'active' ? activeStallMs : queuedStallMs;
                if (now - prev.ts >= timeout) {
                  await this.history.appendEvent(job.id.toString(), {
                    state: 'failed',
                    progress: 0,
                    message: `Part job ${id} stalled at ${prog}% (state=${state})`,
                  });
                  throw new Error(`Part job ${id} stalled (state=${state})`);
                }
              }
            }
            if (totalSize > 0) {
              const totals = await this.readAggregatedPartBytes(redisClient, String(job.id));
              if (totals.totalExpectedBytes > 0) {
                await this.reportByteProgress(job, totals.totalDownloadedBytes, totals.totalExpectedBytes, durlProgressMin, durlProgressMax);
              }
            }
            if (remaining > 0) await new Promise((r) => setTimeout(r, pollMs));
          }
          if (totalSize > 0) await this.reportByteProgress(job, totalSize, totalSize, durlProgressMin, durlProgressMax);
          await this.history.appendEvent(job.id.toString(), { state: 'merging-parts', progress: 80 });
          const outStream = fs.createWriteStream(tempOutFile, { flags: 'w' });
          for (let i = 0; i < parts.length; i++) { const partPath = path.join(partsDir, `part-${i}.bin`); if (!fs.existsSync(partPath)) throw new Error(`Missing part file ${partPath}`); await new Promise<void>((resolve, reject) => { const rs = fs.createReadStream(partPath); rs.on('error', reject); rs.on('end', () => resolve()); rs.pipe(outStream, { end: false }); }); }
          await new Promise<void>((resolve, reject) => {
            outStream.on('error', reject);
            outStream.on('finish', () => resolve());
            outStream.end();
          });
          await job.progress(95);
        }
        await job.progress(100);
        try { if (fs.existsSync(stopFile) || stopRequested) { await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 }); this.logger.log(`Job ${job.id} stopped during download`); try { if (redisClient) await redisClient.del(stopKey); } catch (e) {} try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) {} if (_poll) clearInterval(_poll); return { stopped: true }; } } catch (e) {}
        try { if (fs.existsSync(cancelFile)) { await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 }); this.logger.log(`Job ${job.id} cancelled during download`); return { cancelled: true }; } } catch (e) {}
        const finalDir = path.join(this['dataDir'], 'bilibili', safeTitle); try { if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true }); } catch (e) {}
        const finalPath = fs.existsSync(finalDir) ? path.join(finalDir, `${bvid}-${job.id}.mp4`) : tempOutFile; if (finalPath !== tempOutFile) { try { fs.renameSync(tempOutFile, finalPath); } catch (e) {} }
        await this.history.appendEvent(job.id.toString(), { state: 'finished', progress: 100, result: { path: finalPath, bvid, title } }); try { if (fs.existsSync(cancelFile)) fs.unlinkSync(cancelFile); } catch (e) {}
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

  private async setTotalExpectedBytes(redisClient: any, jobId: string, totalExpectedBytes: number): Promise<void> {
    if (!redisClient || typeof redisClient.hset !== 'function' || totalExpectedBytes <= 0) return;
    try {
      const key = `job:parts:${jobId}`;
      await redisClient.hset(key, 'totalExpectedBytes', String(totalExpectedBytes));
    } catch (e) {
      // ignore redis errors
    }
  }

  private async readAggregatedPartBytes(redisClient: any, jobId: string): Promise<{ totalExpectedBytes: number; totalDownloadedBytes: number }> {
    if (!redisClient || typeof redisClient.hgetall !== 'function') return { totalExpectedBytes: 0, totalDownloadedBytes: 0 };
    try {
      const key = `job:parts:${jobId}`;
      const raw = await redisClient.hgetall(key);
      if (!raw || typeof raw !== 'object') return { totalExpectedBytes: 0, totalDownloadedBytes: 0 };
      let totalExpectedBytes = parseInt(raw.totalExpectedBytes || '0', 10) || 0;
      let totalDownloadedBytes = 0;
      for (const [field, value] of Object.entries(raw)) {
        if (field.endsWith(':downloadedBytes')) {
          totalDownloadedBytes += parseInt(String(value || '0'), 10) || 0;
        }
        if (!totalExpectedBytes && field.endsWith(':expectedBytes')) {
          totalExpectedBytes += parseInt(String(value || '0'), 10) || 0;
        }
      }
      return { totalExpectedBytes, totalDownloadedBytes };
    } catch (e) {
      return { totalExpectedBytes: 0, totalDownloadedBytes: 0 };
    }
  }

  private async reportByteProgress(job: Job, downloaded: number, total: number, minProgress: number, maxProgress: number): Promise<void> {
    if (!(total > 0) || maxProgress <= minProgress) return;
    const safeDownloaded = Math.max(0, Math.min(downloaded, total));
    const ratio = safeDownloaded / total;
    const next = Math.max(minProgress, Math.min(maxProgress, Math.floor(minProgress + ratio * (maxProgress - minProgress))));
    try {
      await job.progress(next);
    } catch (e) {
      // ignore transient progress update errors
    }
  }

  private sanitizeTitle(raw: string): string {
    const trimmed = (raw || '').trim();
    const noSpecials = trimmed.replace(/[\/\\:*?"<>|]/g, '');
    const collapsed = noSpecials.replace(/\s+/g, ' ');
    return collapsed.substring(0, 80) || 'untitled';
  }

  private async fetchVideoTitle(bvid: string, cookies?: string): Promise<string | null> {
    try {
      const headers: Record<string, string> = {};
      if (cookies) headers.Cookie = cookies;
      const resp = await axios.get('https://api.bilibili.com/x/web-interface/view', { params: { bvid }, headers, timeout: 5000 });
      return resp.data?.data?.title || null;
    } catch (e) {
      this.logger.warn(`Failed to fetch video title for ${bvid}: ${e instanceof Error ? e.message : String(e)}`);
      return null;
    }
  }
}
