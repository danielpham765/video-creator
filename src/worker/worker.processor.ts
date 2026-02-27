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
  private static activeMasterJobs = 0;
  private static masterWaiters: Array<() => void> = [];
  private readonly masterConcurrency: number;
  private readonly singleMaxConcurrentDownloads: number;
  private readonly globalSingleMaxConcurrentDownloads: number;
  private readonly limiterLeaseMs: number;
  private readonly limiterWaitMs: number;
  private readonly renewTimers = new Map<string, NodeJS.Timeout>();

  private static readonly GLOBAL_SINGLE_DOWNLOADS_SEMAPHORE_KEY = 'semaphore:downloads:single:global';
  private static readonly ACQUIRE_GLOBAL_PERMIT_LUA = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local leaseMs = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local token = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - leaseMs)
local count = redis.call('ZCARD', key)
if count < limit then
  redis.call('ZADD', key, now, token)
  redis.call('PEXPIRE', key, leaseMs * 2)
  return 1
end
return 0
`;

  private static readonly RENEW_GLOBAL_PERMIT_LUA = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local leaseMs = tonumber(ARGV[2])
local token = ARGV[3]
if redis.call('ZSCORE', key, token) then
  redis.call('ZADD', key, now, token)
  redis.call('PEXPIRE', key, leaseMs * 2)
  return 1
end
return 0
`;

  private static readonly RELEASE_GLOBAL_PERMIT_LUA = `
local key = KEYS[1]
local token = ARGV[1]
redis.call('ZREM', key, token)
if redis.call('ZCARD', key) == 0 then
  redis.call('DEL', key)
end
return 1
`;

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
    const workerConcurrency = Math.max(1, Number(this.config.get('worker.concurrency') ?? 4));
    this.singleMaxConcurrentDownloads = Math.max(1, Number(this.config.get('download.singleMaxConcurrentDownloads') ?? process.env.SINGLE_MAX_CONCURRENT_DOWNLOADS ?? 5));
    this.masterConcurrency = Math.min(workerConcurrency, this.singleMaxConcurrentDownloads);
    this.globalSingleMaxConcurrentDownloads = Math.max(0, Number(this.config.get('download.globalSingleMaxConcurrentDownloads') ?? process.env.GLOBAL_SINGLE_MAX_CONCURRENT_DOWNLOADS ?? 5));
    this.limiterLeaseMs = Math.max(5000, Number(this.config.get('download.globalLimiterLeaseMs') ?? process.env.GLOBAL_LIMITER_LEASE_MS ?? 120000));
    this.limiterWaitMs = Math.max(100, Number(this.config.get('download.globalLimiterWaitMs') ?? process.env.GLOBAL_LIMITER_WAIT_MS ?? 300));
    if (!fs.existsSync(this['dataDir'])) fs.mkdirSync(this['dataDir'], { recursive: true });
  }

  @Process()
  async handleJob(job: Job) {
    await this.acquireMasterSlot(job);
    try {
      this.logger.log('Handling job ' + job.id);
      this.logger.debug(`job.payload=${JSON.stringify(job.data || {})}`);
      const payload = job.data || {};
      const bvid = payload.bvid;
      if (!bvid) throw new Error('missing bvid');
      const cookies = this.loadCookiesFromConfig();
      if (!cookies) {
        this.logger.warn(`Job ${job.id}: cookies not found in config/cookies.json; playback may be limited`);
      } else {
        this.logger.debug(`Job ${job.id}: loaded cookies from config/cookies.json (${cookies.length} chars)`);
      }

      await job.progress(5);
      await this.history.appendEvent(job.id.toString(), { state: 'queued', progress: 5 });
      const cid = await this.playurl.getCidFromBvid(bvid);
      if (!cid) throw new Error('cannot get cid');
      await job.progress(10);
      await this.history.appendEvent(job.id.toString(), { state: 'resolving', progress: 10 });
      const play = await this.playurl.getPlayurl(bvid, cid, cookies);
      this.logger.log('Playurl response code: ' + play?.code);
      this.logger.debug('Playurl response data keys: ' + JSON.stringify(Object.keys(play?.data || {})));

      // If caller provides title, always use it.
      // Otherwise fetch title from source, optionally translate to Vietnamese
      // when detected language is not Vietnamese/English, then normalize.
      const passedTitle = typeof payload.title === 'string' ? payload.title.trim() : '';
      if (passedTitle) {
        this.logger.debug(`Job ${job.id}: using passed title="${passedTitle}"`);
      } else {
        this.logger.debug(`Job ${job.id}: no passed title; will fetch title from source`);
      }
      let title = passedTitle || (await this.fetchVideoTitle(bvid, cookies)) || bvid;
      this.logger.debug(`Job ${job.id}: resolved raw title="${title}"`);
      if (!passedTitle && title && !this.isVietnameseOrEnglishTitle(title)) {
        this.logger.debug(`Job ${job.id}: title detected as non-VI/EN, translating to Vietnamese`);
        const translated = await this.translateToVietnamese(title);
        if (translated) {
          this.logger.debug(`Job ${job.id}: translated title="${translated}"`);
          title = translated;
        } else {
          this.logger.debug(`Job ${job.id}: translation unavailable, keeping original title`);
        }
      } else if (!passedTitle) {
        this.logger.debug(`Job ${job.id}: title detected as VI/EN, skipping translation`);
      }
      const safeTitle = this.normalizeTitleForFolder(title);
      this.logger.debug(`Job ${job.id}: normalized folder title="${safeTitle}" from title="${title}"`);
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
          const videoParts = parts;

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
          const finalAudioParts = audioParts;

          const videoExpectedBytes = videoParts.reduce((s, p) => s + (p.expectedBytes || 0), 0);
          const audioExpectedBytes = finalAudioParts.reduce((s, p) => s + (p.expectedBytes || 0), 0);
          const totalExpectedBytes = videoExpectedBytes + audioExpectedBytes;
          const manifest = { strategy: 'dash-segmented', jobId: String(job.id), bvid, title, safeTitle, totalExpectedBytes, parts: videoParts };
          fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
          await this.setTotalExpectedBytes(redisClient, String(job.id), totalExpectedBytes);

          if (redisClient) {
            try {
              const key = `job:parts:${job.id}`;
              for (const p of videoParts) {
                await redisClient.hset(key, `part:${p.partIndex}:expectedBytes`, String(p.expectedBytes || 0));
                await redisClient.hset(key, `part:${p.partIndex}:state`, 'pending');
              }
              for (const p of finalAudioParts) {
                await redisClient.hset(key, `part:audio:${p.partIndex}:expectedBytes`, String(p.expectedBytes || 0));
                await redisClient.hset(key, `part:audio:${p.partIndex}:state`, 'pending');
              }
            } catch (e) {}
          }

          await this.history.appendEvent(job.id.toString(), { state: 'segmenting', progress: 30, manifestPath });
          this.logger.log(`Job ${job.id}: segmented manifest written with ${videoParts.length} parts`);

          const headers = { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies || '' };
          const partJobs: Job[] = [];
          for (const p of videoParts) {
            this.logger.debug(`Enqueue video part job=${job.id} part=${p.partIndex} expectedBytes=${p.expectedBytes}`);
            const j = await this.partsQueue!.add({ jobId: String(job.id), bvid, segmentUrls: p.segmentUrls, partIndex: p.partIndex, expectedBytes: p.expectedBytes || 0, headers, role: 'video' } as any, { attempts: retryCount, backoff: retryBackoffMs });
            partJobs.push(j);
          }

          const audioPartJobs: Job[] = [];
          if (finalAudioParts.length > 0) {
            for (const p of finalAudioParts) {
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
          const lastProgress: Record<string, { value: number; ts: number; state: string; bytes: number }> = {};
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
              const pjData: any = (pj as any).data || {};
              const partIndex = typeof pjData.partIndex === 'number' ? pjData.partIndex : undefined;
              const role: 'video' | 'audio' = pjData.role === 'audio' ? 'audio' : 'video';
              const state = await pj.getState();
              const prog = (await pj.progress()) as number;
              const downloadedBytes = await this.readPartDownloadedBytes(redisClient, String(job.id), partIndex, role);
              this.logger.debug(`poll part job id=${id} state=${state} progress=${prog} bytes=${downloadedBytes}`);
              if (state === 'completed') continue;
              if (state === 'failed') throw new Error(`Part job ${id} failed`);
              remaining += 1;
              const now = Date.now();
              const prev = lastProgress[id];
              if (!prev || prog > prev.value || prev.state !== state || downloadedBytes > prev.bytes) {
                lastProgress[id] = { value: prog, ts: now, state, bytes: downloadedBytes };
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
          for (let i = 0; i < videoParts.length; i++) { const partPath = path.join(partsDirPath, `part-${i}.bin`); if (!fs.existsSync(partPath)) throw new Error(`Missing part file ${partPath}`); listLines.push(`file '${partPath.replace(/'/g, "'\\''")}'`); }
          fs.writeFileSync(partsListPath, listLines.join('\n'), 'utf8');
          const videoConcatTmp = path.join(manifestDir, `${job.id}-video-concat.mp4`);
          this.logger.debug(`Merging video parts list ${partsListPath} -> ${videoConcatTmp}`);
          await this.ffmpeg.mergeParts(partsListPath, videoConcatTmp);

          // build audio concat if audio parts present
          let audioConcatTmp: string | null = null;
          if (finalAudioParts && finalAudioParts.length > 0) {
            const audioListPath = path.join(manifestDir, 'audio-parts.txt');
            const audioLines: string[] = [];
            for (let i = 0; i < finalAudioParts.length; i++) { const partPath = path.join(partsDirPath, `audio-part-${i}.bin`); if (!fs.existsSync(partPath)) throw new Error(`Missing audio part file ${partPath}`); audioLines.push(`file '${partPath.replace(/'/g, "'\\''")}'`); }
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

        // Strategy order wants durl-byte-range before dash-single when both are available.
        if (!didSegment && play.data?.durl?.length) {
          const durlUrl = play.data.durl[0]?.url;
          if (durlUrl) {
            this.logger.log(`Job ${job.id}: trying durl-byte-range before dash-single fallback`);
            let useParallel = false; let totalSize = 0;
            try {
              const head = await axios.head(durlUrl, { headers: { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, timeout: 5000 });
              const acceptRanges = String(head.headers['accept-ranges'] || '').toLowerCase();
              const contentLength = parseInt(head.headers['content-length'] || '0', 10) || 0;
              if (contentLength > 0) totalSize = contentLength;
              if (acceptRanges.includes('bytes') && contentLength > 0 && contentLength >= 2 * partSizeBytes && this.partsQueue) {
                let rangeProbeOk = false;
                try {
                  const probe = await axios.get(durlUrl, {
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
              }
            } catch (e) {}

            if (useParallel && totalSize > 0) {
              await this.setTotalExpectedBytes(redisClient, String(job.id), totalSize);
              const partSize = partSizeBytes;
              const partCount = Math.ceil(totalSize / partSize);
              const partsDir = path.join(manifestDir, 'parts');
              if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true });
              if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });
              const parts: any[] = [];
              for (let i = 0; i < partCount; i++) {
                const start = i * partSize;
                const end = Math.min(totalSize - 1, (i + 1) * partSize - 1);
                const expectedBytes = end - start + 1;
                parts.push({ partIndex: i, rangeStart: start, rangeEnd: end, expectedBytes, state: 'pending', downloadedBytes: 0 });
              }

              const manifest = { strategy: 'durl-byte-range', jobId: String(job.id), bvid, title, safeTitle, url: durlUrl, totalExpectedBytes: totalSize, parts };
              fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
              this.logger.log(`Job ${job.id}: created byte-range manifest with ${parts.length} parts (total ${totalSize} bytes)`);
              if (redisClient) { try { const key = `job:parts:${job.id}`; await redisClient.hset(key, 'totalExpectedBytes', String(totalSize)); for (const p of parts) { await redisClient.hset(key, `part:${p.partIndex}:expectedBytes`, String(p.expectedBytes)); await redisClient.hset(key, `part:${p.partIndex}:state`, 'pending'); } } catch (e) {} }
              await this.history.appendEvent(job.id.toString(), { state: 'segmenting', progress: 35, manifestPath });
              const headers = { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies || '' };
              const partJobs = [];
              for (const p of parts) { this.logger.debug(`Enqueue byte-range part job=${job.id} part=${p.partIndex} range=${p.rangeStart}-${p.rangeEnd} expected=${p.expectedBytes}`); const j = await this.partsQueue!.add({ jobId: String(job.id), bvid, url: durlUrl, partIndex: p.partIndex, rangeStart: p.rangeStart, rangeEnd: p.rangeEnd, expectedBytes: p.expectedBytes, headers } as any, { attempts: retryCount, backoff: retryBackoffMs }); partJobs.push(j); }
              const activeStallMs = 45000;
              const queuedStallMs = 10 * 60 * 1000;
              const pollMs = 5000;
              const lastProgress: Record<string, { value: number; ts: number; state: string; bytes: number }> = {};
              let remaining = partJobs.length;
              const durlProgressMin = 35;
              const durlProgressMax = 79;
              await this.reportByteProgress(job, 0, totalSize, durlProgressMin, durlProgressMax);
              while (remaining > 0) {
                remaining = 0;
                for (const pj of partJobs) {
                  const id = String(pj.id);
                  const pjData: any = (pj as any).data || {};
                  const partIndex = typeof pjData.partIndex === 'number' ? pjData.partIndex : undefined;
                  const state = await pj.getState();
                  const prog = (await pj.progress()) as number;
                  const downloadedBytes = await this.readPartDownloadedBytes(redisClient, String(job.id), partIndex, 'video');
                  if (state === 'completed') continue;
                  if (state === 'failed') throw new Error(`Part job ${id} failed`);
                  remaining += 1;
                  const now = Date.now();
                  const prev = lastProgress[id];
                  if (!prev || prog > prev.value || prev.state !== state || downloadedBytes > prev.bytes) {
                    lastProgress[id] = { value: prog, ts: now, state, bytes: downloadedBytes };
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
                const totals = await this.readAggregatedPartBytes(redisClient, String(job.id));
                if (totals.totalExpectedBytes > 0) {
                  await this.reportByteProgress(job, totals.totalDownloadedBytes, totals.totalExpectedBytes, durlProgressMin, durlProgressMax);
                }
                if (remaining > 0) await new Promise((r) => setTimeout(r, pollMs));
              }
              await this.reportByteProgress(job, totalSize, totalSize, durlProgressMin, durlProgressMax);
              await this.history.appendEvent(job.id.toString(), { state: 'merging-parts', progress: 80 });
              const outStream = fs.createWriteStream(tempOutFile, { flags: 'w' });
              for (let i = 0; i < parts.length; i++) { const partPath = path.join(partsDir, `part-${i}.bin`); if (!fs.existsSync(partPath)) throw new Error(`Missing part file ${partPath}`); await new Promise<void>((resolve, reject) => { const rs = fs.createReadStream(partPath); rs.on('error', reject); rs.on('end', () => resolve()); rs.pipe(outStream, { end: false }); }); }
              await new Promise<void>((resolve, reject) => {
                outStream.on('error', reject);
                outStream.on('finish', () => resolve());
                outStream.end();
              });
              await job.progress(95);
              didSegment = true;
              this.logger.log(`Job ${job.id}: strategy selected durl-byte-range (preferred over dash-single)`);
            } else {
              this.logger.debug(`Job ${job.id}: durl-byte-range not applicable; continue to dash-single`);
            }
          }
        }

        // fallback: if we didn't do segmented path, download whole video+audio as before
        if (!didSegment) {
          const singlePermit = await this.acquireSingleDownloadPermit(job);
          try {
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
          let lastDashProgressLogAt = 0;
          try { if (fs.existsSync(videoTmp)) dashDownloaded += fs.statSync(videoTmp).size; } catch (e) {}
          try { if (fs.existsSync(audioTmp)) dashDownloaded += fs.statSync(audioTmp).size; } catch (e) {}
          const onDashProgress = (delta: number) => {
            dashDownloaded += delta;
            const now = Date.now();
            if (dashSingleTotalSize > 0 && now - lastDashProgressLogAt >= 2000) {
              const pct = ((dashDownloaded / dashSingleTotalSize) * 100).toFixed(2);
              this.logger.debug(`Job ${job.id}: single(dash) progress ${dashDownloaded}/${dashSingleTotalSize} bytes (${pct}%)`);
              lastDashProgressLogAt = now;
            }
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
          } finally {
            await this.releaseSingleDownloadPermit(singlePermit);
          }
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
          const singlePermit = await this.acquireSingleDownloadPermit(job);
          try {
          let downloaded = 0;
          let lastSingleProgressLogAt = 0;
          try { if (fs.existsSync(tempOutFile)) downloaded = fs.statSync(tempOutFile).size; } catch (e) {}
          const onProgress = (delta: number) => {
            downloaded += delta;
            const now = Date.now();
            if (totalSize > 0 && now - lastSingleProgressLogAt >= 2000) {
              const pct = ((downloaded / totalSize) * 100).toFixed(2);
              this.logger.debug(`Job ${job.id}: single(durl) progress ${downloaded}/${totalSize} bytes (${pct}%)`);
              lastSingleProgressLogAt = now;
            } else if (!totalSize && now - lastSingleProgressLogAt >= 2000) {
              this.logger.debug(`Job ${job.id}: single(durl) progress downloaded=${downloaded} bytes (total unknown)`);
              lastSingleProgressLogAt = now;
            }
            if (totalSize > 0) {
              void this.reportByteProgress(job, downloaded, totalSize, 30, 95);
            }
          };
          if (totalSize > 0) await this.reportByteProgress(job, downloaded, totalSize, 30, 95);
          await resumeDownload(url, tempOutFile, { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, cancelFile, undefined, onProgress, false, { ...downloadOptions, logger: this.logger });
          } finally {
            await this.releaseSingleDownloadPermit(singlePermit);
          }
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
          const lastProgress: Record<string, { value: number; ts: number; state: string; bytes: number }> = {};
          let remaining = partJobs.length;
          const durlProgressMin = 35;
          const durlProgressMax = 79;
          if (totalSize > 0) await this.reportByteProgress(job, 0, totalSize, durlProgressMin, durlProgressMax);
          while (remaining > 0) {
            remaining = 0;
            for (const pj of partJobs) {
              const id = String(pj.id);
              const pjData: any = (pj as any).data || {};
              const partIndex = typeof pjData.partIndex === 'number' ? pjData.partIndex : undefined;
              const state = await pj.getState();
              const prog = (await pj.progress()) as number;
              const downloadedBytes = await this.readPartDownloadedBytes(redisClient, String(job.id), partIndex, 'video');
              if (state === 'completed') continue;
              if (state === 'failed') throw new Error(`Part job ${id} failed`);
              remaining += 1;
              const now = Date.now();
              const prev = lastProgress[id];
              if (!prev || prog > prev.value || prev.state !== state || downloadedBytes > prev.bytes) {
                lastProgress[id] = { value: prog, ts: now, state, bytes: downloadedBytes };
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
    finally {
      this.releaseMasterSlot();
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

  private async readPartDownloadedBytes(
    redisClient: any,
    jobId: string,
    partIndex: number | undefined,
    role: 'video' | 'audio' = 'video',
  ): Promise<number> {
    if (!redisClient || typeof redisClient.hget !== 'function' || typeof partIndex !== 'number' || partIndex < 0) return 0;
    try {
      const key = `job:parts:${jobId}`;
      const field = role === 'audio' ? `part:audio:${partIndex}:downloadedBytes` : `part:${partIndex}:downloadedBytes`;
      const raw = await redisClient.hget(key, field);
      return parseInt(String(raw || '0'), 10) || 0;
    } catch (e) {
      return 0;
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

  // Folder title rule:
  // - Latin/Vietnamese: remove accents, lowercase, spaces -> '-'
  // - Chinese: keep original text
  private normalizeTitleForFolder(raw: string): string {
    const base = (raw || '').trim().replace(/[\/\\:*?"<>|]/g, '');
    if (!base) return 'untitled';

    const hasCjk = /[\u3400-\u9FFF\uF900-\uFAFF]/.test(base);
    if (hasCjk) return base.substring(0, 80) || 'untitled';

    const folded = base
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/đ/g, 'd')
      .replace(/Đ/g, 'd')
      .toLowerCase();

    const slug = folded
      .replace(/\s+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-+|-+$/g, '');

    return (slug || 'untitled').substring(0, 80);
  }

  private isVietnameseOrEnglishTitle(raw: string): boolean {
    const t = String(raw || '').trim();
    if (!t) return true;
    // CJK / Japanese / Korean / Cyrillic / Greek / Arabic / Hebrew / Thai
    const nonViEnScripts = /[\u3400-\u9FFF\uF900-\uFAFF\u3040-\u30FF\uAC00-\uD7AF\u0400-\u04FF\u0370-\u03FF\u0600-\u06FF\u0590-\u05FF\u0E00-\u0E7F]/;
    return !nonViEnScripts.test(t);
  }

  private async translateToVietnamese(text: string): Promise<string | null> {
    const q = String(text || '').trim();
    if (!q) return null;
    try {
      const resp = await axios.get('https://translate.googleapis.com/translate_a/single', {
        params: {
          client: 'gtx',
          sl: 'auto',
          tl: 'vi',
          dt: 't',
          q,
        },
        timeout: 5000,
      });
      const chunks = Array.isArray(resp.data?.[0]) ? resp.data[0] : [];
      const translated = chunks
        .map((c: any) => (Array.isArray(c) ? String(c[0] || '') : ''))
        .join('')
        .trim();
      return translated || null;
    } catch (e) {
      this.logger.warn(`Failed to translate title to Vietnamese: ${e instanceof Error ? e.message : String(e)}`);
      return null;
    }
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

  private loadCookiesFromConfig(): string {
    const cookieFile = path.join(process.cwd(), 'config', 'cookies.json');
    try {
      if (!fs.existsSync(cookieFile)) return '';
      const raw = fs.readFileSync(cookieFile, 'utf8').trim();
      if (!raw) return '';

      // Support browser-export format: [{ name, value, ... }, ...]
      if (raw.startsWith('[')) {
        const arr = JSON.parse(raw);
        if (Array.isArray(arr)) {
          const pairs = arr
            .map((c: any) => {
              const name = String(c?.name || '').trim();
              const value = String(c?.value || '').trim();
              if (!name) return '';
              return `${name}=${value}`;
            })
            .filter(Boolean);
          return pairs.join('; ');
        }
      }

      // Support object map format: { "SESSDATA": "...", ... }
      if (raw.startsWith('{')) {
        const obj = JSON.parse(raw);
        if (obj && typeof obj === 'object') {
          const pairs = Object.entries(obj)
            .map(([k, v]) => `${String(k).trim()}=${String(v ?? '').trim()}`)
            .filter((s) => !s.startsWith('='));
          return pairs.join('; ');
        }
      }

      // Support plain cookie string
      return raw;
    } catch (e) {
      this.logger.warn(`Failed to load cookies from config/cookies.json: ${e instanceof Error ? e.message : String(e)}`);
      return '';
    }
  }

  private async acquireMasterSlot(job: Job): Promise<void> {
    if (WorkerProcessor.activeMasterJobs < this.masterConcurrency) {
      WorkerProcessor.activeMasterJobs += 1;
      this.logger.debug(`Job ${job.id}: acquired master slot (${WorkerProcessor.activeMasterJobs}/${this.masterConcurrency})`);
      return;
    }
    this.logger.debug(`Job ${job.id}: waiting master slot (${WorkerProcessor.activeMasterJobs}/${this.masterConcurrency})`);
    await new Promise<void>((resolve) => WorkerProcessor.masterWaiters.push(resolve));
    WorkerProcessor.activeMasterJobs += 1;
    this.logger.debug(`Job ${job.id}: acquired master slot after wait (${WorkerProcessor.activeMasterJobs}/${this.masterConcurrency})`);
  }

  private releaseMasterSlot(): void {
    WorkerProcessor.activeMasterJobs = Math.max(0, WorkerProcessor.activeMasterJobs - 1);
    const next = WorkerProcessor.masterWaiters.shift();
    if (next) next();
  }

  private async acquireSingleDownloadPermit(job: Job): Promise<string | null> {
    if (this.globalSingleMaxConcurrentDownloads <= 0) return null;
    const redisClient: any = (this.downloadQueue && (this.downloadQueue as any).client) ? (this.downloadQueue as any).client : null;
    if (!redisClient || typeof redisClient.eval !== 'function') {
      throw new Error(`single-download limiter enabled but redis eval is unavailable for job ${job.id}`);
    }
    const token = `single-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}-${job.id}`;
    while (true) {
      const now = Date.now();
      let ok = 0;
      try {
        ok = Number(await redisClient.eval(
          WorkerProcessor.ACQUIRE_GLOBAL_PERMIT_LUA,
          1,
          WorkerProcessor.GLOBAL_SINGLE_DOWNLOADS_SEMAPHORE_KEY,
          String(now),
          String(this.limiterLeaseMs),
          String(this.globalSingleMaxConcurrentDownloads),
          token,
        )) || 0;
      } catch (e) {
        throw new Error(`single-download limiter acquire failed for job ${job.id}: ${String((e as any)?.message || e)}`);
      }
      if (ok === 1) {
        this.startPermitRenew(token, redisClient);
        this.logger.debug(`Job ${job.id}: acquired single-download permit (${token})`);
        return token;
      }
      await new Promise((r) => setTimeout(r, this.limiterWaitMs));
    }
  }

  private startPermitRenew(token: string, redisClient: any): void {
    const period = Math.max(1000, Math.floor(this.limiterLeaseMs / 3));
    const t = setInterval(async () => {
      try {
        await redisClient.eval(
          WorkerProcessor.RENEW_GLOBAL_PERMIT_LUA,
          1,
          WorkerProcessor.GLOBAL_SINGLE_DOWNLOADS_SEMAPHORE_KEY,
          String(Date.now()),
          String(this.limiterLeaseMs),
          token,
        );
      } catch (e) {
        // ignore renew errors; lease expiry will self-heal on next acquire cleanup
      }
    }, period);
    this.renewTimers.set(token, t as any);
  }

  private async releaseSingleDownloadPermit(token: string | null): Promise<void> {
    if (!token) return;
    const timer = this.renewTimers.get(token);
    if (timer) {
      clearInterval(timer);
      this.renewTimers.delete(token);
    }
    const redisClient: any = (this.downloadQueue && (this.downloadQueue as any).client) ? (this.downloadQueue as any).client : null;
    if (!redisClient || typeof redisClient.eval !== 'function') return;
    try {
      await redisClient.eval(
        WorkerProcessor.RELEASE_GLOBAL_PERMIT_LUA,
        1,
        WorkerProcessor.GLOBAL_SINGLE_DOWNLOADS_SEMAPHORE_KEY,
        token,
      );
    } catch (e) {
      // ignore release errors
    }
  }
}
