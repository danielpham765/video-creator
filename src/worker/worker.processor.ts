import { Injectable, Logger } from '@nestjs/common';
import { Processor, Process, InjectQueue } from '@nestjs/bull';
import { Job, Queue } from 'bull';
import { FfmpegService } from '../ffmpeg/ffmpeg.service';
import { JobHistoryService } from '../jobs/job-history.service';
import { JobArchiveService } from '../jobs/job-archive.service';
import * as fs from 'fs';
import * as path from 'path';
import { spawn } from 'child_process';
import { createInterface } from 'readline';
import resumeDownload, { ResumeDownloadResult } from '../utils/resume-download';
import { formatMb, formatMbProgress } from '../utils/size-format';
import { formatElapsedDuration } from '../utils/duration-format';
import axios from 'axios';
import { MediaPlannerService } from '../source/media-planner.service';
import { ConcretePlatform, MediaMode, Platform, ResolvedSource } from '../source/source.types';
import { SourceRegistryService } from '../source/source-registry.service';
import { buildVideoQualityPolicy } from '../source/video-quality';
import { RuntimeConfigService } from '../config/runtime-config.service';
import { createYtDlpCookieFile, safeUnlink } from '../utils/yt-dlp-cookies';

// data directory is configurable via `config.download.dataDir` (absolute or relative)
// part size now configured in `config.download.partSizeBytes` (MB) and converted at runtime

// Bull processor concurrency should not bottleneck master jobs.
// Real limits are still enforced by master slot + global limiter logic.
const MASTER_PROCESSOR_CONCURRENCY = Math.max(
  1,
  Number(process.env.MASTER_PROCESSOR_CONCURRENCY ?? process.env.WORKER_CONCURRENCY ?? 20),
);

@Processor('downloads')
@Injectable()
export class WorkerProcessor {
  private readonly logger = new Logger(WorkerProcessor.name);
  private static activeMasterJobs = 0;
  private static masterWaiters: Array<() => void> = [];
  private readonly renewTimers = new Map<string, NodeJS.Timeout>();
  private readonly aggregateProgressLogState = new Map<string, { ts: number; downloaded: number }>();
  private readonly resultDir: string;

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
    private readonly ffmpeg: FfmpegService,
    private readonly history: JobHistoryService,
    private readonly archive: JobArchiveService,
    private readonly runtimeConfig: RuntimeConfigService,
    private readonly sourceRegistry: SourceRegistryService,
    private readonly mediaPlanner: MediaPlannerService,
    @InjectQueue('downloads') private readonly downloadQueue?: Queue,
    @InjectQueue('download-parts') private readonly partsQueue?: Queue,
  ) {
    const cfgDataDir = String(this.runtimeConfig.getGlobal('download.dataDir') || path.join(process.cwd(), 'data'));
    this['dataDir'] = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    const cfgResultDir = String(this.runtimeConfig.getGlobal('download.resultDir') || path.join(process.cwd(), 'result'));
    this.resultDir = path.isAbsolute(cfgResultDir) ? cfgResultDir : path.resolve(process.cwd(), cfgResultDir);
    if (!fs.existsSync(this['dataDir'])) fs.mkdirSync(this['dataDir'], { recursive: true });
  }

  @Process({ concurrency: MASTER_PROCESSOR_CONCURRENCY })
  async handleJob(job: Job) {
    const jobStartedAt = Date.now();
    let payload: any = {};
    let requestedEngine: 'auto' | 'yt-dlp' | 'native' = 'auto';
    let downloadEngine: 'yt-dlp' | 'native' = 'native';
    let downloadMode = 'native-unknown';
    let ytDlpFallbackContext: {
      requestUrl: string;
      platform: ConcretePlatform;
      vid: string;
      title: string;
      safeTitle: string;
      mediaEffective: MediaMode;
      cookies: string;
      mediaRequested: MediaMode;
      mediaFallbackReason: string | null;
    } | null = null;
    let nativeAttempted = false;
    const manifestDir = path.join(this['dataDir'], String(job.id));
    const manifestPath = path.join(manifestDir, 'manifest.json');
    await this.acquireMasterSlot(job);
    try {
      this.logger.log('Handling job ' + job.id);
      this.logger.debug(`job.payload=${JSON.stringify(job.data || {})}`);
      payload = job.data || {};
      const requestedPlatform = (payload.platform || 'auto') as Platform;
      const mediaRequested = (payload.mediaRequested || 'both') as MediaMode;
      const requestUrl = String(payload.url || '').trim();
      if (!requestUrl) {
        try { job.discard(); } catch {}
        throw new Error('missing url');
      }
      const concretePlatform = this.sourceRegistry.identifyInput(requestUrl, requestedPlatform).platform;
      const configuredEngineRaw = String(this.runtimeConfig.getForSource(concretePlatform, 'download.engine') || 'auto').toLowerCase();
      const configuredEngine: 'auto' | 'yt-dlp' | 'native' =
        configuredEngineRaw === 'yt-dlp' || configuredEngineRaw === 'native'
          ? (configuredEngineRaw as 'yt-dlp' | 'native')
          : 'auto';
      const requestedEngineRaw = String(payload.engine || '').toLowerCase();
      const payloadEngineValid =
        requestedEngineRaw === 'yt-dlp' || requestedEngineRaw === 'native' || requestedEngineRaw === 'auto';
      const requestedEngineSource: 'payload' | 'config' =
        requestedEngineRaw === 'yt-dlp' || requestedEngineRaw === 'native' ? 'payload' : 'config';
      requestedEngine =
        requestedEngineRaw === 'yt-dlp' || requestedEngineRaw === 'native'
          ? (requestedEngineRaw as 'yt-dlp' | 'native')
          : configuredEngine;
      if (requestedEngineRaw && !payloadEngineValid) {
        this.logger.warn(
          `Job ${job.id}: payload.engine="${requestedEngineRaw}" is invalid; fallback to configured engine "${configuredEngine}"`,
        );
      } else if (requestedEngineRaw === 'auto') {
        this.logger.debug(
          `Job ${job.id}: payload.engine="auto" defers to configured engine "${configuredEngine}"`,
        );
      }
      this.logger.debug(
        `Job ${job.id}: engine strategy resolved requested=${requestedEngine} source=${requestedEngineSource} ` +
          `(payload.engine="${requestedEngineRaw || '<empty>'}", configured="${configuredEngineRaw}"=>${configuredEngine})`,
      );
      const page = Number.isFinite(Number(payload.page)) && Number(payload.page) >= 1
        ? Math.floor(Number(payload.page))
        : 1;
      const cookies = this.runtimeConfig.getCookies(concretePlatform);
      if (!cookies) {
        this.logger.warn(`Job ${job.id}: cookies not found in config/cookies/${concretePlatform}.json; playback may be limited`);
      } else {
        this.logger.debug(`Job ${job.id}: loaded cookies from config/cookies/${concretePlatform}.json (${cookies.length} chars)`);
      }

      await job.progress(5);
      await this.history.appendEvent(job.id.toString(), { state: 'queued', progress: 5 });
      await job.progress(10);
      await this.history.appendEvent(job.id.toString(), { state: 'resolving', progress: 10 });
      const resolvedSource = await this.sourceRegistry.resolve({
        url: requestUrl,
        platform: concretePlatform,
        page,
        title: payload.title,
        cookies,
      });
      const vid = resolvedSource.vid;
      const platform = resolvedSource.platform;
      const mediaPlan = this.mediaPlanner.plan(mediaRequested, resolvedSource);
      await this.history.appendEvent(job.id.toString(), { state: 'media-requested', progress: 10, requested: mediaRequested });
      if (mediaPlan.fallbackReason) {
        this.logger.warn(
          `Job ${job.id}: requested=${mediaRequested} effective=${mediaPlan.effective} reason=\"${mediaPlan.fallbackReason}\"`,
        );
        await this.history.appendEvent(job.id.toString(), {
          state: 'media-fallback',
          progress: 10,
          requested: mediaRequested,
          effective: mediaPlan.effective,
          reason: mediaPlan.fallbackReason,
        });
      }
      await this.history.appendEvent(job.id.toString(), { state: 'media-effective', progress: 10, effective: mediaPlan.effective });
      await this.history.appendEvent(job.id.toString(), { state: 'engine-requested', progress: 10, engine: requestedEngine });
      const markDownloadMode = async (engine: 'yt-dlp' | 'native', mode: string, reason?: string): Promise<void> => {
        if (downloadEngine === engine && downloadMode === mode) return;
        const prevEngine = downloadEngine;
        const prevMode = downloadMode;
        downloadEngine = engine;
        downloadMode = mode;
        this.logger.debug(
          `Job ${job.id}: download mode transition ${prevEngine}/${prevMode} -> ${downloadEngine}/${downloadMode}` +
            `${reason ? ` reason="${reason}"` : ''}`,
        );
        await this.history.appendEvent(job.id.toString(), {
          state: 'download-mode',
          progress: Number(await job.progress()) || 0,
          downloadEngine,
          downloadMode,
          fromEngine: prevEngine,
          fromMode: prevMode,
          reason: reason || null,
        });
      };

      const commonHeaders: Record<string, string> = { ...(resolvedSource.headers || {}) };
      if (cookies && !commonHeaders.Cookie) commonHeaders.Cookie = cookies;
      const play = this.toLegacyPlayShape(resolvedSource);
      this.logger.debug('Resolved source streams: ' + JSON.stringify(Object.keys(play?.data || {})));
      const qualityPolicy = buildVideoQualityPolicy(
        this.runtimeConfig.getForSource(platform, 'download.preferVideoQuality'),
      );
      const resolvedQn = Number(resolvedSource?.qualityMeta?.qn ?? play?.data?.quality ?? 0);
      if (resolvedQn > 0 && resolvedQn < qualityPolicy.minAcceptableQn) {
        throw new Error(
          `resolved quality qn=${resolvedQn} is below accepted minimum ` +
            `${qualityPolicy.minAcceptableQn} for prefer=${qualityPolicy.preferredLabel}`,
        );
      }
      this.logger.debug(
        `Job ${job.id}: resolved playurl quality qn=${resolvedQn} (prefer=${qualityPolicy.preferredLabel}, ` +
          `accepted=${qualityPolicy.minAcceptableQn}-${qualityPolicy.preferredQn})`,
      );

      // If caller provides title, always use it.
      // Otherwise fetch title from source, optionally translate to Vietnamese
      // when detected language is not Vietnamese/English, then normalize.
      const passedTitle = typeof payload.title === 'string' ? payload.title.trim() : '';
      if (passedTitle) {
        this.logger.debug(`Job ${job.id}: using passed title="${passedTitle}"`);
      } else {
        this.logger.debug(`Job ${job.id}: no passed title; will fetch title from source`);
      }
      let title = passedTitle || resolvedSource.title || vid;
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
      ytDlpFallbackContext = {
        requestUrl,
        platform,
        vid,
        title,
        safeTitle,
        mediaEffective: mediaPlan.effective,
        cookies,
        mediaRequested,
        mediaFallbackReason: mediaPlan.fallbackReason || null,
      };
      if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true });

      // Compute part size (configured as MB in config.download.partSizeBytes)
      const partSizeMB = Number(this.runtimeConfig.getForSource(platform, 'download.partSizeBytes') ?? 8);
      const partSizeBytes = Math.max(1, Math.floor(partSizeMB)) * 1024 * 1024;
      this.logger.debug(`Using partSizeBytes=${partSizeBytes} (${partSizeMB} MB) for job ${job.id}`);

      // Download options (timeout + proxy) from config
      const downloadTimeoutMs = Number(this.runtimeConfig.getForSource(platform, 'download.timeoutMs') ?? 30000);
      const proxyUrl = String(this.runtimeConfig.getForSource(platform, 'proxy.http') || this.runtimeConfig.getForSource(platform, 'proxy.https') || '');
      const downloadOptions = { timeoutMs: downloadTimeoutMs, proxy: proxyUrl || undefined };
      const retryCount = Number(this.runtimeConfig.getForSource(platform, 'download.retryCount') ?? 3);
      const retryBackoffMs = Number(this.runtimeConfig.getForSource(platform, 'download.retryBackoffMs') ?? 5000);
      const resumeEnabled = Boolean(this.runtimeConfig.getForSource(platform, 'download.resumeEnabled') ?? true);

      const stopKey = `job:stop:${job.id}`;
      const cancelKey = `job:cancel:${job.id}`;
      let stopRequested = false;
      let cancelRequested = false;
      const abortRequested = () => stopRequested || cancelRequested;
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

      if (requestedEngine === 'yt-dlp') {
        await markDownloadMode('yt-dlp', 'yt-dlp-direct', 'engine explicitly resolved to yt-dlp');
        const ytDlpResult = await this.tryDirectDownloadViaYtDlp({
          job,
          requestUrl,
          platform,
          vid,
          title,
          safeTitle,
          mediaEffective: mediaPlan.effective,
          cookies,
          manifestDir,
          stopRequested: () => stopRequested,
          cancelRequested: () => cancelRequested,
        });
        if (ytDlpResult?.stopped) {
          await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
          this.logger.log(`Job ${job.id} stopped during yt-dlp download`);
          try { if (redisClient) await redisClient.del(stopKey); } catch (e) {}
          if (_poll) clearInterval(_poll);
          try {
            if (subClient) {
              if (typeof subClient.unsubscribe === 'function') {
                try { await subClient.unsubscribe(stopKey); } catch {}
                try { await subClient.unsubscribe(cancelKey); } catch {}
              }
              if (typeof subClient.disconnect === 'function') await subClient.disconnect();
              if (typeof subClient.quit === 'function') await subClient.quit();
            }
          } catch (e) {}
          return { stopped: true };
        }
        if (ytDlpResult?.cancelled) {
          await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
          this.logger.log(`Job ${job.id} cancelled during yt-dlp download`);
          if (_poll) clearInterval(_poll);
          try {
            if (subClient) {
              if (typeof subClient.unsubscribe === 'function') {
                try { await subClient.unsubscribe(stopKey); } catch {}
                try { await subClient.unsubscribe(cancelKey); } catch {}
              }
              if (typeof subClient.disconnect === 'function') await subClient.disconnect();
              if (typeof subClient.quit === 'function') await subClient.quit();
            }
          } catch (e) {}
          return { cancelled: true };
        }
        if (ytDlpResult?.path) {
          await job.progress(100);
          await this.history.appendEvent(job.id.toString(), {
            state: 'finished',
            progress: 100,
            result: {
              path: ytDlpResult.path,
              platform,
              vid,
              title,
              mediaRequested,
              mediaEffective: mediaPlan.effective,
              mediaFallbackReason: mediaPlan.fallbackReason || null,
              downloadEngine,
              downloadMode,
            },
          });
          this.cleanupJobTempArtifacts(String(job.id), ytDlpResult.path);
          this.logger.log(
            `Job ${job.id} finished: output=${ytDlpResult.path} engine=yt-dlp ` +
            `elapsed=${formatElapsedDuration(Date.now() - jobStartedAt)}`,
          );
          return {
            path: ytDlpResult.path,
            platform,
            vid,
            title,
            mediaRequested,
            mediaEffective: mediaPlan.effective,
            mediaFallbackReason: mediaPlan.fallbackReason || null,
            downloadEngine,
            downloadMode,
          };
        }
        if (requestedEngine === 'yt-dlp') {
          throw new Error('yt-dlp engine requested but direct download failed');
        }
      } else {
        nativeAttempted = true;
        await markDownloadMode(
          'native',
          requestedEngine === 'native' ? 'native-forced' : 'native-primary',
          requestedEngine === 'native'
            ? 'engine explicitly resolved to native'
            : 'engine=auto prefers native before yt-dlp fallback',
        );
      }

      if (play.data?.dash) {
        await markDownloadMode('native', 'dash-single', 'DASH stream detected; start with dash-single baseline');
        const selectedDash = this.selectDashTracks(play.data.dash, qualityPolicy.preferredQn, qualityPolicy.minAcceptableQn);
        const videoUrl = selectedDash.videoUrl;
        const audioUrl = selectedDash.audioUrl;
        if (!videoUrl || !audioUrl) throw new Error('dash urls missing');
        const videoTmp = path.join(this['dataDir'], `${job.id}-video`);
        const audioTmp = path.join(this['dataDir'], `${job.id}-audio`);
        const tempOutFile = path.join(this['dataDir'], `${vid}-${job.id}.mp4`);
        this.adoptResumeArtifacts(job, payload, vid, videoTmp, audioTmp, tempOutFile);

        // Persist a minimal manifest so that later tools (including partial merge)
        // know basic metadata even for non-partitioned DASH jobs.
        try {
          const dashManifest = {
            strategy: 'dash-single',
            jobId: String(job.id),
            platform,
            vid,
            title,
            safeTitle,
            mediaRequested,
            mediaEffective: mediaPlan.effective,
            mediaFallbackReason: mediaPlan.fallbackReason || null,
            totalExpectedBytes: 0,
            parts: [] as any[],
          };
          fs.writeFileSync(manifestPath, JSON.stringify(dashManifest, null, 2), 'utf8');
        } catch (e) {}

        if (cancelRequested) {
          await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
          this.logger.log(`Job ${job.id} cancelled before download start`);
          return { cancelled: true };
        }
        if (stopRequested) {
          await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
          this.logger.log(`Job ${job.id} stopped before download start`);
          try { if (redisClient) await redisClient.del(stopKey); } catch (e) {}
          // no local stop marker cleanup; Redis signal key is authoritative
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
          const manifestResp = await axios.get(videoUrl, { headers: commonHeaders, timeout: 5000, responseType: 'text', maxContentLength: 128 * 1024, maxBodyLength: 128 * 1024 });
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
          const manifestRespA = await axios.get(audioUrl, { headers: commonHeaders, timeout: 5000, responseType: 'text', maxContentLength: 128 * 1024, maxBodyLength: 128 * 1024 });
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
          await markDownloadMode('native', 'dash-segmented', 'manifest segments detected (HLS/MPD); prefer segmented strategy');
          const partsDir = path.join(manifestDir, 'parts'); if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true }); if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });

          const segments: Array<{ url: string; approxBytes: number }> = [];
          for (let i = 0; i < segmentList.length; i++) {
            const su = segmentList[i]; let approx = 0; try { const h = await axios.head(su, { headers: commonHeaders, timeout: 5000 }); approx = parseInt(h.headers['content-length'] || '0', 10) || 0; } catch (e) { approx = 0; }
            segments.push({ url: su, approxBytes: approx });
          }

          this.logger.debug(`Collected ${segments.length} segments (video)`);

          const audioSegments: Array<{ url: string; approxBytes: number }> = [];
          if (Array.isArray(audioSegmentList) && audioSegmentList.length > 0) {
            for (let i = 0; i < audioSegmentList.length; i++) {
              const su = audioSegmentList[i];
              let approx = 0;
              try {
                const h = await axios.head(su, { headers: commonHeaders, timeout: 5000 });
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
          const manifest = {
            strategy: 'dash-segmented',
            jobId: String(job.id),
            platform,
            vid,
            title,
            safeTitle,
            mediaRequested,
            mediaEffective: mediaPlan.effective,
            mediaFallbackReason: mediaPlan.fallbackReason || null,
            totalExpectedBytes,
            parts: videoParts,
          };
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

          const headers = { ...commonHeaders };
          const totalPartJobs = videoParts.length + finalAudioParts.length;
          const partJobs: Job[] = [];
          for (const p of videoParts) {
            this.logger.debug(`Enqueue video part job=${job.id} part=${p.partIndex} expectedBytes=${p.expectedBytes}`);
            const j = await this.partsQueue!.add({ jobId: String(job.id), vid, platform, totalJobCount: totalPartJobs, segmentUrls: p.segmentUrls, partIndex: p.partIndex, expectedBytes: p.expectedBytes || 0, headers, role: 'video' } as any, { attempts: retryCount, backoff: retryBackoffMs });
            await this.indexPartJob(redisClient, String(job.id), String(j.id));
            partJobs.push(j);
          }

          const audioPartJobs: Job[] = [];
          if (finalAudioParts.length > 0) {
            for (const p of finalAudioParts) {
              this.logger.debug(`Enqueue audio part job=${job.id} part=${p.partIndex} expectedBytes=${p.expectedBytes}`);
              const j = await this.partsQueue!.add({ jobId: String(job.id), vid, platform, totalJobCount: totalPartJobs, segmentUrls: p.segmentUrls, partIndex: p.partIndex, expectedBytes: p.expectedBytes || 0, headers, role: 'audio' } as any, { attempts: retryCount, backoff: retryBackoffMs });
              await this.indexPartJob(redisClient, String(job.id), String(j.id));
              audioPartJobs.push(j);
            }
          }

          // Wait for all part jobs to complete with stall detection.
          // Important: do not treat waiting/delayed jobs as stalled just because they are queued.
          const activeStallMs = Math.max(30000, Number(this.runtimeConfig.getForSource(platform, 'download.partActiveStallMs') ?? 180000));
          const pollMs = 5000;
          const lastProgress: Record<string, { value: number; ts: number; state: string; bytes: number }> = {};
          let remaining = partJobs.length + (Array.isArray(audioPartJobs) ? audioPartJobs.length : 0);
          const segmentedProgressMin = 30;
          const segmentedProgressMax = 74;
          if (totalExpectedBytes > 0) {
            await this.reportByteProgress(job, 0, totalExpectedBytes, segmentedProgressMin, segmentedProgressMax);
          }
          while (remaining > 0) {
            if (cancelRequested) {
              await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
              this.logger.log(`Job ${job.id} cancelled during segmented download`);
              return { cancelled: true };
            }
            if (stopRequested) {
              await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
              this.logger.log(`Job ${job.id} stopped during segmented download`);
              try { if (redisClient) await redisClient.del(stopKey); } catch (e) {}
              // no local stop marker cleanup; Redis signal key is authoritative
              return { stopped: true };
            }
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
                if (state !== 'active') {
                  // waiting/delayed/paused jobs can stay queued for a long time;
                  // only treat no-progress as stalled while a part is actively downloading.
                  lastProgress[id] = { value: prog, ts: now, state, bytes: downloadedBytes };
                } else if (now - prev.ts >= activeStallMs) {
                  await this.history.appendEvent(job.id.toString(), { state: 'failed', progress: 0, message: `Part job ${id} stalled at ${prog}% (state=${state})` });
                  throw new Error(`Part job ${id} stalled (state=${state})`);
                }
              }
            }
            if (totalExpectedBytes > 0) {
              const totals = await this.readAggregatedPartBytes(redisClient, String(job.id));
              if (totals.totalExpectedBytes > 0) {
                this.logAggregateProgress(String(job.id), 'dash-segmented', totals.totalDownloadedBytes, totals.totalExpectedBytes);
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
                await this.resumeDownloadWithRetry(
                  `${job.id} segmented-audio-fallback`,
                  audioUrl,
                  audioTmp,
                  commonHeaders,
                  abortRequested,
                  undefined,
                  undefined,
                  downloadOptions,
                  retryCount,
                  retryBackoffMs,
                );
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

        // Strategy order: dash-segmented -> dash-byte-range -> durl-byte-range -> dash-single.
        if (!didSegment && this.partsQueue) {
          const headers = { ...commonHeaders };
          const [videoProbe, audioProbe] = await Promise.all([
            this.probeByteRangeParallel(videoUrl, headers, partSizeBytes),
            this.probeByteRangeParallel(audioUrl, headers, partSizeBytes),
          ]);
          const useVideoParallel = videoProbe.useParallel && videoProbe.totalSize > 0;
          const useAudioParallel = audioProbe.useParallel && audioProbe.totalSize > 0;
          if (useVideoParallel && audioProbe.totalSize > 0) {
            await markDownloadMode('native', 'dash-byte-range', 'segmented not used; DASH supports byte-range parallelization');
            const partsDir = path.join(manifestDir, 'parts');
            if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true });
            if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });

            const videoParts = this.buildByteRangeParts(videoProbe.totalSize, partSizeBytes);
            const audioParts = useAudioParallel ? this.buildByteRangeParts(audioProbe.totalSize, partSizeBytes) : [];
            const totalExpectedBytes = videoProbe.totalSize + audioProbe.totalSize;
            await this.setTotalExpectedBytes(redisClient, String(job.id), totalExpectedBytes);

            const manifest = {
              strategy: 'dash-byte-range',
              jobId: String(job.id),
              vid,
              title,
              safeTitle,
              totalExpectedBytes,
              video: { url: videoUrl, totalExpectedBytes: videoProbe.totalSize, parts: videoParts },
              audio: { url: audioUrl, totalExpectedBytes: audioProbe.totalSize, parts: audioParts },
            };
            fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
            this.logger.log(
              `Job ${job.id}: strategy selected dash-byte-range ` +
              `(videoParts=${videoParts.length}, audioParts=${audioParts.length}, audioMode=${useAudioParallel ? 'parallel' : 'single'})`,
            );

            let initialDownloadedBytes = 0;
            const migrated = this.adoptRangePartsFromPreviousJob(job, payload, partsDir, videoParts, audioParts);
            if (migrated.videoCount > 0 || migrated.audioCount > 0) {
              this.logger.log(
                `Job ${job.id}: adopted dash-byte-range parts from job ${migrated.fromJobId} (video=${migrated.videoCount}, audio=${migrated.audioCount}, bytes=${migrated.downloadedBytes})`,
              );
            }
            initialDownloadedBytes = migrated.downloadedBytes;

            if (redisClient) {
              try {
                const key = `job:parts:${job.id}`;
                await redisClient.hset(key, 'totalExpectedBytes', String(totalExpectedBytes));
                // Avoid blocking the master worker with thousands of sequential HSET calls.
                // Parts processor will update per-part fields when jobs actually run.
              } catch (e) {}
            }

            await this.history.appendEvent(job.id.toString(), { state: 'segmenting', progress: 30, manifestPath });
            const totalPartJobs = videoParts.length + audioParts.length;
            const partJobs: Job[] = [];
            for (const p of videoParts) {
              const partPath = path.join(partsDir, `part-${p.partIndex}.bin`);
              const done = this.getCompletedPartBytes(partPath, p.expectedBytes);
              if (done > 0) {
                if (redisClient) {
                  try {
                    const key = `job:parts:${job.id}`;
                    const prefix = `part:${p.partIndex}`;
                    await redisClient.hset(key, `${prefix}:expectedBytes`, String(p.expectedBytes));
                    await redisClient.hset(key, `${prefix}:downloadedBytes`, String(done));
                    await redisClient.hset(key, `${prefix}:state`, 'completed');
                  } catch (e) {}
                }
                continue;
              }
              const j = await this.partsQueue.add(
                { jobId: String(job.id), vid, totalJobCount: totalPartJobs, url: videoUrl, partIndex: p.partIndex, rangeStart: p.rangeStart, rangeEnd: p.rangeEnd, expectedBytes: p.expectedBytes, headers, role: 'video' } as any,
                { attempts: retryCount, backoff: retryBackoffMs },
              );
              await this.indexPartJob(redisClient, String(job.id), String(j.id));
              partJobs.push(j);
            }
            const audioPartJobs: Job[] = [];
            if (useAudioParallel) {
              for (const p of audioParts) {
                const partPath = path.join(partsDir, `audio-part-${p.partIndex}.bin`);
                const done = this.getCompletedPartBytes(partPath, p.expectedBytes);
                if (done > 0) {
                  if (redisClient) {
                    try {
                      const key = `job:parts:${job.id}`;
                      const prefix = `part:audio:${p.partIndex}`;
                      await redisClient.hset(key, `${prefix}:expectedBytes`, String(p.expectedBytes));
                      await redisClient.hset(key, `${prefix}:downloadedBytes`, String(done));
                      await redisClient.hset(key, `${prefix}:state`, 'completed');
                    } catch (e) {}
                  }
                  continue;
                }
                const j = await this.partsQueue.add(
                  { jobId: String(job.id), vid, totalJobCount: totalPartJobs, url: audioUrl, partIndex: p.partIndex, rangeStart: p.rangeStart, rangeEnd: p.rangeEnd, expectedBytes: p.expectedBytes, headers, role: 'audio' } as any,
                  { attempts: retryCount, backoff: retryBackoffMs },
                );
                await this.indexPartJob(redisClient, String(job.id), String(j.id));
                audioPartJobs.push(j);
              }
            }

            const activeStallMs = Math.max(30000, Number(this.runtimeConfig.getForSource(platform, 'download.partActiveStallMs') ?? 180000));
            const pollMs = 5000;
            const lastProgress: Record<string, { value: number; ts: number; state: string; bytes: number }> = {};
            let remaining = partJobs.length + audioPartJobs.length;
            const dashByteRangeProgressMin = 30;
            const dashByteRangeProgressMax = 74;
            await this.reportByteProgress(job, initialDownloadedBytes, totalExpectedBytes, dashByteRangeProgressMin, dashByteRangeProgressMax);
            while (remaining > 0) {
              if (cancelRequested) {
                await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
                this.logger.log(`Job ${job.id} cancelled during dash-byte-range download`);
                return { cancelled: true };
              }
              if (stopRequested) {
                await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
                this.logger.log(`Job ${job.id} stopped during dash-byte-range download`);
                try { if (redisClient) await redisClient.del(stopKey); } catch (e) {}
                // no local stop marker cleanup; Redis signal key is authoritative
                return { stopped: true };
              }
              remaining = 0;
              for (const pj of [...partJobs, ...audioPartJobs]) {
                const id = String(pj.id);
                const pjData: any = (pj as any).data || {};
                const partIndex = typeof pjData.partIndex === 'number' ? pjData.partIndex : undefined;
                const role: 'video' | 'audio' = pjData.role === 'audio' ? 'audio' : 'video';
                const state = await pj.getState();
                const prog = (await pj.progress()) as number;
                const downloadedBytes = await this.readPartDownloadedBytes(redisClient, String(job.id), partIndex, role);
                if (state === 'completed') continue;
                if (state === 'failed') throw new Error(`Part job ${id} failed`);
                remaining += 1;
                const now = Date.now();
                const prev = lastProgress[id];
                if (!prev || prog > prev.value || prev.state !== state || downloadedBytes > prev.bytes) {
                  lastProgress[id] = { value: prog, ts: now, state, bytes: downloadedBytes };
                } else {
                  if (state !== 'active') {
                    lastProgress[id] = { value: prog, ts: now, state, bytes: downloadedBytes };
                  } else if (now - prev.ts >= activeStallMs) {
                    await this.history.appendEvent(job.id.toString(), { state: 'failed', progress: 0, message: `Part job ${id} stalled at ${prog}% (state=${state})` });
                    throw new Error(`Part job ${id} stalled (state=${state})`);
                  }
                }
              }
              const totals = await this.readAggregatedPartBytes(redisClient, String(job.id));
              if (totals.totalExpectedBytes > 0) {
                const effectiveDownloaded = Math.max(
                  Math.max(0, initialDownloadedBytes),
                  Math.max(0, totals.totalDownloadedBytes),
                );
                this.logAggregateProgress(String(job.id), 'dash-byte-range', effectiveDownloaded, totals.totalExpectedBytes);
                await this.reportByteProgress(job, effectiveDownloaded, totals.totalExpectedBytes, dashByteRangeProgressMin, dashByteRangeProgressMax);
              }
              if (remaining > 0) await new Promise((r) => setTimeout(r, pollMs));
            }
            await this.reportByteProgress(job, totalExpectedBytes, totalExpectedBytes, dashByteRangeProgressMin, dashByteRangeProgressMax);

            await this.history.appendEvent(job.id.toString(), { state: 'merging-parts', progress: 75 });
            const partsDirPath = path.join(manifestDir, 'parts');
            const videoConcatTmp = path.join(manifestDir, `${job.id}-video-concat.mp4`);
            let audioConcatTmp = path.join(manifestDir, `${job.id}-audio-concat.mp4`);
            this.logger.debug(`Job ${job.id}: concatenating byte-range video parts (${videoParts.length}) -> ${videoConcatTmp}`);
            await this.concatByteRangeParts(partsDirPath, 'part-', videoParts.length, videoConcatTmp);
            if (useAudioParallel) {
              this.logger.debug(`Job ${job.id}: concatenating byte-range audio parts (${audioParts.length}) -> ${audioConcatTmp}`);
              await this.concatByteRangeParts(partsDirPath, 'audio-part-', audioParts.length, audioConcatTmp);
            } else {
              await job.progress(85);
              await this.history.appendEvent(job.id.toString(), { state: 'downloading-audio', progress: 85 });
              if (!resumeEnabled && fs.existsSync(audioTmp)) { try { fs.unlinkSync(audioTmp); } catch (e) {} }
              await this.resumeDownloadWithRetry(
                `${job.id} dash-byte-range-audio-fallback`,
                audioUrl,
                audioTmp,
                commonHeaders,
                abortRequested,
                undefined,
                undefined,
                downloadOptions,
                retryCount,
                retryBackoffMs,
              );
              audioConcatTmp = audioTmp;
            }

            await job.progress(90);
            await this.history.appendEvent(job.id.toString(), { state: 'merging', progress: 90 });
            await this.ffmpeg.merge(videoConcatTmp, audioConcatTmp, tempOutFile);
            try { fs.unlinkSync(videoConcatTmp); } catch (e) {}
            try { if (audioConcatTmp !== audioTmp) fs.unlinkSync(audioConcatTmp); } catch (e) {}
            didSegment = true;
          } else {
            const nextStrategy = play.data?.durl?.length ? 'durl-byte-range' : 'dash-single';
            this.logger.debug(
              `Job ${job.id}: dash-byte-range not applicable; continue strategy fallback ` +
              `(video: useParallel=${videoProbe.useParallel}, size=${videoProbe.totalSize}; ` +
              `audio: useParallel=${audioProbe.useParallel}, size=${audioProbe.totalSize}; partSizeBytes=${partSizeBytes}; next=${nextStrategy})`,
            );
          }
        }

        // Strategy order wants durl-byte-range before dash-single when both are available.
        if (!didSegment && play.data?.durl?.length) {
          const durlUrl = play.data.durl[0]?.url;
          if (durlUrl) {
            this.logger.log(`Job ${job.id}: strategy fallback dash-byte-range -> durl-byte-range (preferred over dash-single)`);
            let useParallel = false; let totalSize = 0;
            try {
              const head = await axios.head(durlUrl, { headers: commonHeaders, timeout: 5000 });
              const acceptRanges = String(head.headers['accept-ranges'] || '').toLowerCase();
              const contentLength = parseInt(head.headers['content-length'] || '0', 10) || 0;
              if (contentLength > 0) totalSize = contentLength;
              if (acceptRanges.includes('bytes') && contentLength > 0 && contentLength >= 2 * partSizeBytes && this.partsQueue) {
                let rangeProbeOk = false;
                try {
                  const probe = await axios.get(durlUrl, {
                    headers: { ...commonHeaders, Range: 'bytes=0-1' },
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

              const manifest = {
                strategy: 'durl-byte-range',
                jobId: String(job.id),
                platform,
                vid,
                title,
                safeTitle,
                mediaRequested,
                mediaEffective: mediaPlan.effective,
                mediaFallbackReason: mediaPlan.fallbackReason || null,
                url: durlUrl,
                totalExpectedBytes: totalSize,
                parts,
              };
              fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
              this.logger.log(`Job ${job.id}: created byte-range manifest with ${parts.length} parts (total ${totalSize} bytes)`);
              if (redisClient) { try { const key = `job:parts:${job.id}`; await redisClient.hset(key, 'totalExpectedBytes', String(totalSize)); for (const p of parts) { await redisClient.hset(key, `part:${p.partIndex}:expectedBytes`, String(p.expectedBytes)); await redisClient.hset(key, `part:${p.partIndex}:state`, 'pending'); } } catch (e) {} }
              await this.history.appendEvent(job.id.toString(), { state: 'segmenting', progress: 35, manifestPath });
              const headers = { ...commonHeaders };
              const partJobs = [];
              for (const p of parts) { this.logger.debug(`Enqueue byte-range part job=${job.id} part=${p.partIndex} range=${p.rangeStart}-${p.rangeEnd} expected=${p.expectedBytes}`); const j = await this.partsQueue!.add({ jobId: String(job.id), vid, platform, totalJobCount: parts.length, url: durlUrl, partIndex: p.partIndex, rangeStart: p.rangeStart, rangeEnd: p.rangeEnd, expectedBytes: p.expectedBytes, headers } as any, { attempts: retryCount, backoff: retryBackoffMs }); await this.indexPartJob(redisClient, String(job.id), String(j.id)); partJobs.push(j); }
              const activeStallMs = Math.max(30000, Number(this.runtimeConfig.getForSource(platform, 'download.partActiveStallMs') ?? 180000));
              const pollMs = 5000;
              const lastProgress: Record<string, { value: number; ts: number; state: string; bytes: number }> = {};
              let remaining = partJobs.length;
              const durlProgressMin = 35;
              const durlProgressMax = 79;
              await this.reportByteProgress(job, 0, totalSize, durlProgressMin, durlProgressMax);
              while (remaining > 0) {
                if (cancelRequested) {
                  await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
                  this.logger.log(`Job ${job.id} cancelled during durl-byte-range download`);
                  return { cancelled: true };
                }
                if (stopRequested) {
                  await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
                  this.logger.log(`Job ${job.id} stopped during durl-byte-range download`);
                  try { if (redisClient) await redisClient.del(stopKey); } catch (e) {}
                  // no local stop marker cleanup; Redis signal key is authoritative
                  return { stopped: true };
                }
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
                    if (state !== 'active') {
                      lastProgress[id] = { value: prog, ts: now, state, bytes: downloadedBytes };
                    } else if (now - prev.ts >= activeStallMs) {
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
                  this.logAggregateProgress(String(job.id), 'durl-byte-range', totals.totalDownloadedBytes, totals.totalExpectedBytes);
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
              await markDownloadMode(
                'native',
                'durl-byte-range',
                'strategy upgraded from durl-single baseline: DURL byte-range is supported (from DASH fallback path)',
              );
              this.logger.log(`Job ${job.id}: strategy selected durl-byte-range (preferred over dash-single)`);
            } else {
              this.logger.debug(
                `Job ${job.id}: durl-byte-range not applicable (useParallel=${useParallel}, totalSize=${totalSize}); ` +
                `fallback to dash-single`,
              );
            }
          }
        }

        // fallback: if we didn't do segmented path, download whole video+audio as before
        if (!didSegment) {
          this.logger.debug(`Job ${job.id}: strategy fallback to dash-single (segmented/byte-range paths not applicable)`);
          const singlePermit = await this.acquireSingleDownloadPermit(job);
          try {
          if (cancelRequested) { await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 }); this.logger.log(`Job ${job.id} cancelled before download start`); return { cancelled: true }; }
          let dashSingleTotalSize = 0;
          try {
            const vh = await axios.head(videoUrl, { headers: commonHeaders, timeout: 5000 });
            dashSingleTotalSize += parseInt(vh.headers['content-length'] || '0', 10) || 0;
          } catch (e) {}
          try {
            const ah = await axios.head(audioUrl, { headers: commonHeaders, timeout: 5000 });
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
              this.logger.debug(
                `Job ${job.id}: single(dash) progress ${formatMbProgress(dashDownloaded, dashSingleTotalSize)} (${pct}%)`,
              );
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
            const videoStartedAt = Date.now();
            await this.resumeDownloadWithRetry(
              `${job.id} dash-video`,
              videoUrl,
              videoTmp,
              commonHeaders,
              abortRequested,
              undefined,
              onDashProgress,
              downloadOptions,
              retryCount,
              retryBackoffMs,
            );
            const videoSize = fs.existsSync(videoTmp) ? fs.statSync(videoTmp).size : 0;
            this.logger.log(
              `Job ${job.id} video completed (${formatMb(videoSize)} MB) ` +
              `elapsed=${formatElapsedDuration(Date.now() - videoStartedAt)}`,
            );
          } catch (err: any) {
            if (String(err?.message || '').toLowerCase().includes('stopped')) { await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 }); this.logger.log(`Job ${job.id} stopped during video download`); try { if (redisClient) await redisClient.del(stopKey); } catch (e) {} if (_poll) clearInterval(_poll); try { if (subClient) { if (typeof subClient.unsubscribe === 'function') { try { await subClient.unsubscribe(stopKey); } catch {} try { await subClient.unsubscribe(cancelKey); } catch {} } if (typeof subClient.disconnect === 'function') await subClient.disconnect(); if (typeof subClient.quit === 'function') await subClient.quit(); } } catch (e) {} return { stopped: true }; }
            throw err;
          }
          await job.progress(50);
          await this.history.appendEvent(job.id.toString(), { state: 'downloading-audio', progress: 50 });
            try {
              if (!resumeEnabled && fs.existsSync(audioTmp)) { try { fs.unlinkSync(audioTmp); } catch (e) {} }
              const audioStartedAt = Date.now();
              await this.resumeDownloadWithRetry(
                `${job.id} dash-audio`,
                audioUrl,
                audioTmp,
                commonHeaders,
                abortRequested,
                undefined,
                onDashProgress,
                downloadOptions,
                retryCount,
                retryBackoffMs,
              );
              const audioSize = fs.existsSync(audioTmp) ? fs.statSync(audioTmp).size : 0;
              this.logger.log(
                `Job ${job.id} audio completed (${formatMb(audioSize)} MB) ` +
                `elapsed=${formatElapsedDuration(Date.now() - audioStartedAt)}`,
              );
            } catch (err: any) { if (String(err?.message || '').toLowerCase().includes('stopped')) { await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 }); this.logger.log(`Job ${job.id} stopped during audio download`); try { if (redisClient) await redisClient.del(stopKey); } catch (e) {} if (_poll) clearInterval(_poll); return { stopped: true }; } throw err; }
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

        const preparedOutput = await this.applyMediaModeOutput(tempOutFile, manifestDir, String(job.id), mediaPlan.effective);
        const ext = mediaPlan.effective === 'audio' ? 'm4a' : 'mp4';
        // Move final file into <resultDir>/<platform>/<title>/<vid>-<jobId>.<ext>
        const finalDir = path.join(this.resultDir, platform, safeTitle);
        try { if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true }); } catch (e) {}
        const finalPath = fs.existsSync(finalDir) ? path.join(finalDir, `${vid}-${job.id}.${ext}`) : preparedOutput;
        if (finalPath !== preparedOutput) { try { fs.renameSync(preparedOutput, finalPath); } catch (e) {} }

        await job.progress(100);
        await this.history.appendEvent(job.id.toString(), {
          state: 'finished',
          progress: 100,
          result: {
            path: finalPath,
            platform,
            vid,
            title,
            mediaRequested,
            mediaEffective: mediaPlan.effective,
            mediaFallbackReason: mediaPlan.fallbackReason || null,
            downloadEngine,
            downloadMode,
          },
        });
        this.cleanupJobTempArtifacts(String(job.id), finalPath);
        this.logger.log(
          `Job ${job.id} finished: output=${finalPath} engine=${downloadEngine} mode=${downloadMode} ` +
          `elapsed=${formatElapsedDuration(Date.now() - jobStartedAt)}`,
        );
        return {
          path: finalPath,
          platform,
          vid,
          title,
          mediaRequested,
          mediaEffective: mediaPlan.effective,
          mediaFallbackReason: mediaPlan.fallbackReason || null,
          downloadEngine,
          downloadMode,
        };
      }

      // existing durl (single url / byte-range) flow remains unchanged
      if (play.data?.durl?.length) {
        await markDownloadMode(
          'native',
          'durl-single',
          'DURL stream detected; set baseline mode before byte-range upgrade checks',
        );
        if (resolvedQn > 0 && resolvedQn < qualityPolicy.minAcceptableQn) {
          throw new Error(
            `durl quality qn=${resolvedQn} is below accepted minimum ` +
              `${qualityPolicy.minAcceptableQn} for prefer=${qualityPolicy.preferredLabel}`,
          );
        }
        const url = play.data.durl[0].url;
        const tempOutFile = path.join(this['dataDir'], `${vid}-${job.id}.mp4`);
        this.adoptResumeArtifacts(job, payload, vid, undefined, undefined, tempOutFile);
        if (cancelRequested) { await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 }); this.logger.log(`Job ${job.id} cancelled before download start`); return { cancelled: true }; }
        await job.progress(30);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading', progress: 30 });

        // Try to get content-length and Accept-Ranges to decide whether to use parallel parts.
        let useParallel = false; let totalSize = 0;
        try {
          const head = await axios.head(url, { headers: commonHeaders, timeout: 5000 });
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
                  headers: { ...commonHeaders, Range: 'bytes=0-1' },
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
              this.logger.debug(
                `Job ${job.id}: single(durl) progress ${formatMbProgress(downloaded, totalSize)} (${pct}%)`,
              );
              lastSingleProgressLogAt = now;
            } else if (!totalSize && now - lastSingleProgressLogAt >= 2000) {
              this.logger.debug(
                `Job ${job.id}: single(durl) progress downloaded=${formatMb(downloaded)} MB (total unknown)`,
              );
              lastSingleProgressLogAt = now;
            }
            if (totalSize > 0) {
              void this.reportByteProgress(job, downloaded, totalSize, 30, 95);
            }
          };
          if (totalSize > 0) await this.reportByteProgress(job, downloaded, totalSize, 30, 95);
          await this.resumeDownloadWithRetry(
            `${job.id} durl-single`,
            url,
            tempOutFile,
            commonHeaders,
            abortRequested,
            undefined,
            onProgress,
            downloadOptions,
            retryCount,
            retryBackoffMs,
          );
          } finally {
            await this.releaseSingleDownloadPermit(singlePermit);
          }
        } else {
          await markDownloadMode(
            'native',
            'durl-byte-range',
            'strategy upgraded from durl-single baseline: DURL accepts byte-range (valid 206 probe)',
          );
          const partSize = partSizeBytes; const partCount = Math.ceil(totalSize / partSize); const partsDir = path.join(manifestDir, 'parts'); if (!fs.existsSync(manifestDir)) fs.mkdirSync(manifestDir, { recursive: true }); if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });
          const parts: any[] = [];
          for (let i = 0; i < partCount; i++) { const start = i * partSize; const end = Math.min(totalSize - 1, (i + 1) * partSize - 1); const expectedBytes = end - start + 1; parts.push({ partIndex: i, rangeStart: start, rangeEnd: end, expectedBytes, state: 'pending', downloadedBytes: 0 }); }
          const manifest = {
            strategy: 'durl-byte-range',
            jobId: String(job.id),
            platform,
            vid,
            title,
            safeTitle,
            mediaRequested,
            mediaEffective: mediaPlan.effective,
            mediaFallbackReason: mediaPlan.fallbackReason || null,
            url,
            totalExpectedBytes: totalSize,
            parts,
          };
          fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
          this.logger.log(`Job ${job.id}: created byte-range manifest with ${parts.length} parts (total ${totalSize} bytes)`);
          if (redisClient) { try { const key = `job:parts:${job.id}`; await redisClient.hset(key, 'totalExpectedBytes', String(totalSize)); for (const p of parts) { await redisClient.hset(key, `part:${p.partIndex}:expectedBytes`, String(p.expectedBytes)); await redisClient.hset(key, `part:${p.partIndex}:state`, 'pending'); } } catch (e) {} }
          await this.history.appendEvent(job.id.toString(), { state: 'segmenting', progress: 35, manifestPath });
          const headers = { ...commonHeaders };
          const partJobs = [];
          for (const p of parts) { this.logger.debug(`Enqueue byte-range part job=${job.id} part=${p.partIndex} range=${p.rangeStart}-${p.rangeEnd} expected=${p.expectedBytes}`); const j = await this.partsQueue!.add({ jobId: String(job.id), vid, platform, totalJobCount: parts.length, url, partIndex: p.partIndex, rangeStart: p.rangeStart, rangeEnd: p.rangeEnd, expectedBytes: p.expectedBytes, headers } as any, { attempts: retryCount, backoff: retryBackoffMs }); await this.indexPartJob(redisClient, String(job.id), String(j.id)); partJobs.push(j); }
          const activeStallMs = Math.max(30000, Number(this.runtimeConfig.getForSource(platform, 'download.partActiveStallMs') ?? 180000));
          const pollMs = 5000;
          const lastProgress: Record<string, { value: number; ts: number; state: string; bytes: number }> = {};
          let remaining = partJobs.length;
          const durlProgressMin = 35;
          const durlProgressMax = 79;
          if (totalSize > 0) await this.reportByteProgress(job, 0, totalSize, durlProgressMin, durlProgressMax);
          while (remaining > 0) {
            if (cancelRequested) {
              await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
              this.logger.log(`Job ${job.id} cancelled during durl-byte-range download`);
              return { cancelled: true };
            }
            if (stopRequested) {
              await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
              this.logger.log(`Job ${job.id} stopped during durl-byte-range download`);
              try { if (redisClient) await redisClient.del(stopKey); } catch (e) {}
              // no local stop marker cleanup; Redis signal key is authoritative
              return { stopped: true };
            }
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
                if (state !== 'active') {
                  lastProgress[id] = { value: prog, ts: now, state, bytes: downloadedBytes };
                } else if (now - prev.ts >= activeStallMs) {
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
                this.logAggregateProgress(String(job.id), 'durl-byte-range', totals.totalDownloadedBytes, totals.totalExpectedBytes);
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
        try { if (stopRequested) { await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 }); this.logger.log(`Job ${job.id} stopped during download`); try { if (redisClient) await redisClient.del(stopKey); } catch (e) {} if (_poll) clearInterval(_poll); return { stopped: true }; } } catch (e) {}
        try { if (cancelRequested) { await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 }); this.logger.log(`Job ${job.id} cancelled during download`); return { cancelled: true }; } } catch (e) {}
        const preparedOutput = await this.applyMediaModeOutput(tempOutFile, manifestDir, String(job.id), mediaPlan.effective);
        const ext = mediaPlan.effective === 'audio' ? 'm4a' : 'mp4';
        const finalDir = path.join(this.resultDir, platform, safeTitle); try { if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true }); } catch (e) {}
        const finalPath = fs.existsSync(finalDir) ? path.join(finalDir, `${vid}-${job.id}.${ext}`) : preparedOutput; if (finalPath !== preparedOutput) { try { fs.renameSync(preparedOutput, finalPath); } catch (e) {} }
        await this.history.appendEvent(job.id.toString(), {
          state: 'finished',
          progress: 100,
          result: {
            path: finalPath,
            platform,
            vid,
            title,
            mediaRequested,
            mediaEffective: mediaPlan.effective,
            mediaFallbackReason: mediaPlan.fallbackReason || null,
            downloadEngine,
            downloadMode,
          },
        });
        this.cleanupJobTempArtifacts(String(job.id), finalPath);
        this.logger.log(
          `Job ${job.id} finished: output=${finalPath} engine=${downloadEngine} mode=${downloadMode} ` +
          `elapsed=${formatElapsedDuration(Date.now() - jobStartedAt)}`,
        );
        return {
          path: finalPath,
          platform,
          vid,
          title,
          mediaRequested,
          mediaEffective: mediaPlan.effective,
          mediaFallbackReason: mediaPlan.fallbackReason || null,
          downloadEngine,
          downloadMode,
        };
      }

      throw new Error('No downloadable urls found or DRM-protected');
    } catch (err: any) {
      const recovered = await this.tryFinalizeCompletedByteRangeManifest(job, payload, manifestDir, manifestPath);
      if (recovered) return recovered;
      const recoveredSingle = await this.tryFinalizeSingleModeArtifacts(job, payload, manifestDir, manifestPath);
      if (recoveredSingle) return recoveredSingle;
      if (requestedEngine === 'auto' && nativeAttempted && ytDlpFallbackContext) {
        this.logger.warn(
          `Job ${job.id}: native download failed at mode=${downloadMode} (${String(err?.message || err)}), ` +
            `fallback to yt-dlp`,
        );
        await this.history.appendEvent(String(job.id), {
          state: 'download-mode',
          progress: Number(await job.progress()) || 0,
          downloadEngine: 'yt-dlp',
          downloadMode: 'yt-dlp-fallback',
          fromEngine: downloadEngine,
          fromMode: downloadMode,
          reason: 'native attempt failed under engine=auto',
        });
        const ytDlpFallback = await this.tryDirectDownloadViaYtDlp({
          job,
          requestUrl: ytDlpFallbackContext.requestUrl,
          platform: ytDlpFallbackContext.platform,
          vid: ytDlpFallbackContext.vid,
          title: ytDlpFallbackContext.title,
          safeTitle: ytDlpFallbackContext.safeTitle,
          mediaEffective: ytDlpFallbackContext.mediaEffective,
          cookies: ytDlpFallbackContext.cookies,
          manifestDir,
          stopRequested: () => false,
          cancelRequested: () => false,
        });
        if (ytDlpFallback?.path) {
          await job.progress(100);
          await this.history.appendEvent(String(job.id), {
            state: 'finished',
            progress: 100,
            result: {
              path: ytDlpFallback.path,
              platform: ytDlpFallbackContext.platform,
              vid: ytDlpFallbackContext.vid,
              title: ytDlpFallbackContext.title,
              mediaRequested: ytDlpFallbackContext.mediaRequested,
              mediaEffective: ytDlpFallbackContext.mediaEffective,
              mediaFallbackReason: ytDlpFallbackContext.mediaFallbackReason,
              downloadEngine: 'yt-dlp',
              downloadMode: 'yt-dlp-fallback',
            },
          });
          this.cleanupJobTempArtifacts(String(job.id), ytDlpFallback.path);
          this.logger.log(
            `Job ${job.id} finished: output=${ytDlpFallback.path} engine=yt-dlp mode=yt-dlp-fallback ` +
            `fallback=native-failed elapsed=${formatElapsedDuration(Date.now() - jobStartedAt)}`,
          );
          return {
            path: ytDlpFallback.path,
            platform: ytDlpFallbackContext.platform,
            vid: ytDlpFallbackContext.vid,
            title: ytDlpFallbackContext.title,
            mediaRequested: ytDlpFallbackContext.mediaRequested,
            mediaEffective: ytDlpFallbackContext.mediaEffective,
            mediaFallbackReason: ytDlpFallbackContext.mediaFallbackReason,
            downloadEngine: 'yt-dlp',
            downloadMode: 'yt-dlp-fallback',
          };
        }
      }
      this.logger.error(`Job ${job.id} failed: ${err?.message || err}`);
      try { await job.progress(0); } catch (e) { }
      try { await this.history.appendEvent(job.id.toString(), { state: 'failed', progress: 0, message: err?.message }); } catch (e) { }
      throw err;
    }
    finally {
      try {
        await this.archive.archiveMasterJob(String(job.id), { reason: 'worker-finally' });
      } catch (e) {
        // archival failures are non-fatal for execution path
      }
      this.releaseMasterSlot();
    }
  }

  private toLegacyPlayShape(source: ResolvedSource): any {
    const dashPair = source.streams.dashPair;
    const videoOnly = source.streams.videoOnly;
    const audioOnly = source.streams.audioOnly;
    const muxed = source.streams.muxedBoth || source.streams.audioOnly || source.streams.videoOnly;
    const hasDash = Boolean(dashPair || (videoOnly && audioOnly));
    return {
      data: {
        quality: Number(source.qualityMeta?.qn || 0) || 0,
        dash: hasDash
          ? {
              // Do not default to any fixed quality here; keep unknown quality as 0 so policy checks stay correct.
              video: [{ id: Number(source.qualityMeta?.qn || 0) || 0, baseUrl: dashPair?.videoUrl || videoOnly?.url }],
              audio: [{ id: 30280, baseUrl: dashPair?.audioUrl || audioOnly?.url }],
            }
          : undefined,
        durl: muxed?.url ? [{ url: muxed.url }] : undefined,
      },
    };
  }

  private async applyMediaModeOutput(
    inputPath: string,
    manifestDir: string,
    jobId: string,
    mode: MediaMode,
  ): Promise<string> {
    if (mode === 'both') return inputPath;
    if (!fs.existsSync(inputPath)) return inputPath;
    if (mode === 'video') {
      const out = path.join(manifestDir, `${jobId}-video-only.mp4`);
      try { if (fs.existsSync(out)) fs.unlinkSync(out); } catch (e) {}
      await this.ffmpeg.stripAudio(inputPath, out);
      try { fs.unlinkSync(inputPath); } catch (e) {}
      return out;
    }
    const out = path.join(manifestDir, `${jobId}-audio-only.m4a`);
    try { if (fs.existsSync(out)) fs.unlinkSync(out); } catch (e) {}
    await this.ffmpeg.extractAudioToM4a(inputPath, out);
    try { fs.unlinkSync(inputPath); } catch (e) {}
    return out;
  }

  private async tryFinalizeCompletedByteRangeManifest(
    job: Job,
    payload: any,
    manifestDir: string,
    manifestPath: string,
  ): Promise<any | null> {
    try {
      if (!fs.existsSync(manifestPath)) return null;
      const raw = await fs.promises.readFile(manifestPath, 'utf8');
      const manifest = JSON.parse(raw || '{}');
      const partsDir = path.join(manifestDir, 'parts');
      if (!fs.existsSync(partsDir)) return null;
      const strategy = String(manifest?.strategy || '');
      const vid = String(manifest?.vid || payload?.vid || `job-${job.id}`);
      const platform = String(manifest?.platform || payload?.platform || 'generic');
      const title = String(manifest?.title || payload?.title || vid);
      const safeTitle = String(manifest?.safeTitle || this.normalizeTitleForFolder(title));
      const mediaRequested = String(manifest?.mediaRequested || payload?.mediaRequested || 'both');
      const mediaEffective = String(manifest?.mediaEffective || mediaRequested);
      const mediaFallbackReason = manifest?.mediaFallbackReason || null;

      await this.history.appendEvent(String(job.id), { state: 'recovering-merge', progress: 90, from: 'completed-manifest' });
      let recoveryInputPath: string | null = null;

      if (strategy === 'durl-byte-range') {
        const parts: any[] = Array.isArray(manifest?.parts) ? manifest.parts : [];
        if (!parts.length) return null;
        for (let i = 0; i < parts.length; i++) {
          if (!fs.existsSync(path.join(partsDir, `part-${i}.bin`))) return null;
        }
        const mergedTmp = path.join(manifestDir, `${job.id}-recovered-merged.mp4`);
        try { if (fs.existsSync(mergedTmp)) fs.unlinkSync(mergedTmp); } catch (e) {}
        await this.concatByteRangeParts(partsDir, 'part-', parts.length, mergedTmp);
        recoveryInputPath = mergedTmp;
      } else if (strategy === 'dash-byte-range') {
        const videoParts: any[] = Array.isArray(manifest?.video?.parts) ? manifest.video.parts : [];
        const audioParts: any[] = Array.isArray(manifest?.audio?.parts) ? manifest.audio.parts : [];
        if (!videoParts.length) return null;
        for (const p of videoParts) {
          if (!fs.existsSync(path.join(partsDir, `part-${Number(p?.partIndex || 0)}.bin`))) return null;
        }
        const videoConcatTmp = path.join(manifestDir, `${job.id}-recovered-video-concat.mp4`);
        try { if (fs.existsSync(videoConcatTmp)) fs.unlinkSync(videoConcatTmp); } catch (e) {}
        await this.concatByteRangeParts(partsDir, 'part-', videoParts.length, videoConcatTmp);

        let audioConcatTmp: string | null = null;
        if (audioParts.length > 0) {
          for (const p of audioParts) {
            if (!fs.existsSync(path.join(partsDir, `audio-part-${Number(p?.partIndex || 0)}.bin`))) return null;
          }
          audioConcatTmp = path.join(manifestDir, `${job.id}-recovered-audio-concat.mp4`);
          try { if (fs.existsSync(audioConcatTmp)) fs.unlinkSync(audioConcatTmp); } catch (e) {}
          await this.concatByteRangeParts(partsDir, 'audio-part-', audioParts.length, audioConcatTmp);
        }

        if (mediaEffective === 'video') {
          recoveryInputPath = videoConcatTmp;
        } else if (mediaEffective === 'audio') {
          if (!audioConcatTmp) return null;
          recoveryInputPath = audioConcatTmp;
        } else {
          if (!audioConcatTmp) return null;
          const mergedTmp = path.join(manifestDir, `${job.id}-recovered-merged.mp4`);
          try { if (fs.existsSync(mergedTmp)) fs.unlinkSync(mergedTmp); } catch (e) {}
          await this.ffmpeg.merge(videoConcatTmp, audioConcatTmp, mergedTmp);
          recoveryInputPath = mergedTmp;
        }
      } else if (strategy === 'dash-segmented') {
        const videoParts: any[] = Array.isArray(manifest?.parts) ? manifest.parts : [];
        if (!videoParts.length) return null;
        for (let i = 0; i < videoParts.length; i++) {
          if (!fs.existsSync(path.join(partsDir, `part-${i}.bin`))) return null;
        }
        const videoListPath = path.join(manifestDir, `${job.id}-recover-video-parts.txt`);
        const videoLines: string[] = [];
        for (let i = 0; i < videoParts.length; i++) {
          const p = path.join(partsDir, `part-${i}.bin`);
          videoLines.push(`file '${p.replace(/'/g, "'\\''")}'`);
        }
        await fs.promises.writeFile(videoListPath, videoLines.join('\n'), 'utf8');
        const videoConcatTmp = path.join(manifestDir, `${job.id}-recovered-video-concat.mp4`);
        try { if (fs.existsSync(videoConcatTmp)) fs.unlinkSync(videoConcatTmp); } catch (e) {}
        await this.ffmpeg.mergeParts(videoListPath, videoConcatTmp);

        let audioCount = 0;
        while (fs.existsSync(path.join(partsDir, `audio-part-${audioCount}.bin`))) audioCount += 1;
        let audioConcatTmp: string | null = null;
        if (audioCount > 0) {
          const audioListPath = path.join(manifestDir, `${job.id}-recover-audio-parts.txt`);
          const audioLines: string[] = [];
          for (let i = 0; i < audioCount; i++) {
            const p = path.join(partsDir, `audio-part-${i}.bin`);
            audioLines.push(`file '${p.replace(/'/g, "'\\''")}'`);
          }
          await fs.promises.writeFile(audioListPath, audioLines.join('\n'), 'utf8');
          audioConcatTmp = path.join(manifestDir, `${job.id}-recovered-audio-concat.mp4`);
          try { if (fs.existsSync(audioConcatTmp)) fs.unlinkSync(audioConcatTmp); } catch (e) {}
          await this.ffmpeg.mergeParts(audioListPath, audioConcatTmp);
        }

        if (mediaEffective === 'video') {
          recoveryInputPath = videoConcatTmp;
        } else if (mediaEffective === 'audio') {
          if (!audioConcatTmp) return null;
          recoveryInputPath = audioConcatTmp;
        } else {
          if (!audioConcatTmp) return null;
          const mergedTmp = path.join(manifestDir, `${job.id}-recovered-merged.mp4`);
          try { if (fs.existsSync(mergedTmp)) fs.unlinkSync(mergedTmp); } catch (e) {}
          await this.ffmpeg.merge(videoConcatTmp, audioConcatTmp, mergedTmp);
          recoveryInputPath = mergedTmp;
        }
      } else {
        return null;
      }

      if (!recoveryInputPath || !fs.existsSync(recoveryInputPath)) return null;
      const preparedOutput = await this.applyMediaModeOutput(
        recoveryInputPath,
        manifestDir,
        String(job.id),
        mediaEffective as MediaMode,
      );
      const ext = mediaEffective === 'audio' ? 'm4a' : 'mp4';
      const finalDir = path.join(this.resultDir, platform, safeTitle);
      try { if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true }); } catch (e) {}
      const finalPath = fs.existsSync(finalDir) ? path.join(finalDir, `${vid}-${job.id}.${ext}`) : preparedOutput;
      if (finalPath !== preparedOutput) { try { fs.renameSync(preparedOutput, finalPath); } catch (e) {} }

      await job.progress(100);
      await this.history.appendEvent(String(job.id), {
        state: 'finished',
        progress: 100,
        result: {
          path: finalPath,
          platform,
          vid,
          title,
          mediaRequested,
          mediaEffective,
          mediaFallbackReason,
          downloadEngine: 'native',
          downloadMode: String(manifest?.strategy || 'native-recovered'),
          recoveredFromManifest: true,
        },
      });
      this.cleanupJobTempArtifacts(String(job.id), finalPath);
      this.logger.warn(
        `Job ${job.id}: recovered and finalized from completed manifest -> ${finalPath} ` +
        `(elapsed=${this.formatJobElapsed(job)})`,
      );
      return {
        path: finalPath,
        platform,
        vid,
        title,
        mediaRequested,
        mediaEffective,
        mediaFallbackReason,
        downloadEngine: 'native',
        downloadMode: String(manifest?.strategy || 'native-recovered'),
        recoveredFromManifest: true,
      };
    } catch {
      return null;
    }
  }

  private async tryFinalizeSingleModeArtifacts(
    job: Job,
    payload: any,
    manifestDir: string,
    manifestPath: string,
  ): Promise<any | null> {
    try {
      let manifest: any = {};
      if (fs.existsSync(manifestPath)) {
        try {
          const raw = await fs.promises.readFile(manifestPath, 'utf8');
          manifest = JSON.parse(raw || '{}');
        } catch {
          manifest = {};
        }
      }

      const strategy = String(manifest?.strategy || '');
      if (strategy && strategy !== 'dash-single') return null;

      const vid = String(manifest?.vid || payload?.vid || `job-${job.id}`);
      const platform = String(manifest?.platform || payload?.platform || 'generic');
      const title = String(manifest?.title || payload?.title || vid);
      const safeTitle = String(manifest?.safeTitle || this.normalizeTitleForFolder(title));
      const mediaRequested = String(manifest?.mediaRequested || payload?.mediaRequested || 'both');
      const mediaEffective = String(manifest?.mediaEffective || mediaRequested);
      const mediaFallbackReason = manifest?.mediaFallbackReason || null;

      const videoTmp = path.join(this['dataDir'], `${job.id}-video`);
      const audioTmp = path.join(this['dataDir'], `${job.id}-audio`);
      const tempOutFile = path.join(this['dataDir'], `${vid}-${job.id}.mp4`);

      let recoveryInputPath: string | null = null;
      const hasVideoTmp = fs.existsSync(videoTmp) && (fs.statSync(videoTmp).size > 0);
      const hasAudioTmp = fs.existsSync(audioTmp) && (fs.statSync(audioTmp).size > 0);
      if (hasVideoTmp && hasAudioTmp) {
        const mergedTmp = path.join(manifestDir, `${job.id}-recovered-single-merged.mp4`);
        try { if (fs.existsSync(mergedTmp)) fs.unlinkSync(mergedTmp); } catch (e) {}
        await this.ffmpeg.merge(videoTmp, audioTmp, mergedTmp);
        recoveryInputPath = mergedTmp;
      } else if (await this.shouldAcceptRecoveredSingleFile(String(job.id), tempOutFile)) {
        recoveryInputPath = tempOutFile;
      } else {
        return null;
      }

      await this.history.appendEvent(String(job.id), { state: 'recovering-merge', progress: 90, from: 'single-artifacts' });
      const preparedOutput = await this.applyMediaModeOutput(
        recoveryInputPath,
        manifestDir,
        String(job.id),
        mediaEffective as MediaMode,
      );
      const ext = mediaEffective === 'audio' ? 'm4a' : 'mp4';
      const finalDir = path.join(this.resultDir, platform, safeTitle);
      try { if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true }); } catch (e) {}
      const finalPath = fs.existsSync(finalDir) ? path.join(finalDir, `${vid}-${job.id}.${ext}`) : preparedOutput;
      if (finalPath !== preparedOutput) { try { fs.renameSync(preparedOutput, finalPath); } catch (e) {} }

      await job.progress(100);
      await this.history.appendEvent(String(job.id), {
        state: 'finished',
        progress: 100,
        result: {
          path: finalPath,
          platform,
          vid,
          title,
          mediaRequested,
          mediaEffective,
          mediaFallbackReason,
          downloadEngine: 'native',
          downloadMode: 'native-single-recovered',
          recoveredFromSingleArtifacts: true,
        },
      });
      this.cleanupJobTempArtifacts(String(job.id), finalPath);
      this.logger.warn(
        `Job ${job.id}: recovered and finalized from single artifacts -> ${finalPath} ` +
        `(elapsed=${this.formatJobElapsed(job)})`,
      );
      return {
        path: finalPath,
        platform,
        vid,
        title,
        mediaRequested,
        mediaEffective,
        mediaFallbackReason,
        downloadEngine: 'native',
        downloadMode: 'native-single-recovered',
        recoveredFromSingleArtifacts: true,
      };
    } catch {
      return null;
    }
  }

  private async shouldAcceptRecoveredSingleFile(jobId: string, tempOutFile: string): Promise<boolean> {
    try {
      if (!fs.existsSync(tempOutFile)) return false;
      const size = fs.statSync(tempOutFile).size;
      if (size <= 0) return false;
      const expected = await this.readJobTotalExpectedBytes(jobId);
      if (expected <= 0) return true;
      return size >= Math.floor(expected * 0.98);
    } catch {
      return false;
    }
  }

  private async readJobTotalExpectedBytes(jobId: string): Promise<number> {
    try {
      const redisClient: any = (this.downloadQueue && (this.downloadQueue as any).client) ? (this.downloadQueue as any).client : null;
      if (!redisClient || typeof redisClient.hget !== 'function') return 0;
      const raw = await redisClient.hget(`job:parts:${jobId}`, 'totalExpectedBytes');
      return parseInt(String(raw || '0'), 10) || 0;
    } catch {
      return 0;
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

  private async indexPartJob(redisClient: any, jobId: string, partJobId: string | number): Promise<void> {
    if (!redisClient || typeof redisClient.sadd !== 'function') return;
    try {
      const key = `job:parts:index:${jobId}`;
      await redisClient.sadd(key, String(partJobId));
      if (typeof redisClient.expire === 'function') {
        await redisClient.expire(key, 7 * 24 * 60 * 60);
      }
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

  private logAggregateProgress(jobId: string, strategy: string, downloaded: number, total: number): void {
    if (!(total > 0)) return;
    const key = `${jobId}:${strategy}`;
    const now = Date.now();
    const prev = this.aggregateProgressLogState.get(key);
    if (prev && now - prev.ts < 2000 && downloaded <= prev.downloaded) return;
    const pct = ((Math.max(0, Math.min(downloaded, total)) / total) * 100).toFixed(2);
    this.logger.debug(`Job ${jobId}: ${strategy} aggregate progress ${formatMbProgress(downloaded, total)} (${pct}%)`);
    this.aggregateProgressLogState.set(key, { ts: now, downloaded });
  }

  private selectDashTracks(dash: any, preferredQn: number, minAcceptableQn: number): { videoUrl: string; audioUrl: string; videoQn: number } {
    const videos: any[] = Array.isArray(dash?.video) ? dash.video : [];
    const audios: any[] = Array.isArray(dash?.audio) ? dash.audio : [];
    if (!videos.length || !audios.length) throw new Error('dash tracks missing');

    const sortedVideos = [...videos].sort((a, b) => Number(b?.id || 0) - Number(a?.id || 0));
    const selectedVideo = sortedVideos.find((v) => {
      const qn = Number(v?.id || 0);
      return qn <= preferredQn && qn >= minAcceptableQn;
    });
    if (!selectedVideo) {
      const offered = sortedVideos.map((v) => Number(v?.id || 0)).filter((n) => n > 0).join(',');
      throw new Error(
        `no DASH video track satisfies preferred policy ` +
          `(required qn ${minAcceptableQn}-${preferredQn}, offered=[${offered}])`,
      );
    }

    const sortedAudios = [...audios].sort((a, b) => Number(b?.bandwidth || 0) - Number(a?.bandwidth || 0));
    const selectedAudio = sortedAudios[0];
    const videoUrl = String(selectedVideo?.baseUrl || selectedVideo?.base_url || '').trim();
    const audioUrl = String(selectedAudio?.baseUrl || selectedAudio?.base_url || '').trim();
    if (!videoUrl || !audioUrl) throw new Error('selected DASH track urls missing');

    this.logger.debug(`Selected DASH tracks: videoQn=${Number(selectedVideo?.id || 0)} audioBandwidth=${Number(selectedAudio?.bandwidth || 0)}`);
    return { videoUrl, audioUrl, videoQn: Number(selectedVideo?.id || 0) };
  }

  private async probeByteRangeParallel(
    url: string,
    headers: Record<string, string>,
    partSizeBytes: number,
  ): Promise<{ useParallel: boolean; totalSize: number }> {
    let totalSize = 0;
    try {
      const head = await axios.head(url, { headers, timeout: 5000 });
      const acceptRanges = String(head.headers['accept-ranges'] || '').toLowerCase();
      const contentLength = parseInt(head.headers['content-length'] || '0', 10) || 0;
      totalSize = contentLength > 0 ? contentLength : 0;
      if (!acceptRanges.includes('bytes') || contentLength <= 0 || contentLength < 2 * partSizeBytes) {
        return { useParallel: false, totalSize };
      }

      const probe = await axios.get(url, {
        headers: { ...headers, Range: 'bytes=0-1' },
        timeout: 5000,
        responseType: 'stream',
        validateStatus: (status: number) => status === 200 || status === 206,
      });
      const useParallel = probe.status === 206;
      try { if (probe.data && typeof probe.data.destroy === 'function') probe.data.destroy(); } catch (e) {}
      return { useParallel, totalSize };
    } catch (e) {
      return { useParallel: false, totalSize };
    }
  }

  private buildByteRangeParts(totalSize: number, partSizeBytes: number): Array<{ partIndex: number; rangeStart: number; rangeEnd: number; expectedBytes: number; state: string; downloadedBytes: number }> {
    const parts: Array<{ partIndex: number; rangeStart: number; rangeEnd: number; expectedBytes: number; state: string; downloadedBytes: number }> = [];
    const partCount = Math.ceil(totalSize / partSizeBytes);
    for (let i = 0; i < partCount; i++) {
      const start = i * partSizeBytes;
      const end = Math.min(totalSize - 1, (i + 1) * partSizeBytes - 1);
      const expectedBytes = end - start + 1;
      parts.push({ partIndex: i, rangeStart: start, rangeEnd: end, expectedBytes, state: 'pending', downloadedBytes: 0 });
    }
    return parts;
  }

  private async concatByteRangeParts(partsDirPath: string, prefix: string, count: number, outPath: string): Promise<void> {
    const outStream = fs.createWriteStream(outPath, { flags: 'w' });
    for (let i = 0; i < count; i++) {
      const partPath = path.join(partsDirPath, `${prefix}${i}.bin`);
      if (!fs.existsSync(partPath)) throw new Error(`Missing part file ${partPath}`);
      await new Promise<void>((resolve, reject) => {
        const rs = fs.createReadStream(partPath);
        rs.on('error', reject);
        rs.on('end', () => resolve());
        rs.pipe(outStream, { end: false });
      });
    }
    await new Promise<void>((resolve, reject) => {
      outStream.on('error', reject);
      outStream.on('finish', () => resolve());
      outStream.end();
    });
  }

  private cleanupJobTempArtifacts(jobId: string, finalPath: string): void {
    try {
      const jobDir = path.join(this['dataDir'], String(jobId));
      if (fs.existsSync(jobDir)) fs.rmSync(jobDir, { recursive: true, force: true });
    } catch (e) {}
    for (const suffix of ['video', 'audio']) {
      try {
        const tmp = path.join(this['dataDir'], `${jobId}-${suffix}`);
        if (tmp !== finalPath && fs.existsSync(tmp)) fs.unlinkSync(tmp);
      } catch (e) {}
    }
    try {
      const stopFile = path.join(this['dataDir'], `${jobId}.stop`);
      if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile);
    } catch (e) {}
    try {
      const cancelFile = path.join(this['dataDir'], `${jobId}.cancel`);
      if (fs.existsSync(cancelFile)) fs.unlinkSync(cancelFile);
    } catch (e) {}
  }

  private adoptResumeArtifacts(
    job: Job,
    payload: any,
    vid: string,
    videoTmp?: string,
    audioTmp?: string,
    tempOutFile?: string,
  ): void {
    const current = String(job.id);
    const sources = this.getResumeSourceJobIds(payload, current);
    if (!sources.length) return;

    const adopt = (src: string | undefined, dst: string | undefined, label: string) => {
      if (!src || !dst) return;
      try {
        if (fs.existsSync(dst)) return;
        if (!fs.existsSync(src)) return;
        fs.renameSync(src, dst);
        this.logger.debug(`Job ${job.id}: adopted ${label} -> ${dst}`);
      } catch (e) {
        this.logger.warn(`Job ${job.id}: failed adopting ${label}: ${e instanceof Error ? e.message : String(e)}`);
      }
    };

    for (const sourceJobId of sources) {
      adopt(path.join(this['dataDir'], `${sourceJobId}-video`), videoTmp, `video partial from job ${sourceJobId}`);
      adopt(path.join(this['dataDir'], `${sourceJobId}-audio`), audioTmp, `audio partial from job ${sourceJobId}`);
      adopt(path.join(this['dataDir'], `${vid}-${sourceJobId}.mp4`), tempOutFile, `media partial from job ${sourceJobId}`);
      const done = (videoTmp && fs.existsSync(videoTmp)) || (audioTmp && fs.existsSync(audioTmp)) || (tempOutFile && fs.existsSync(tempOutFile));
      if (done) break;
    }
  }

  private getCompletedPartBytes(partPath: string, expectedBytes: number): number {
    try {
      if (!fs.existsSync(partPath)) return 0;
      const size = fs.statSync(partPath).size;
      if (!(expectedBytes > 0)) return size > 0 ? size : 0;
      return size === expectedBytes ? size : 0;
    } catch (e) {
      return 0;
    }
  }

  private adoptRangePartsFromPreviousJob(
    job: Job,
    payload: any,
    targetPartsDir: string,
    videoParts: Array<{ partIndex: number; expectedBytes: number }>,
    audioParts: Array<{ partIndex: number; expectedBytes: number }>,
  ): { fromJobId: string | null; videoCount: number; audioCount: number; downloadedBytes: number } {
    const sources = this.getResumeSourceJobIds(payload, String(job.id));
    if (!sources.length) {
      return { fromJobId: null, videoCount: 0, audioCount: 0, downloadedBytes: 0 };
    }

    let videoCount = 0;
    let audioCount = 0;
    let downloadedBytes = 0;
    let lastSource: string | null = null;
    const adoptOne = (srcName: string, dstName: string, expectedBytes: number, isAudio: boolean) => {
      let src = '';
      for (const sourceJobId of sources) {
        const cand = path.join(this['dataDir'], sourceJobId, 'parts', srcName);
        if (fs.existsSync(cand)) {
          src = cand;
          lastSource = sourceJobId;
          break;
        }
      }
      if (!src) return;
      const dst = path.join(targetPartsDir, dstName);
      try {
        if (fs.existsSync(dst)) {
          const done = this.getCompletedPartBytes(dst, expectedBytes);
          if (done > 0) {
            downloadedBytes += done;
            if (isAudio) audioCount += 1; else videoCount += 1;
          }
          return;
        }
        const done = this.getCompletedPartBytes(src, expectedBytes);
        if (done <= 0) return;
        fs.renameSync(src, dst);
        downloadedBytes += done;
        if (isAudio) audioCount += 1; else videoCount += 1;
      } catch (e) {
        // ignore and let missing part be redownloaded
      }
    };

    for (const p of videoParts) adoptOne(`part-${p.partIndex}.bin`, `part-${p.partIndex}.bin`, p.expectedBytes, false);
    for (const p of audioParts) adoptOne(`audio-part-${p.partIndex}.bin`, `audio-part-${p.partIndex}.bin`, p.expectedBytes, true);

    return { fromJobId: lastSource, videoCount, audioCount, downloadedBytes };
  }

  private getResumeSourceJobIds(payload: any, currentJobId: string): string[] {
    const out: string[] = [];
    const add = (v: any) => {
      const s = String(v || '').trim();
      if (!s || s === currentJobId || out.includes(s)) return;
      out.push(s);
    };
    add(payload?.resumeFromJobId);
    if (Array.isArray(payload?.resumeFromJobIds)) {
      for (const v of payload.resumeFromJobIds) add(v);
    }
    return out;
  }

  private isStopOrCancelError(err: any): boolean {
    const msg = String(err?.message || err || '').toLowerCase();
    return msg.includes('stopped') || msg.includes('cancelled') || msg.includes('canceled');
  }

  private isTransientDownloadError(err: any): boolean {
    const msg = String(err?.message || err || '').toLowerCase();
    return (
      msg.includes('aborted')
      || msg.includes('socket hang up')
      || msg.includes('econnreset')
      || msg.includes('etimedout')
      || msg.includes('econnaborted')
      || msg.includes('network')
      || msg.includes('download stalled')
    );
  }

  private async resumeDownloadWithRetry(
    label: string,
    url: string,
    dest: string,
    headers: Record<string, string>,
    cancelCheck: (() => boolean) | undefined,
    stopCheck: (() => boolean) | undefined,
    onProgress: ((delta: number) => void) | undefined,
    downloadOptions: any,
    retryCount: number,
    retryBackoffMs: number,
  ): Promise<ResumeDownloadResult> {
    const maxAttempts = Math.max(1, retryCount + 1);
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      let resumeFrom = 0;
      try {
        if (fs.existsSync(dest)) resumeFrom = fs.statSync(dest).size;
      } catch (e) {
        resumeFrom = 0;
      }
      this.logger.debug(
        `Job ${label}: download start offset=${resumeFrom} bytes (attempt ${attempt}/${maxAttempts})${resumeFrom > 0 ? ' [resume]' : ' [fresh]'}`,
      );
      try {
        return await resumeDownload(
          url,
          dest,
          headers,
          cancelCheck,
          stopCheck,
          onProgress,
          false,
          { ...downloadOptions, logger: this.logger },
        );
      } catch (err: any) {
        if (this.isStopOrCancelError(err)) throw err;
        if (!this.isTransientDownloadError(err)) throw err;
        if (attempt >= maxAttempts) throw err;
        this.logger.warn(
          `Job ${label}: transient download error "${String(err?.message || err)}", retry ${attempt}/${maxAttempts - 1} after ${retryBackoffMs}ms`,
        );
        await new Promise((r) => setTimeout(r, retryBackoffMs));
      }
    }
    throw new Error('unreachable');
  }

  private async tryDirectDownloadViaYtDlp(params: {
    job: Job;
    requestUrl: string;
    platform: ConcretePlatform;
    vid: string;
    title: string;
    safeTitle: string;
    mediaEffective: MediaMode;
    cookies: string;
    manifestDir: string;
    stopRequested: () => boolean;
    cancelRequested: () => boolean;
  }): Promise<{ path?: string; stopped?: boolean; cancelled?: boolean } | null> {
    const {
      job,
      requestUrl,
      platform,
      vid,
      safeTitle,
      mediaEffective,
      cookies,
      manifestDir,
      stopRequested,
      cancelRequested,
    } = params;
    if (!requestUrl) return null;
    const binaries = ['/usr/local/bin/yt-dlp', '/usr/bin/yt-dlp', 'yt-dlp'];
    const outputTemplate = path.join(manifestDir, `${job.id}-yt-dlp.%(ext)s`);
    const qualityPolicy = buildVideoQualityPolicy(
      this.runtimeConfig.getForSource(platform, 'download.preferVideoQuality'),
    );
    const preferredHeight = this.toHeightFromQn(qualityPolicy.preferredQn);
    const minAcceptableHeight = this.toHeightFromQn(qualityPolicy.minAcceptableQn);
    const formatSelector = mediaEffective === 'audio'
      ? 'bestaudio/b'
      : mediaEffective === 'video'
        ? `bv*[height<=${preferredHeight}][height>=${minAcceptableHeight}]/b[height<=${preferredHeight}][height>=${minAcceptableHeight}]`
        : `bv*[height<=${preferredHeight}][height>=${minAcceptableHeight}]+ba/` +
          `b[height<=${preferredHeight}][height>=${minAcceptableHeight}]`;
    const configuredParallel = Number(this.runtimeConfig.getForSource(platform, 'download.parallelMaxConcurrentDownloads') ?? 10);
    const concurrentFragments = Math.max(1, Math.min(32, Math.floor(configuredParallel) || 1));
    const cookieHeader = String(cookies || '').trim();
    const cookieVariants: Array<{ label: string; cookieHeader: string }> =
      platform === 'youtube' && cookieHeader
        ? [
            { label: 'with-cookie', cookieHeader },
            { label: 'without-cookie', cookieHeader: '' },
          ]
        : [{ label: cookieHeader ? 'with-cookie' : 'no-cookie', cookieHeader }];
    let sawNonEnoentFailure = false;
    let lastFailureDetail = '';

    for (const bin of binaries) {
      for (const cookieVariant of cookieVariants) {
        let cookieFilePath: string | null = null;
        const args = [
          '--newline',
          '--progress',
          '--no-warnings',
          '--no-playlist',
          '--concurrent-fragments',
          String(concurrentFragments),
          '-f',
          formatSelector,
          '-o',
          outputTemplate,
        ];
        if (mediaEffective === 'both' || mediaEffective === 'video') {
          args.push('--merge-output-format', 'mp4');
        }
        if (mediaEffective === 'audio') {
          args.push('--extract-audio', '--audio-format', 'm4a');
        }
        if (cookieVariant.cookieHeader) {
          cookieFilePath = createYtDlpCookieFile({
            cookieHeader: cookieVariant.cookieHeader,
            targetUrl: requestUrl,
            outputDir: manifestDir,
            filePrefix: `${job.id}-yt-dlp-cookies`,
          });
          if (cookieFilePath) args.push('--cookies', cookieFilePath);
        }
        args.push(requestUrl);

        let killedBySignal = false;
        let stage = 'starting';
        let lastProgressEmitAt = 0;
        let lastPercent = -1;
        let lastHistoryBucket = -1;
        let lastMappedProgress = 12;
        let downloadPass = 1;
        let lastPassResetAt = 0;
        const recentToolLines: Array<{ stream: 'stdout' | 'stderr'; line: string }> = [];
        const rememberToolLine = (stream: 'stdout' | 'stderr', line: string): void => {
          const clean = String(line || '').trim();
          if (!clean) return;
          recentToolLines.push({ stream, line: clean });
          if (recentToolLines.length > 80) recentToolLines.shift();
        };

        const mapProgress = (currentStage: string, percent: number): number => {
          if (currentStage === 'downloading') return Math.max(15, Math.min(92, Math.floor(15 + (percent * 0.77))));
          if (currentStage === 'merging' || currentStage === 'extracting') return Math.max(92, Math.min(98, Math.floor(92 + (percent * 0.06))));
          return 15;
        };
        const setStage = async (next: string): Promise<void> => {
          if (stage === next) return;
          stage = next;
          this.logger.debug(`Job ${job.id}: yt-dlp stage=${stage}`);
          await this.history.appendEvent(String(job.id), { state: 'yt-dlp-stage', progress: await job.progress(), stage });
        };

        try {
          await this.history.appendEvent(String(job.id), { state: 'yt-dlp-start', progress: 12 });
          this.logger.debug(
            `Job ${job.id}: yt-dlp options format=${formatSelector} concurrentFragments=${concurrentFragments} ` +
              `platform=${platform} bin=${bin} cookieMode=${cookieVariant.label}`,
          );
          const child = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] });
          const monitor = setInterval(() => {
            if (cancelRequested()) {
              killedBySignal = true;
              try { child.kill('SIGTERM'); } catch {}
            } else if (stopRequested()) {
              killedBySignal = true;
              try { child.kill('SIGTERM'); } catch {}
            }
          }, 300);

        const onLine = async (lineRaw: string, stream: 'stdout' | 'stderr'): Promise<void> => {
          const line = String(lineRaw || '').trim();
          if (!line) return;
          rememberToolLine(stream, line);
          if (line.includes('[download]')) await setStage('downloading');
          if (line.includes('[Merger]') || line.toLowerCase().includes('merging formats')) await setStage('merging');
          if (line.includes('[ExtractAudio]')) await setStage('extracting');

          const m = line.match(/(\d+(?:\.\d+)?)%/);
          if (!m) return;
          const pct = Number(m[1]);
          if (!Number.isFinite(pct)) return;
          const now = Date.now();
          const looksLikeNewPass = stage === 'downloading'
            && lastPercent >= 90
            && pct <= 5
            && now - lastPassResetAt > 1000;
          if (looksLikeNewPass) {
            downloadPass += 1;
            lastHistoryBucket = -1;
            lastPassResetAt = now;
            this.logger.debug(`Job ${job.id}: yt-dlp started download pass ${downloadPass} (progress reset ${lastPercent.toFixed(1)}% -> ${pct.toFixed(1)}%)`);
            await this.history.appendEvent(String(job.id), {
              state: 'yt-dlp-download-pass',
              progress: lastMappedProgress,
              pass: downloadPass,
            });
          }
          if (Math.abs(pct - lastPercent) < 0.5 && now - lastProgressEmitAt < 1500) return;
          lastPercent = pct;
          lastProgressEmitAt = now;
          const mapped = Math.max(lastMappedProgress, mapProgress(stage, pct));
          lastMappedProgress = mapped;
          await job.progress(mapped);
          this.logger.debug(`Job ${job.id}: yt-dlp stage=${stage} pass=${downloadPass} progress=${pct.toFixed(1)}%`);
          const historyBucket = Math.floor(pct / 10);
          if (historyBucket > lastHistoryBucket) {
            lastHistoryBucket = historyBucket;
            await this.history.appendEvent(String(job.id), {
              state: 'yt-dlp-progress',
              progress: mapped,
              stage,
              pass: downloadPass,
              downloadedPercent: Number(pct.toFixed(1)),
            });
          }
        };

        const rlOut = createInterface({ input: child.stdout });
        const rlErr = createInterface({ input: child.stderr });
        let lineQueue = Promise.resolve();
        const enqueueLine = (line: string, stream: 'stdout' | 'stderr'): void => {
          lineQueue = lineQueue
            .then(() => onLine(line, stream))
            .catch(() => undefined);
        };
        rlOut.on('line', (line) => { enqueueLine(line, 'stdout'); });
        rlErr.on('line', (line) => { enqueueLine(line, 'stderr'); });

        const exitCode = await new Promise<number>((resolve, reject) => {
          child.on('error', reject);
          child.on('close', (code) => resolve(Number(code ?? 1)));
        });
        clearInterval(monitor);
        rlOut.close();
        rlErr.close();

          if (killedBySignal) {
            if (cancelRequested()) return { cancelled: true };
            if (stopRequested()) return { stopped: true };
          }
          if (exitCode !== 0) {
            throw new Error(`yt-dlp exited with code ${exitCode}`);
          }

          const entries = fs.readdirSync(manifestDir)
            .filter((name) => name.startsWith(`${job.id}-yt-dlp.`))
            .map((name) => {
              const full = path.join(manifestDir, name);
              let mtime = 0;
              try { mtime = fs.statSync(full).mtimeMs; } catch {}
              return { full, name, mtime };
            })
            .sort((a, b) => b.mtime - a.mtime);
          const selected = entries[0]?.full || '';
          if (!selected || !fs.existsSync(selected)) {
            throw new Error('yt-dlp completed but output file was not found');
          }
          const ext = path.extname(selected).replace('.', '').trim() || (mediaEffective === 'audio' ? 'm4a' : 'mp4');
          const finalDir = path.join(this.resultDir, platform, safeTitle);
          if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true });
          const finalPath = path.join(finalDir, `${vid}-${job.id}.${ext}`);
          if (selected !== finalPath) fs.renameSync(selected, finalPath);
          await job.progress(99);
          await this.history.appendEvent(String(job.id), {
            state: 'yt-dlp-finished',
            progress: 99,
            path: finalPath,
            downloadEngine: 'yt-dlp',
            downloadMode: 'yt-dlp-direct',
          });
          return { path: finalPath };
        } catch (e: any) {
          if (String(e?.code || '') === 'ENOENT') continue;
          if (cancelRequested()) return { cancelled: true };
          if (stopRequested()) return { stopped: true };
          const deprecationPattern = /deprecated feature/i;
          const progressNoisePattern = /^\[download\]\s+\d+(?:\.\d+)?%/i;
          const significantPattern = /(error|unable|forbidden|denied|unsupported|not available|private|login|sign in|captcha|http error|403|429|extractor|failed)/i;
          const lines = recentToolLines.map((x) => x.line);
          const significant = lines.filter((line) =>
            !deprecationPattern.test(line)
            && !progressNoisePattern.test(line)
            && significantPattern.test(line),
          );
          const fallbackContext = lines.filter((line) => !progressNoisePattern.test(line));
          const context = (significant.length > 0 ? significant : fallbackContext).slice(-4);
          const contextText = context.length > 0 ? context.join(' | ') : '';
          const detailCore = contextText ? `${String(e?.message || e)} | yt-dlp: ${contextText}` : String(e?.message || e);
          const detail = `${detailCore} (bin=${bin},cookieMode=${cookieVariant.label})`;
          sawNonEnoentFailure = true;
          lastFailureDetail = detail;
          this.logger.warn(`Job ${job.id}: yt-dlp attempt failed (${detail})`);
          if (platform === 'youtube' && cookieVariant.label === 'with-cookie') {
            this.logger.warn(`Job ${job.id}: retry yt-dlp without cookies for youtube after cookie-mode failure`);
          }
        } finally {
          safeUnlink(cookieFilePath);
        }
      }
    }
    if (sawNonEnoentFailure) {
      this.logger.warn(`Job ${job.id}: yt-dlp direct download failed (${lastFailureDetail}), fallback to native downloader`);
      await this.history.appendEvent(String(job.id), {
        state: 'yt-dlp-failed',
        progress: 12,
        message: lastFailureDetail,
      });
      return null;
    }
    this.logger.warn(`Job ${params.job.id}: yt-dlp binary not found, fallback to native downloader`);
    await this.history.appendEvent(String(params.job.id), { state: 'yt-dlp-unavailable', progress: 12 });
    return null;
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

  private toHeightFromQn(qn: number): number {
    const normalized = Number(qn || 0) || 0;
    if (normalized >= 120) return 2160;
    if (normalized >= 116) return 1440;
    if (normalized >= 80) return 1080;
    if (normalized >= 64) return 720;
    if (normalized >= 32) return 480;
    if (normalized >= 16) return 360;
    return 0;
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

  private async acquireMasterSlot(job: Job): Promise<void> {
    while (true) {
      const masterConcurrency = this.getCurrentMasterConcurrency();
      if (WorkerProcessor.activeMasterJobs < masterConcurrency) {
        WorkerProcessor.activeMasterJobs += 1;
        this.logger.debug(`Job ${job.id}: acquired master slot (${WorkerProcessor.activeMasterJobs}/${masterConcurrency})`);
        return;
      }
      this.logger.debug(`Job ${job.id}: waiting master slot (${WorkerProcessor.activeMasterJobs}/${masterConcurrency})`);
      await new Promise<void>((resolve) => setTimeout(resolve, 250));
    }
  }

  private releaseMasterSlot(): void {
    WorkerProcessor.activeMasterJobs = Math.max(0, WorkerProcessor.activeMasterJobs - 1);
    const next = WorkerProcessor.masterWaiters.shift();
    if (next) next();
  }

  private async acquireSingleDownloadPermit(job: Job): Promise<string | null> {
    const redisClient: any = (this.downloadQueue && (this.downloadQueue as any).client) ? (this.downloadQueue as any).client : null;
    const token = `single-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}-${job.id}`;
    while (true) {
      const limiter = this.getCurrentSingleLimiterConfig();
      if (limiter.maxConcurrent <= 0) return null;
      if (!redisClient || typeof redisClient.eval !== 'function') {
        throw new Error(`single-download limiter enabled but redis eval is unavailable for job ${job.id}`);
      }
      const now = Date.now();
      let ok = 0;
      try {
        ok = Number(await redisClient.eval(
          WorkerProcessor.ACQUIRE_GLOBAL_PERMIT_LUA,
          1,
          WorkerProcessor.GLOBAL_SINGLE_DOWNLOADS_SEMAPHORE_KEY,
          String(now),
          String(limiter.leaseMs),
          String(limiter.maxConcurrent),
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
      await new Promise((r) => setTimeout(r, limiter.waitMs));
    }
  }

  private startPermitRenew(token: string, redisClient: any): void {
    const base = this.getCurrentSingleLimiterConfig();
    const period = Math.max(1000, Math.floor(base.leaseMs / 3));
    const t = setInterval(async () => {
      try {
        const limiter = this.getCurrentSingleLimiterConfig();
        await redisClient.eval(
          WorkerProcessor.RENEW_GLOBAL_PERMIT_LUA,
          1,
          WorkerProcessor.GLOBAL_SINGLE_DOWNLOADS_SEMAPHORE_KEY,
          String(Date.now()),
          String(limiter.leaseMs),
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

  private getCurrentMasterConcurrency(): number {
    const workerConcurrency = Math.max(1, Number(this.runtimeConfig.getGlobal('worker.concurrency') ?? 4));
    const singleMax = Math.max(1, Number(this.runtimeConfig.getGlobal('download.singleMaxConcurrentDownloads') ?? process.env.SINGLE_MAX_CONCURRENT_DOWNLOADS ?? 5));
    return Math.min(workerConcurrency, singleMax);
  }

  private getCurrentSingleLimiterConfig(): { maxConcurrent: number; leaseMs: number; waitMs: number } {
    return {
      maxConcurrent: Math.max(0, Number(this.runtimeConfig.getGlobal('download.globalSingleMaxConcurrentDownloads') ?? process.env.GLOBAL_SINGLE_MAX_CONCURRENT_DOWNLOADS ?? 5)),
      leaseMs: Math.max(5000, Number(this.runtimeConfig.getGlobal('download.globalLimiterLeaseMs') ?? process.env.GLOBAL_LIMITER_LEASE_MS ?? 120000)),
      waitMs: Math.max(100, Number(this.runtimeConfig.getGlobal('download.globalLimiterWaitMs') ?? process.env.GLOBAL_LIMITER_WAIT_MS ?? 300)),
    };
  }

  private formatJobElapsed(job: Job): string {
    const ts = Number((job as any)?.timestamp || 0);
    if (!Number.isFinite(ts) || ts <= 0) return 'n/a';
    return formatElapsedDuration(Math.max(0, Date.now() - ts));
  }
}
