import { Injectable, Logger } from '@nestjs/common';
import { Processor, Process, InjectQueue } from '@nestjs/bull';
import { Job, Queue } from 'bull';
import * as fs from 'fs';
import * as path from 'path';
import resumeDownload, { ProgressCallback, ResumeDownloadResult } from '../utils/resume-download';
import { formatMb, formatMbProgress } from '../utils/size-format';
import { formatElapsedDuration } from '../utils/duration-format';
import { RuntimeConfigService } from '../config/runtime-config.service';
import { ConcretePlatform } from '../source/source.types';

export interface PartJobData {
  jobId: string;
  vid: string;
  totalJobCount?: number;
  // for byte-range parts
  url?: string;
  partIndex: number;
  rangeStart?: number;
  rangeEnd?: number;
  // for segment-based parts
  segmentUrls?: string[];
  expectedBytes: number;
  headers?: Record<string, string>;
  role?: 'video' | 'audio';
  platform?: ConcretePlatform;
}

const GLOBAL_PARTS_SEMAPHORE_KEY = 'semaphore:downloads:parallel:global';
const ACQUIRE_GLOBAL_PERMIT_LUA = `
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

const RENEW_GLOBAL_PERMIT_LUA = `
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

const RELEASE_GLOBAL_PERMIT_LUA = `
local key = KEYS[1]
local token = ARGV[1]
redis.call('ZREM', key, token)
if redis.call('ZCARD', key) == 0 then
  redis.call('DEL', key)
end
return 1
`;

// Bull processor concurrency must be >1, otherwise each worker process can only
// execute one part job at a time regardless of limiter settings.
const PARTS_PROCESSOR_CONCURRENCY = Math.max(
  1,
  Number(process.env.PARTS_PROCESSOR_CONCURRENCY ?? process.env.PARALLEL_MAX_CONCURRENT_DOWNLOADS ?? 50),
);

@Processor('download-parts')
@Injectable()
export class PartsProcessor {
  private readonly logger = new Logger(PartsProcessor.name);
  private static activePartJobs = 0;
  private static partWaiters: Array<() => void> = [];
  private readonly partOwnerWaitTimeoutMs: number;
  private readonly partOwnerPollMs: number;
  private readonly renewTimers = new Map<string, NodeJS.Timeout>();

  constructor(@InjectQueue('download-parts') private readonly partsQueue: Queue, private readonly runtimeConfig: RuntimeConfigService) {
    const cfgDataDir = String(this.runtimeConfig.getGlobal('download.dataDir') || path.join(process.cwd(), 'data'));
    this['dataDir'] = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    this.partOwnerWaitTimeoutMs = Math.max(5000, Number(this.runtimeConfig.getGlobal('download.partOwnerWaitTimeoutMs') ?? process.env.PART_OWNER_WAIT_TIMEOUT_MS ?? 180000));
    this.partOwnerPollMs = Math.max(200, Number(this.runtimeConfig.getGlobal('download.partOwnerPollMs') ?? process.env.PART_OWNER_POLL_MS ?? 750));
    if (!fs.existsSync(this['dataDir'])) fs.mkdirSync(this['dataDir'], { recursive: true });
  }

  @Process({ concurrency: PARTS_PROCESSOR_CONCURRENCY })
  async handlePart(job: Job<PartJobData>) {
    const permit = await this.acquirePartSlot(job);
    const partStartedAt = Date.now();
    try {
      const data = job.data;
      const { jobId, vid, totalJobCount, url, partIndex, rangeStart, rangeEnd, segmentUrls, expectedBytes, headers, role } = data as any;
      const rawPlatform = String((data as any)?.platform || 'generic').toLowerCase();
      const platform: ConcretePlatform = rawPlatform === 'bilibili' || rawPlatform === 'youtube' || rawPlatform === 'generic'
        ? (rawPlatform as ConcretePlatform)
        : 'generic';
      const redisClientForSignal: any =
        (this.partsQueue && (this.partsQueue as any).client) ? (this.partsQueue as any).client : null;
      const resolvedTotalJobCount =
        (typeof totalJobCount === 'number' && totalJobCount > 0)
          ? totalJobCount
          : (await this.resolveTotalJobCount(jobId, redisClientForSignal));
      const displayTotalJobCount = resolvedTotalJobCount > 0 ? resolvedTotalJobCount : (partIndex + 1);
      this.logger.log(`Handling download job ${jobId} part ${partIndex}/${displayTotalJobCount} (role=${role || 'video'}, queueJobId=${job.id})`);
      // this.logger.debug(`part payload=${JSON.stringify(data)}`);

      const cancelKey = `job:cancel:${jobId}`;
      const stopKey = `job:stop:${jobId}`;
      let shouldAbort = false;
      let signalPollTimer: NodeJS.Timeout | null = null;
      if (!shouldAbort && redisClientForSignal && typeof redisClientForSignal.get === 'function') {
        try {
          const [cancelSignal, stopSignal] = await Promise.all([redisClientForSignal.get(cancelKey), redisClientForSignal.get(stopKey)]);
          shouldAbort = Boolean(cancelSignal || stopSignal);
          signalPollTimer = setInterval(async () => {
            if (shouldAbort) return;
            try {
              const [c, s] = await Promise.all([redisClientForSignal.get(cancelKey), redisClientForSignal.get(stopKey)]);
              if (c || s) shouldAbort = true;
            } catch {}
          }, 300);
        } catch (e) {
          // ignore redis signal read errors
        }
      }
      if (shouldAbort) {
        this.logger.log(`Job ${jobId} part ${partIndex}: skipped because master is stop/cancel requested`);
        await job.progress(100).catch(() => undefined);
        return { jobId, vid, partIndex, skipped: true };
      }

      const partsDir = path.join(this['dataDir'], String(jobId), 'parts');
      if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });
      const partName = role === 'audio' ? `audio-part-${partIndex}.bin` : `part-${partIndex}.bin`;
      const partPath = path.join(partsDir, partName);
      const fieldPrefix = role === 'audio' ? `part:audio:${partIndex}` : `part:${partIndex}`;
      const partLockKey = `job:part:lock:${jobId}:${role === 'audio' ? 'audio' : 'video'}:${partIndex}`;
      const partLockToken = `${process.pid}-${job.id}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
      const lockLeaseMs = Math.max(this.partOwnerWaitTimeoutMs * 2, 300000);
      const owner = await this.acquirePartOwnership({
        redisClient: redisClientForSignal,
        lockKey: partLockKey,
        lockToken: partLockToken,
        lockLeaseMs,
        jobId: String(jobId),
        partIndex,
        displayTotalJobCount,
        fieldPrefix,
        expectedBytes,
        partPath,
      });
      if (!owner.acquired) {
        await job.progress(100).catch(() => undefined);
        return { jobId, vid, partIndex, deduped: true };
      }

      try {
        // If a previous attempt already produced a complete part file, treat this as idempotent success.
        // This avoids duplicate retry jobs truncating valid part outputs.
        try {
          if (fs.existsSync(partPath)) {
            const existingSize = fs.statSync(partPath).size;
            if (this.isCompletePartSize(existingSize, expectedBytes)) {
              this.logger.log(`Job ${jobId} part ${partIndex}/${displayTotalJobCount}: reusing existing part (${formatMb(existingSize)} MB) in ${formatElapsedDuration(Date.now() - partStartedAt)}`);
              const redisClient: any = (this.partsQueue && (this.partsQueue as any).client) ? (this.partsQueue as any).client : null;
              if (redisClient) {
                try {
                  const key = `job:parts:${jobId}`;
                  await redisClient.hset(key, `${fieldPrefix}:state`, 'completed');
                  await redisClient.hset(key, `${fieldPrefix}:expectedBytes`, String(expectedBytes));
                  await redisClient.hset(key, `${fieldPrefix}:downloadedBytes`, String(existingSize));
                } catch (e) {}
              }
              await job.progress(100).catch(() => undefined);
              return { jobId, vid, partIndex, path: partPath, bytes: existingSize, expectedBytes };
            }
            fs.unlinkSync(partPath);
          }
        } catch (e) {
          // ignore
        }

        const reqHeaders: Record<string, string> = Object.assign({}, headers || {});

        const redisClient: any = redisClientForSignal;

        let downloaded = 0;
        // mark part as active in Redis if available
        if (redisClient) {
          try {
            const key = `job:parts:${jobId}`;
            redisClient.hset(key, `${fieldPrefix}:state`, 'active').catch(() => undefined);
          } catch (e) {
            // ignore
          }
        }
        const onProgress: ProgressCallback = (delta) => {
          downloaded += delta;
          // Best-effort: update Bull progress percentage for this part.
          const pct = expectedBytes > 0 ? Math.min(100, Math.floor((downloaded / expectedBytes) * 100)) : 0;
          job.progress(pct).catch(() => undefined);
          // Also update Redis manifest for this part if available.
          if (redisClient) {
            try {
              const key = `job:parts:${jobId}`;
              // Store downloaded bytes; callers can also read expectedBytes/state from Redis.
              redisClient
                .hset(key, `${fieldPrefix}:downloadedBytes`, String(downloaded))
                .catch(() => undefined);
            } catch (e) {
              // ignore redis errors
            }
          }
        };

        if (Array.isArray(segmentUrls) && segmentUrls.length > 0) {
          this.logger.log(`Job ${jobId} part ${partIndex}: downloading ${segmentUrls.length} segments`);
          this.logger.debug(`segmentUrls[0]=${segmentUrls[0]} (first of ${segmentUrls.length})`);
          const appendStream = fs.createWriteStream(partPath, { flags: 'a' });
          try {
            for (let si = 0; si < segmentUrls.length; si++) {
              const segUrl = segmentUrls[si];
              const segTmp = path.join(partsDir, `${role === 'audio' ? 'audio-part' : 'part'}-${partIndex}-seg-${si}.tmp`);
              // download each segment into a tmp file
                try {
                this.logger.debug(`Downloading segment ${si} for job ${jobId} part ${partIndex}: ${segUrl}`);
                if (!this.runtimeConfig.getForSource(platform, 'download.resumeEnabled')) {
                  try { if (fs.existsSync(segTmp)) fs.unlinkSync(segTmp); } catch (e) {}
                }
                await resumeDownload(segUrl, segTmp, reqHeaders, () => shouldAbort, undefined, onProgress, false, {
                  timeoutMs: Number(this.runtimeConfig.getForSource(platform, 'download.timeoutMs') ?? 30000),
                  proxy: String(this.runtimeConfig.getForSource(platform, 'proxy.http') || this.runtimeConfig.getForSource(platform, 'proxy.https') || '' ) || undefined,
                  logger: this.logger,
                });
              } catch (e) {
                try { if (fs.existsSync(segTmp)) fs.unlinkSync(segTmp); } catch (er) {}
                throw e;
              }
              // append segment into part file
              await new Promise<void>((resolve, reject) => {
                const rs = fs.createReadStream(segTmp);
                rs.on('error', reject);
                rs.on('end', () => resolve());
                rs.pipe(appendStream, { end: false });
              });
              try { fs.unlinkSync(segTmp); } catch (e) {}
            }
          } finally {
            try { appendStream.end(); } catch (e) {}
          }
          const finalSize = fs.existsSync(partPath) ? fs.statSync(partPath).size : 0;
          if (finalSize <= 0) throw new Error(`Part ${partIndex} of job ${jobId} produced empty file (segments)`);
          // mark completed
          if (redisClient) {
            try {
              const key = `job:parts:${jobId}`;
              const fieldPrefix = role === 'audio' ? `part:audio:${partIndex}` : `part:${partIndex}`;
              await redisClient.hset(key, `${fieldPrefix}:state`, 'completed');
              await redisClient.hset(key, `${fieldPrefix}:expectedBytes`, String(expectedBytes));
            } catch (e) {}
          }
          this.logger.log(`Job ${jobId} part ${partIndex}/${displayTotalJobCount} completed (${formatMbProgress(finalSize, expectedBytes)}) in ${formatElapsedDuration(Date.now() - partStartedAt)}`);
          this.logger.debug(`Part ${partIndex} wrote ${formatMb(finalSize)} MB to ${partPath}`);
          return { jobId, vid, partIndex, path: partPath, bytes: finalSize, expectedBytes };
        }

        // byte-range path
        this.logger.log(`Job ${jobId} part ${partIndex}/${displayTotalJobCount}: downloading bytes ${rangeStart}-${rangeEnd}`);
        // this.logger.debug(`Byte-range download headers: ${JSON.stringify(reqHeaders)}`);
        if (!this.runtimeConfig.getForSource(platform, 'download.resumeEnabled')) {
          try { if (fs.existsSync(partPath)) fs.unlinkSync(partPath); } catch (e) {}
        }
        // Ensure byte-range jobs request only their assigned slice.
        if (typeof rangeStart === 'number' && typeof rangeEnd === 'number') {
          reqHeaders.Range = `bytes=${rangeStart}-${rangeEnd}`;
        }

        const result: ResumeDownloadResult = await resumeDownload(
          url!,
          partPath,
          reqHeaders,
          () => shouldAbort,
          undefined,
          onProgress,
          true,
          {
            timeoutMs: Number(this.runtimeConfig.getForSource(platform, 'download.timeoutMs') ?? 30000),
            proxy: String(this.runtimeConfig.getForSource(platform, 'proxy.http') || this.runtimeConfig.getForSource(platform, 'proxy.https') || '') || undefined,
            logger: this.logger,
          },
        );

        // Sanity: ensure we downloaded at least something.
        const finalSize = fs.existsSync(partPath) ? fs.statSync(partPath).size : 0;
        if (finalSize <= 0) {
          throw new Error(`Part ${partIndex} of job ${jobId} produced empty file`);
        }
        if (typeof expectedBytes === 'number' && expectedBytes > 0 && finalSize !== expectedBytes) {
          throw new Error(
            `Part ${partIndex} of job ${jobId} size mismatch: got ${finalSize}, expected ${expectedBytes}. Range may be ignored by origin.`,
          );
        }

        // Mark part as completed in Redis (state + expectedBytes) for consumers that track per-part status.
        if (redisClient) {
          try {
            const key = `job:parts:${jobId}`;
            await redisClient.hset(key, `${fieldPrefix}:state`, 'completed');
            await redisClient.hset(key, `${fieldPrefix}:expectedBytes`, String(expectedBytes));
          } catch (e) {
            // ignore redis errors
          }
        }

        this.logger.log(`Job ${jobId} part ${partIndex}/${displayTotalJobCount} completed (${formatMbProgress(finalSize, expectedBytes)}) in ${formatElapsedDuration(Date.now() - partStartedAt)}`);
        this.logger.debug(`Part ${partIndex} final size=${finalSize} md5=${result.md5 || 'n/a'}`);

        return {
          jobId,
          vid,
          partIndex,
          path: partPath,
          bytes: finalSize,
          md5: result.md5,
          expectedBytes,
        };
      } finally {
        if (signalPollTimer) clearInterval(signalPollTimer);
        await this.releasePartOwnership(redisClientForSignal, partLockKey, partLockToken);
      }
    } finally {
      await this.releasePartSlot(permit);
    }
  }

  private isCompletePartSize(size: number, expectedBytes: number): boolean {
    if (typeof expectedBytes === 'number' && expectedBytes > 0) return size === expectedBytes;
    return size > 0;
  }

  private async isPartAlreadyCompleted(
    redisClient: any,
    jobId: string,
    fieldPrefix: string,
    expectedBytes: number,
    partPath: string,
  ): Promise<boolean> {
    try {
      if (fs.existsSync(partPath)) {
        const size = fs.statSync(partPath).size;
        if (this.isCompletePartSize(size, expectedBytes)) return true;
      }
    } catch (e) {}
    if (!redisClient || typeof redisClient.hget !== 'function') return false;
    try {
      const key = `job:parts:${jobId}`;
      const state = await redisClient.hget(key, `${fieldPrefix}:state`);
      return state === 'completed';
    } catch (e) {
      return false;
    }
  }

  private async acquirePartOwnership(input: {
    redisClient: any;
    lockKey: string;
    lockToken: string;
    lockLeaseMs: number;
    jobId: string;
    partIndex: number;
    displayTotalJobCount: number;
    fieldPrefix: string;
    expectedBytes: number;
    partPath: string;
  }): Promise<{ acquired: boolean }> {
    const {
      redisClient,
      lockKey,
      lockToken,
      lockLeaseMs,
      jobId,
      partIndex,
      displayTotalJobCount,
      fieldPrefix,
      expectedBytes,
      partPath,
    } = input;

    if (!redisClient || typeof redisClient.set !== 'function') return { acquired: true };
    const started = Date.now();
    while (Date.now() - started < this.partOwnerWaitTimeoutMs) {
      try {
        const ok = await redisClient.set(lockKey, lockToken, 'PX', lockLeaseMs, 'NX');
        if (ok === 'OK') return { acquired: true };
      } catch (e) {
        return { acquired: true };
      }

      if (await this.isPartAlreadyCompleted(redisClient, jobId, fieldPrefix, expectedBytes, partPath)) {
        this.logger.log(`Job ${jobId} part ${partIndex}/${displayTotalJobCount}: duplicate queue job skipped (part already completed by another worker)`);
        return { acquired: false };
      }
      await new Promise((r) => setTimeout(r, this.partOwnerPollMs));
    }
    this.logger.log(`Job ${jobId} part ${partIndex}/${displayTotalJobCount}: duplicate queue job skipped after owner wait timeout`);
    return { acquired: false };
  }

  private async releasePartOwnership(redisClient: any, lockKey: string, lockToken: string): Promise<void> {
    if (!redisClient || typeof redisClient.get !== 'function' || typeof redisClient.del !== 'function') return;
    try {
      const current = await redisClient.get(lockKey);
      if (current === lockToken) {
        await redisClient.del(lockKey);
      }
    } catch (e) {
      // ignore lock release errors
    }
  }

  private async acquirePartSlot(job: Job<PartJobData>): Promise<string | null> {
    while (true) {
      const parallelMaxConcurrentDownloads = this.getCurrentPartConcurrency();
      if (PartsProcessor.activePartJobs < parallelMaxConcurrentDownloads) {
        PartsProcessor.activePartJobs += 1;
        try {
          const permit = await this.acquireGlobalPermit(job);
          // this.logger.debug(
          //   `Part queue job ${job.id}: acquired part slot (${PartsProcessor.activePartJobs}/${parallelMaxConcurrentDownloads})`,
          // );
          return permit;
        } catch (e) {
          PartsProcessor.activePartJobs = Math.max(0, PartsProcessor.activePartJobs - 1);
          const next = PartsProcessor.partWaiters.shift();
          if (next) next();
          throw e;
        }
      }
      // this.logger.debug(`Part queue job ${job.id}: waiting part slot (${PartsProcessor.activePartJobs}/${parallelMaxConcurrentDownloads})`);
      await new Promise<void>((resolve) => setTimeout(resolve, 250));
    }
  }

  private async releasePartSlot(permit: string | null): Promise<void> {
    await this.releaseGlobalPermit(permit);
    PartsProcessor.activePartJobs = Math.max(0, PartsProcessor.activePartJobs - 1);
    const next = PartsProcessor.partWaiters.shift();
    if (next) next();
  }

  private async resolveTotalJobCount(jobId: string, redisClient: any): Promise<number> {
    if (!redisClient || typeof redisClient.hkeys !== 'function') return 0;
    try {
      const key = `job:parts:${jobId}`;
      const fields: string[] = await redisClient.hkeys(key);
      if (!Array.isArray(fields) || fields.length === 0) return 0;

      const re = /^part(?::audio)?:\d+:expectedBytes$/;

      let count = 0;
      for (const field of fields) {
        if (re.test(field)) count += 1;
      }
      return count;
    } catch (e) {
      return 0;
    }
  }

  private async acquireGlobalPermit(job: Job<PartJobData>): Promise<string | null> {
    const redisClient: any = (this.partsQueue && (this.partsQueue as any).client) ? (this.partsQueue as any).client : null;

    const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}-${job.id}`;
    while (true) {
      const limiter = this.getCurrentParallelLimiterConfig();
      if (limiter.maxConcurrent <= 0) return null;
      if (!redisClient || typeof redisClient.eval !== 'function') {
        throw new Error(`global limiter enabled but redis eval is unavailable for part job ${job.id}`);
      }
      const now = Date.now();
      let ok = 0;
      try {
        ok = Number(await redisClient.eval(
          ACQUIRE_GLOBAL_PERMIT_LUA,
          1,
          GLOBAL_PARTS_SEMAPHORE_KEY,
          String(now),
          String(limiter.leaseMs),
          String(limiter.maxConcurrent),
          token,
        )) || 0;
      } catch (e) {
        throw new Error(`global limiter acquire failed for part job ${job.id}: ${String((e as any)?.message || e)}`);
      }

      if (ok === 1) {
        this.startPermitRenew(token, redisClient);
        return token;
      }
      await new Promise((r) => setTimeout(r, limiter.waitMs));
    }
  }

  private startPermitRenew(token: string, redisClient: any): void {
    const base = this.getCurrentParallelLimiterConfig();
    const period = Math.max(1000, Math.floor(base.leaseMs / 3));
    const t = setInterval(async () => {
      try {
        const limiter = this.getCurrentParallelLimiterConfig();
        await redisClient.eval(
          RENEW_GLOBAL_PERMIT_LUA,
          1,
          GLOBAL_PARTS_SEMAPHORE_KEY,
          String(Date.now()),
          String(limiter.leaseMs),
          token,
        );
      } catch (e) {
        // ignore renew errors; lease expiry will self-heal on the next acquire cleanup
      }
    }, period);
    this.renewTimers.set(token, t as any);
  }

  private async releaseGlobalPermit(token: string | null): Promise<void> {
    if (!token) return;
    const timer = this.renewTimers.get(token);
    if (timer) {
      clearInterval(timer);
      this.renewTimers.delete(token);
    }
    const redisClient: any = (this.partsQueue && (this.partsQueue as any).client) ? (this.partsQueue as any).client : null;
    if (!redisClient || typeof redisClient.eval !== 'function') return;
    try {
      await redisClient.eval(
        RELEASE_GLOBAL_PERMIT_LUA,
        1,
        GLOBAL_PARTS_SEMAPHORE_KEY,
        token,
      );
    } catch (e) {
      // ignore release errors
    }
  }

  private getCurrentPartConcurrency(): number {
    return Math.max(1, Number(this.runtimeConfig.getGlobal('download.parallelMaxConcurrentDownloads') ?? process.env.PARALLEL_MAX_CONCURRENT_DOWNLOADS ?? 10));
  }

  private getCurrentParallelLimiterConfig(): { maxConcurrent: number; leaseMs: number; waitMs: number } {
    return {
      maxConcurrent: Math.max(0, Number(this.runtimeConfig.getGlobal('download.globalParallelMaxConcurrentDownloads') ?? process.env.GLOBAL_PARALLEL_MAX_CONCURRENT_DOWNLOADS ?? 10)),
      leaseMs: Math.max(5000, Number(this.runtimeConfig.getGlobal('download.globalLimiterLeaseMs') ?? process.env.GLOBAL_LIMITER_LEASE_MS ?? 120000)),
      waitMs: Math.max(100, Number(this.runtimeConfig.getGlobal('download.globalLimiterWaitMs') ?? process.env.GLOBAL_LIMITER_WAIT_MS ?? 300)),
    };
  }
}
