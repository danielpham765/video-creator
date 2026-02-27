import { Injectable, Logger } from '@nestjs/common';
import { Processor, Process, InjectQueue } from '@nestjs/bull';
import { Job, Queue } from 'bull';
import * as fs from 'fs';
import * as path from 'path';
import { ConfigService } from '@nestjs/config';
import resumeDownload, { ProgressCallback, ResumeDownloadResult } from '../utils/resume-download';

export interface PartJobData {
  jobId: string;
  bvid: string;
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

@Processor('download-parts')
@Injectable()
export class PartsProcessor {
  private readonly logger = new Logger(PartsProcessor.name);
  private static activePartJobs = 0;
  private static partWaiters: Array<() => void> = [];
  private readonly parallelMaxConcurrentDownloads: number;
  private readonly globalParallelMaxConcurrentDownloads: number;
  private readonly globalLimiterLeaseMs: number;
  private readonly globalLimiterWaitMs: number;
  private readonly renewTimers = new Map<string, NodeJS.Timeout>();

  constructor(@InjectQueue('download-parts') private readonly partsQueue: Queue, private readonly config: ConfigService) {
    const cfgDataDir = String(this.config.get('download.dataDir') || path.join(process.cwd(), 'data'));
    this['dataDir'] = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    this.parallelMaxConcurrentDownloads = Math.max(1, Number(this.config.get('download.parallelMaxConcurrentDownloads') ?? process.env.PARALLEL_MAX_CONCURRENT_DOWNLOADS ?? 10));
    this.globalParallelMaxConcurrentDownloads = Math.max(0, Number(this.config.get('download.globalParallelMaxConcurrentDownloads') ?? process.env.GLOBAL_PARALLEL_MAX_CONCURRENT_DOWNLOADS ?? 10));
    this.globalLimiterLeaseMs = Math.max(5000, Number(this.config.get('download.globalLimiterLeaseMs') ?? process.env.GLOBAL_LIMITER_LEASE_MS ?? 120000));
    this.globalLimiterWaitMs = Math.max(100, Number(this.config.get('download.globalLimiterWaitMs') ?? process.env.GLOBAL_LIMITER_WAIT_MS ?? 300));
    if (!fs.existsSync(this['dataDir'])) fs.mkdirSync(this['dataDir'], { recursive: true });
  }

  @Process()
  async handlePart(job: Job<PartJobData>) {
    const permit = await this.acquirePartSlot(job);
    try {
      const data = job.data;
      const { jobId, bvid, url, partIndex, rangeStart, rangeEnd, segmentUrls, expectedBytes, headers, role } = data as any;
      this.logger.log(`Handling download job ${jobId} part ${partIndex} (role=${role || 'video'}, queueJobId=${job.id})`);
      this.logger.debug(`part payload=${JSON.stringify(data)}`);

      const partsDir = path.join(this['dataDir'], String(jobId), 'parts');
      if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });
      const partName = role === 'audio' ? `audio-part-${partIndex}.bin` : `part-${partIndex}.bin`;
      const partPath = path.join(partsDir, partName);

      // If a previous attempt already produced a complete part file, treat this as idempotent success.
      // This avoids duplicate retry jobs truncating valid part outputs.
      try {
        if (fs.existsSync(partPath)) {
          const existingSize = fs.statSync(partPath).size;
          if (typeof expectedBytes === 'number' && expectedBytes > 0 && existingSize === expectedBytes) {
            this.logger.log(`Job ${jobId} part ${partIndex}: reusing existing part (${existingSize} bytes)`);
            const redisClient: any = (this.partsQueue && (this.partsQueue as any).client) ? (this.partsQueue as any).client : null;
            if (redisClient) {
              try {
                const key = `job:parts:${jobId}`;
                const fieldPrefix = role === 'audio' ? `part:audio:${partIndex}` : `part:${partIndex}`;
                await redisClient.hset(key, `${fieldPrefix}:state`, 'completed');
                await redisClient.hset(key, `${fieldPrefix}:expectedBytes`, String(expectedBytes));
                await redisClient.hset(key, `${fieldPrefix}:downloadedBytes`, String(existingSize));
              } catch (e) {}
            }
            await job.progress(100).catch(() => undefined);
            return { jobId, bvid, partIndex, path: partPath, bytes: existingSize, expectedBytes };
          }
          fs.unlinkSync(partPath);
        }
      } catch (e) {
        // ignore
      }

      const reqHeaders: Record<string, string> = Object.assign({}, headers || {});

      const fieldPrefix = role === 'audio' ? `part:audio:${partIndex}` : `part:${partIndex}`;

      const redisClient: any =
        (this.partsQueue && (this.partsQueue as any).client) ? (this.partsQueue as any).client : null;

      let downloaded = 0;
      // mark part as active in Redis if available
      if (redisClient) {
        try {
          const key = `job:parts:${jobId}`;
          const fieldPrefix = role === 'audio' ? `part:audio:${partIndex}` : `part:${partIndex}`;
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
              if (!this.config.get('download.resumeEnabled')) {
                try { if (fs.existsSync(segTmp)) fs.unlinkSync(segTmp); } catch (e) {}
              }
              await resumeDownload(segUrl, segTmp, reqHeaders, path.join(this['dataDir'], `${jobId}.cancel`), undefined, onProgress, false, {
                timeoutMs: Number(this.config.get('download.timeoutMs') ?? 30000),
                proxy: String(this.config.get('proxy.http') || this.config.get('proxy.https') || '' ) || undefined,
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
        this.logger.log(`Job ${jobId} part ${partIndex} completed (segments, ${finalSize}/${expectedBytes} bytes)`);
        this.logger.debug(`Part ${partIndex} wrote ${finalSize} bytes to ${partPath}`);
        return { jobId, bvid, partIndex, path: partPath, bytes: finalSize, expectedBytes };
      }

      // byte-range path
      this.logger.log(`Job ${jobId} part ${partIndex}: downloading bytes ${rangeStart}-${rangeEnd}`);
      this.logger.debug(`Byte-range download headers: ${JSON.stringify(reqHeaders)}`);
      const cancelFile = path.join(this['dataDir'], `${jobId}.cancel`);
      if (!this.config.get('download.resumeEnabled')) {
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
        cancelFile,
        undefined,
        onProgress,
        true,
        {
          timeoutMs: Number(this.config.get('download.timeoutMs') ?? 30000),
          proxy: String(this.config.get('proxy.http') || this.config.get('proxy.https') || '') || undefined,
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
          await redisClient.hset(key, `part:${partIndex}:state`, 'completed');
          await redisClient.hset(key, `part:${partIndex}:expectedBytes`, String(expectedBytes));
        } catch (e) {
          // ignore redis errors
        }
      }

      this.logger.log(`Job ${jobId} part ${partIndex} completed (${finalSize}/${expectedBytes} bytes)`);
      this.logger.debug(`Part ${partIndex} final size=${finalSize} md5=${result.md5 || 'n/a'}`);

      return {
        jobId,
        bvid,
        partIndex,
        path: partPath,
        bytes: finalSize,
        md5: result.md5,
        expectedBytes,
      };
    } finally {
      await this.releasePartSlot(permit);
    }
  }

  private async acquirePartSlot(job: Job<PartJobData>): Promise<string | null> {
    if (PartsProcessor.activePartJobs < this.parallelMaxConcurrentDownloads) {
      PartsProcessor.activePartJobs += 1;
      this.logger.debug(`Part queue job ${job.id}: acquired part slot (${PartsProcessor.activePartJobs}/${this.parallelMaxConcurrentDownloads})`);
      try {
        const permit = await this.acquireGlobalPermit(job);
        return permit;
      } catch (e) {
        PartsProcessor.activePartJobs = Math.max(0, PartsProcessor.activePartJobs - 1);
        const next = PartsProcessor.partWaiters.shift();
        if (next) next();
        throw e;
      }
    }
    this.logger.debug(`Part queue job ${job.id}: waiting part slot (${PartsProcessor.activePartJobs}/${this.parallelMaxConcurrentDownloads})`);
    await new Promise<void>((resolve) => PartsProcessor.partWaiters.push(resolve));
    PartsProcessor.activePartJobs += 1;
    this.logger.debug(`Part queue job ${job.id}: acquired part slot after wait (${PartsProcessor.activePartJobs}/${this.parallelMaxConcurrentDownloads})`);
    try {
      const permit = await this.acquireGlobalPermit(job);
      return permit;
    } catch (e) {
      PartsProcessor.activePartJobs = Math.max(0, PartsProcessor.activePartJobs - 1);
      const next = PartsProcessor.partWaiters.shift();
      if (next) next();
      throw e;
    }
  }

  private async releasePartSlot(permit: string | null): Promise<void> {
    await this.releaseGlobalPermit(permit);
    PartsProcessor.activePartJobs = Math.max(0, PartsProcessor.activePartJobs - 1);
    const next = PartsProcessor.partWaiters.shift();
    if (next) next();
  }

  private async acquireGlobalPermit(job: Job<PartJobData>): Promise<string | null> {
    if (this.globalParallelMaxConcurrentDownloads <= 0) return null;
    const redisClient: any = (this.partsQueue && (this.partsQueue as any).client) ? (this.partsQueue as any).client : null;
    if (!redisClient || typeof redisClient.eval !== 'function') {
      throw new Error(`global limiter enabled but redis eval is unavailable for part job ${job.id}`);
    }

    const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}-${job.id}`;
    while (true) {
      const now = Date.now();
      let ok = 0;
      try {
        ok = Number(await redisClient.eval(
          ACQUIRE_GLOBAL_PERMIT_LUA,
          1,
          GLOBAL_PARTS_SEMAPHORE_KEY,
          String(now),
          String(this.globalLimiterLeaseMs),
          String(this.globalParallelMaxConcurrentDownloads),
          token,
        )) || 0;
      } catch (e) {
        throw new Error(`global limiter acquire failed for part job ${job.id}: ${String((e as any)?.message || e)}`);
      }

      if (ok === 1) {
        this.startPermitRenew(token, redisClient);
        this.logger.debug(`Part queue job ${job.id}: acquired global permit (${token})`);
        return token;
      }
      await new Promise((r) => setTimeout(r, this.globalLimiterWaitMs));
    }
  }

  private startPermitRenew(token: string, redisClient: any): void {
    const period = Math.max(1000, Math.floor(this.globalLimiterLeaseMs / 3));
    const t = setInterval(async () => {
      try {
        await redisClient.eval(
          RENEW_GLOBAL_PERMIT_LUA,
          1,
          GLOBAL_PARTS_SEMAPHORE_KEY,
          String(Date.now()),
          String(this.globalLimiterLeaseMs),
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
}
