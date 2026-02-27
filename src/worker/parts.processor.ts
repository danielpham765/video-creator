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

@Processor('download-parts')
@Injectable()
export class PartsProcessor {
  private readonly logger = new Logger(PartsProcessor.name);

  constructor(@InjectQueue('download-parts') private readonly partsQueue: Queue, private readonly config: ConfigService) {
    const cfgDataDir = String(this.config.get('download.dataDir') || path.join(process.cwd(), 'data'));
    this['dataDir'] = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    if (!fs.existsSync(this['dataDir'])) fs.mkdirSync(this['dataDir'], { recursive: true });
  }

  @Process()
  async handlePart(job: Job<PartJobData>) {
    const data = job.data;
    const { jobId, bvid, url, partIndex, rangeStart, rangeEnd, segmentUrls, expectedBytes, headers, role } = data as any;
    this.logger.log(`Handling part job ${job.id} for download job ${jobId} (part ${partIndex}, role=${role || 'video'})`);
    this.logger.debug(`part payload=${JSON.stringify(data)}`);

    const partsDir = path.join(this['dataDir'], String(jobId), 'parts');
    if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });
    const partName = role === 'audio' ? `audio-part-${partIndex}.bin` : `part-${partIndex}.bin`;
    const partPath = path.join(partsDir, partName);

    // For now, parts are not resumable: start from scratch if file exists.
    try {
      if (fs.existsSync(partPath)) fs.unlinkSync(partPath);
    } catch (e) {
      // ignore
    }

    const reqHeaders: Record<string, string> = Object.assign({}, headers || {});

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
            .hset(key, `part:${partIndex}:downloadedBytes`, String(downloaded))
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
            await resumeDownload(segUrl, segTmp, reqHeaders, path.join(this['dataDir'], `${jobId}.cancel`), undefined, onProgress, false, {
              timeoutMs: Number(this.config.get('download.timeoutMs') ?? 30000),
              proxy: String(this.config.get('proxy.http') || this.config.get('proxy.https') || '' ) || undefined,
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
      },
    );

    // Sanity: ensure we downloaded at least something.
    const finalSize = fs.existsSync(partPath) ? fs.statSync(partPath).size : 0;
    if (finalSize <= 0) {
      throw new Error(`Part ${partIndex} of job ${jobId} produced empty file`);
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
  }
}

