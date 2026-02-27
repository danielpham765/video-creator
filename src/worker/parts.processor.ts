import { Injectable, Logger } from '@nestjs/common';
import { Processor, Process, InjectQueue } from '@nestjs/bull';
import { Job, Queue } from 'bull';
import * as fs from 'fs';
import * as path from 'path';
import { resumeDownload, ProgressCallback, ResumeDownloadResult } from '../utils/resume-download';

const DATA_DIR = path.resolve(process.cwd(), 'data');

export interface PartJobData {
  jobId: string;
  bvid: string;
  url: string;
  partIndex: number;
  rangeStart: number;
  rangeEnd: number;
  expectedBytes: number;
  headers?: Record<string, string>;
}

@Processor('download-parts')
@Injectable()
export class PartsProcessor {
  private readonly logger = new Logger(PartsProcessor.name);

  constructor(@InjectQueue('download-parts') private readonly partsQueue: Queue) {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  @Process()
  async handlePart(job: Job<PartJobData>) {
    const data = job.data;
    const { jobId, bvid, url, partIndex, rangeStart, rangeEnd, expectedBytes, headers } = data;

    const partsDir = path.join(DATA_DIR, String(jobId), 'parts');
    if (!fs.existsSync(partsDir)) fs.mkdirSync(partsDir, { recursive: true });
    const partPath = path.join(partsDir, `part-${partIndex}.bin`);

    // For now, parts are not resumable: start from scratch if file exists.
    try {
      if (fs.existsSync(partPath)) fs.unlinkSync(partPath);
    } catch (e) {
      // ignore
    }

    const reqHeaders: Record<string, string> = Object.assign({}, headers || {});
    reqHeaders.Range = `bytes=${rangeStart}-${rangeEnd}`;

    const redisClient: any =
      (this.partsQueue && (this.partsQueue as any).client) ? (this.partsQueue as any).client : null;

    let downloaded = 0;
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

    this.logger.log(`Job ${jobId} part ${partIndex}: downloading bytes ${rangeStart}-${rangeEnd}`);

    const result: ResumeDownloadResult = await resumeDownload(
      url,
      partPath,
      reqHeaders,
      undefined,
      undefined,
      onProgress,
      true,
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

