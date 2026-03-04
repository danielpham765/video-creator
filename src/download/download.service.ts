import { Injectable, HttpException, HttpStatus } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { Queue } from 'bull';
import { RuntimeConfigService } from '../config/runtime-config.service';
import { ConcretePlatform } from '../source/source.types';

@Injectable()
export class DownloadService {
  constructor(@InjectQueue('downloads') private readonly downloadQueue: Queue, private readonly runtimeConfig: RuntimeConfigService) {}

  async enqueue(payload: any) {
    const rawPlatform = String(payload?.platform || 'generic').toLowerCase();
    const platform: ConcretePlatform = rawPlatform === 'bilibili' || rawPlatform === 'youtube' || rawPlatform === 'generic'
      ? (rawPlatform as ConcretePlatform)
      : 'generic';
    const retryCount = Number(this.runtimeConfig.getForSource(platform, 'download.retryCount') ?? 3);
    const retryBackoffMs = Number(this.runtimeConfig.getForSource(platform, 'download.retryBackoffMs') ?? 5000);
    try {
      const job = await this.downloadQueue.add(payload, { attempts: retryCount, backoff: retryBackoffMs });
      return job;
    } catch (e) {
      // If Redis or the queue is unavailable, surface a 503 Service Unavailable
      throw new HttpException(
        { message: 'Queue service unavailable: ' + String(e?.message || e) },
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }
  }
}
