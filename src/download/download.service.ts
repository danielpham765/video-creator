import { Injectable } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { Queue } from 'bull';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class DownloadService {
  constructor(@InjectQueue('downloads') private readonly downloadQueue: Queue, private readonly config: ConfigService) {}

  async enqueue(payload: any) {
    const retryCount = Number(this.config.get('download.retryCount') ?? 3);
    const retryBackoffMs = Number(this.config.get('download.retryBackoffMs') ?? 5000);
    const job = await this.downloadQueue.add(payload, { attempts: retryCount, backoff: retryBackoffMs });
    return job;
  }
}
