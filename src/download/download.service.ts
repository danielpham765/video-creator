import { Injectable } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { Queue } from 'bull';

@Injectable()
export class DownloadService {
  constructor(@InjectQueue('downloads') private readonly downloadQueue: Queue) {}

  async enqueue(payload: any) {
    const job = await this.downloadQueue.add(payload, { attempts: 3, backoff: 5000 });
    return job;
  }
}
