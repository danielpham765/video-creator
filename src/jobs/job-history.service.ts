
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class JobHistoryService {
  private readonly logger = new Logger(JobHistoryService.name);
  private readonly jobsDir: string;

  constructor(private readonly config: ConfigService) {
    const cfgDataDir = String(this.config.get('download.dataDir') || path.join(process.cwd(), 'data'));
    const dataDir = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    this.jobsDir = path.join(dataDir, 'jobs');
    if (!fs.existsSync(this.jobsDir)) fs.mkdirSync(this.jobsDir, { recursive: true });
  }

  filePath(jobId: string) {
    return path.join(this.jobsDir, `${jobId}.json`);
  }

  async appendEvent(jobId: string, event: any) {
    const file = this.filePath(jobId);
    const record = { ...event, ts: event.ts || new Date().toISOString() };
    try {
      let arr: any[] = [];
      if (fs.existsSync(file)) {
        const raw = await fs.promises.readFile(file, 'utf8');
        arr = JSON.parse(raw || '[]');
      }
      arr.push(record);
      await fs.promises.writeFile(file, JSON.stringify(arr, null, 2), 'utf8');
    } catch (err: any) {
      this.logger.error('Failed to append job event: ' + (err?.message || err));
    }
  }

  async getHistory(jobId: string) {
    const file = this.filePath(jobId);
    if (!fs.existsSync(file)) return [];
    try {
      const raw = await fs.promises.readFile(file, 'utf8');
      return JSON.parse(raw || '[]');
    } catch (err: any) {
      this.logger.error('Failed to read job history: ' + (err?.message || err));
      return [];
    }
  }
}
