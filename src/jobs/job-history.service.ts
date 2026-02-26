import { Injectable, Logger } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';

const JOBS_DIR = path.resolve(process.cwd(), 'data', 'jobs');

@Injectable()
export class JobHistoryService {
  private readonly logger = new Logger(JobHistoryService.name);

  constructor() {
    if (!fs.existsSync(JOBS_DIR)) fs.mkdirSync(JOBS_DIR, { recursive: true });
  }

  filePath(jobId: string) {
    return path.join(JOBS_DIR, `${jobId}.json`);
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
