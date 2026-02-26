import { Body, Controller, Get, NotFoundException, Param, Post } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { Queue } from 'bull';
import { DownloadService } from './download.service';
import { JobHistoryService } from '../jobs/job-history.service';

import * as path from 'path';
import * as fs from 'fs';

@Controller('download')
export class DownloadController {
  constructor(
    private readonly downloadService: DownloadService,
    @InjectQueue('downloads') private readonly downloadQueue: Queue,
    private readonly history: JobHistoryService,
  ) {}

  @Post()
  async startDownload(@Body() body: any) {
    const job = await this.downloadService.enqueue(body);
    return { jobId: job.id };
  }

  @Get('status/:id')
  async getStatus(@Param('id') id: string) {
    const job = await this.downloadQueue.getJob(id as any);
    if (!job) return { id, state: 'not-found' };
    const state = await job.getState();
    const progress = await job.progress();
    const failedReason = job.failedReason || null;
    const result = job.returnvalue || null;
    const history = await this.history.getHistory(id);
    return { id: job.id, state, progress, failedReason, result, history };
  }

  @Post(':id/resume')
  async resume(@Param('id') id: string) {
    const job = await this.downloadQueue.getJob(id as any);
    if (!job) throw new NotFoundException('job not found');
    const data = job.data || {};
    const bvid = data.bvid;
    const cookies = data.cookies;
    if (!bvid) throw new NotFoundException('bvid missing from job data');

    const DATA_DIR = path.resolve(process.cwd(), 'data');
    const stopKey = `job:stop:${id}`;

    // signal worker to stop the currently active job by writing a Redis key
    try {
      const client: any = (this.downloadQueue as any).client;
      if (client && typeof client.set === 'function') {
        // set a short TTL to avoid stale flags
        await client.set(stopKey, String(Date.now()), 'EX', 60);
      }
    } catch (e) {
      // ignore redis errors and fall back to filesystem marker
      try { if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) { }
      try { fs.writeFileSync(path.join(DATA_DIR, `${id}.stop`), String(Date.now())); } catch (e) { }
    }
    await this.history.appendEvent(id, { state: 'stop-requested', progress: 0 });

    // wait for worker to acknowledge stop (worker will append 'stopped')
    const timeout = 15000; // ms
    const interval = 500; // ms
    const start = Date.now();
    let stopped = false;
    while (Date.now() - start < timeout) {
      const hist = await this.history.getHistory(id);
      if (hist.find(h => h.state === 'stopped')) { stopped = true; break; }
      await new Promise(r => setTimeout(r, interval));
    }

    if (!stopped) {
      // best-effort: proceed anyway after timeout
      await this.history.appendEvent(id, { state: 'stop-timeout', progress: 0 });
    }

    try {
      const client: any = (this.downloadQueue as any).client;
      if (client && typeof client.del === 'function') await client.del(stopKey);
    } catch (e) {
      try { const f = path.join(DATA_DIR, `${id}.stop`); if (fs.existsSync(f)) fs.unlinkSync(f); } catch (e) { }
    }

    // requeue the job for the worker to perform the resume; try to reuse same job id
    let newJob: any = null;
    try {
      await this.history.appendEvent(id, { state: 'requeueing', progress: 0 });
      try {
        newJob = await this.downloadQueue.add(data, { jobId: id, attempts: 3, backoff: 5000 });
      } catch (e) {
        // if adding with same id fails, add as a fresh job
        newJob = await this.downloadQueue.add(data, { attempts: 3, backoff: 5000 });
      }
      await this.history.appendEvent(id, { state: 'resume-requested', progress: 1 });
    } catch (e) {
      await this.history.appendEvent(id, { state: 'requeue-failed', progress: 0, message: String(e?.message || e) });
      throw e;
    }

    return { status: 'resume-enqueued', newJobId: newJob?.id || null };
  }

  @Post(':id/cancel')
  async cancel(@Param('id') id: string) {
    const job = await this.downloadQueue.getJob(id as any);
    const DATA_DIR = path.resolve(process.cwd(), 'data');
    const cancelFile = path.join(DATA_DIR, `${id}.cancel`);
    try {
      // create cancel marker for worker to cooperatively stop
      try { if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) { }
      fs.writeFileSync(cancelFile, String(Date.now()));
    } catch (e) {
      // ignore
    }

    if (job) {
      try {
        await job.remove();
      } catch (e) {
        // could be active — worker will check cancel marker
      }
    }

    try { await this.history.appendEvent(id, { state: 'cancelled', progress: 0 }); } catch (e) { }
    return { status: 'cancelled', id };
  }
}
