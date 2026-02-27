import { Body, Controller, Get, NotFoundException, Param, Post, BadRequestException } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { Queue } from 'bull';
import { DownloadService } from './download.service';
import { CreateDownloadDto } from './dto/create-download.dto';
import { ApiBody } from '@nestjs/swagger';
import { JobHistoryService } from '../jobs/job-history.service';
import { FfmpegService } from '../ffmpeg/ffmpeg.service';
import { ConfigService } from '@nestjs/config';

import * as path from 'path';
import * as fs from 'fs';

@Controller('download')
export class DownloadController {
  constructor(
    private readonly downloadService: DownloadService,
    @InjectQueue('downloads') private readonly downloadQueue: Queue,
    private readonly history: JobHistoryService,
    private readonly ffmpeg: FfmpegService,
    private readonly config: ConfigService,
  ) {}

  @Post()
  @ApiBody({ type: CreateDownloadDto })
  async startDownload(@Body() payload: CreateDownloadDto) {
    // Normalize incoming body: accept either a `bvid` already, or a `url` containing a BV id.
    let bvid: string | undefined = (payload as any).bvid;
    if (!bvid && payload.url) {
      const u = String(payload.url || '');
      // try to find BV id in common Bilibili URL forms
      const m1 = u.match(/BV[0-9A-Za-z]+/);
      const m2 = u.match(/[?&]bvid=(BV[0-9A-Za-z]+)/);
      if (m1) bvid = m1[0];
      else if (m2) bvid = m2[1];
    }

    if (!bvid) {
      // If caller didn't provide a bvid or a url containing one, reject the request
      throw new BadRequestException('bvid missing from request; provide a Bilibili video URL or bvid');
    }

    const jobPayload: any = { bvid, url: payload.url, title: payload.title };
    const job = await this.downloadService.enqueue(jobPayload);
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
    // Try to include parts progress from Redis if available
    const client: any = (this.downloadQueue as any).client;
    let partsSummary: any = null;
    try {
      if (client && typeof client.hgetall === 'function') {
        const key = `job:parts:${id}`;
        const raw = await client.hgetall(key);
        if (raw && Object.keys(raw).length) {
          // parse parts into a structured object
          const parts: any[] = [];
          let totalExpected = 0;
          let totalDownloaded = 0;
          Object.keys(raw).forEach((k) => {
            const m = k.match(/^part(?::audio)?:?(\d+):(expectedBytes|downloadedBytes|state)$/);
            if (m) {
              const idx = parseInt(m[1], 10);
              const field = m[2];
              parts[idx] = parts[idx] || { partIndex: idx };
              if (field === 'expectedBytes') parts[idx].expectedBytes = parseInt(raw[k] || '0', 10) || 0;
              if (field === 'downloadedBytes') parts[idx].downloadedBytes = parseInt(raw[k] || '0', 10) || 0;
              if (field === 'state') parts[idx].state = raw[k];
            }
            if (k === 'totalExpectedBytes') {
              totalExpected = parseInt(raw[k] || '0', 10) || 0;
            }
          });
          for (const p of parts) {
            if (!p) continue;
            totalDownloaded += p.downloadedBytes || 0;
          }
          partsSummary = { parts: parts.filter(Boolean), totalExpectedBytes: totalExpected, totalDownloadedBytes: totalDownloaded };
        }
      }
    } catch (e) {
      // ignore redis errors
    }

    return { id: job.id, state, progress, failedReason, result, history, parts: partsSummary };
  }

  @Post(':id/resume')
  async resume(@Param('id') id: string) {
    const job = await this.downloadQueue.getJob(id as any);
    if (!job) throw new NotFoundException('job not found');
    const data = job.data || {};
    const bvid = data.bvid;
    const cookies = data.cookies;
    if (!bvid) throw new NotFoundException('bvid missing from job data');

    const cfgDataDir = String(this.config.get('download.dataDir') || path.join(process.cwd(), 'data'));
    const DATA_DIR = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
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

    // Requeue as a fresh job id so Bull always creates executable work.
    // Reusing an existing failed job id can return the old job record without re-processing.
    let newJob: any = null;
      try {
        await this.history.appendEvent(id, { state: 'requeueing', progress: 0 });
        const retryCount = Number(this.config.get('download.retryCount') ?? 3);
        const retryBackoffMs = Number(this.config.get('download.retryBackoffMs') ?? 5000);
        newJob = await this.downloadQueue.add(data, { attempts: retryCount, backoff: retryBackoffMs });
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
    const cfgDataDir = String(this.config.get('download.dataDir') || path.join(process.cwd(), 'data'));
    const DATA_DIR = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    const cancelFile = path.join(DATA_DIR, `${id}.cancel`);
    try {
      // create cancel marker for worker to cooperatively stop
      try { if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) { }
      fs.writeFileSync(cancelFile, String(Date.now()));
    } catch (e) {
      // ignore
    }

    // also set Redis cancellation key so remote workers see it
    try {
      const client: any = (this.downloadQueue as any).client;
      const cancelKey = `job:cancel:${id}`;
      if (client && typeof client.set === 'function') {
        await client.set(cancelKey, String(Date.now()), 'EX', 60);
        if (typeof client.publish === 'function') {
          try { await client.publish(cancelKey, '1'); } catch (e) { }
        }
      }
    } catch (e) {
      // ignore redis errors
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

  @Post(':id/merge-partial')
  async mergePartial(@Param('id') id: string) {
    const jobId = String(id);
    const cfgDataDir = String(this.config.get('download.dataDir') || path.join(process.cwd(), 'data'));
    const DATA_DIR = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    const manifestDir = path.join(DATA_DIR, jobId);
    const manifestPath = path.join(manifestDir, 'manifest.json');
    if (!fs.existsSync(manifestPath)) {
      throw new NotFoundException('manifest not found for job');
    }

    let manifest: any;
    try {
      const raw = await fs.promises.readFile(manifestPath, 'utf8');
      manifest = JSON.parse(raw || '{}');
    } catch (e) {
      throw new BadRequestException('invalid manifest for job');
    }

    const bvid: string = manifest?.bvid;
    if (!bvid) throw new BadRequestException('manifest missing bvid');

    const title: string = manifest?.title || bvid;
    const safeTitle = this.sanitizeTitle(title);

    // Currently partial merge is only meaningful for byte-range (durl) strategy.
    if (manifest?.strategy !== 'durl-byte-range') {
      throw new BadRequestException('partial merge is only supported for byte-range downloads');
    }

    const partsDir = path.join(manifestDir, 'parts');
    if (!fs.existsSync(partsDir)) {
      throw new BadRequestException('no parts directory for job');
    }

    const parts: any[] = Array.isArray(manifest.parts) ? manifest.parts : [];
    if (!parts.length) throw new BadRequestException('manifest has no parts');

    // Determine largest contiguous prefix of parts [0..k] that have existing files.
    let prefixCount = 0;
    for (let i = 0; i < parts.length; i++) {
      const partPath = path.join(partsDir, `part-${i}.bin`);
      if (!fs.existsSync(partPath)) break;
      prefixCount++;
    }

    if (prefixCount === 0) {
      await this.history.appendEvent(jobId, { state: 'partial-merge-not-possible', progress: 0 });
      throw new BadRequestException('no completed parts to merge');
    }

    const partialTmp = path.join(DATA_DIR, `${jobId}-partial-temp.mp4`);
    try {
      if (fs.existsSync(partialTmp)) fs.unlinkSync(partialTmp);
    } catch (e) {
      // ignore
    }

    // Concatenate prefix part files into a temporary partial file.
    const outStream = fs.createWriteStream(partialTmp, { flags: 'w' });
    for (let i = 0; i < prefixCount; i++) {
      const partPath = path.join(partsDir, `part-${i}.bin`);
      await new Promise<void>((resolve, reject) => {
        const rs = fs.createReadStream(partPath);
        rs.on('error', reject);
        rs.on('end', () => resolve());
        rs.pipe(outStream, { end: false });
      });
    }
    outStream.end();

    const finalDir = path.join(DATA_DIR, 'bilibili', safeTitle);
    try {
      if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true });
    } catch (e) {
      // ignore
    }
    const finalPath = fs.existsSync(finalDir)
      ? path.join(finalDir, `${bvid}-${jobId}-partial.mp4`)
      : partialTmp;
    if (finalPath !== partialTmp) {
      try {
        fs.renameSync(partialTmp, finalPath);
      } catch (e) {
        // fall back to temp location if rename fails
      }
    }

    await this.history.appendEvent(jobId, {
      state: 'partial-merged',
      progress: 100,
      result: {
        path: finalPath,
        bvid,
        title,
        mergedParts: prefixCount,
        totalParts: parts.length,
      },
    });

    return { status: 'partial-merged', id: jobId, path: finalPath, mergedParts: prefixCount, totalParts: parts.length };
  }

  private sanitizeTitle(raw: string): string {
    const trimmed = (raw || '').trim();
    const noSpecials = trimmed.replace(/[\/\\:*?"<>|]/g, '');
    const collapsed = noSpecials.replace(/\s+/g, ' ');
    return collapsed.substring(0, 80) || 'untitled';
  }
}
