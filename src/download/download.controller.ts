import { Body, Controller, Get, NotFoundException, Param, Post, BadRequestException } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { Queue } from 'bull';
import { DownloadService } from './download.service';
import { CreateDownloadDto } from './dto/create-download.dto';
import { ApiBody, ApiOperation, ApiParam } from '@nestjs/swagger';
import { downloadSwagger } from './download.swagger';
import { JobHistoryService } from '../jobs/job-history.service';
import { FfmpegService } from '../ffmpeg/ffmpeg.service';
import { RuntimeConfigService } from '../config/runtime-config.service';
import { SourceRegistryService } from '../source/source-registry.service';
import { ConcretePlatform, MediaMode, Platform } from '../source/source.types';

import * as path from 'path';
import * as fs from 'fs';

@Controller('download')
export class DownloadController {
  constructor(
    private readonly downloadService: DownloadService,
    @InjectQueue('downloads') private readonly downloadQueue: Queue,
    @InjectQueue('download-parts') private readonly partsQueue: Queue,
    private readonly history: JobHistoryService,
    private readonly ffmpeg: FfmpegService,
    private readonly runtimeConfig: RuntimeConfigService,
    private readonly sourceRegistry: SourceRegistryService,
  ) {}

  @Post()
  @ApiOperation(downloadSwagger.post.startDownload.operation)
  @ApiBody(downloadSwagger.post.startDownload.body)
  async startDownload(@Body() payload: CreateDownloadDto) {
    const url = String(payload.url || '').trim();
    if (!url) {
      throw new BadRequestException('url is required');
    }
    let page = 1;
    const urlPage = url.match(/[?&]p=(\d+)/);
    if (urlPage && Number.isFinite(Number(urlPage[1])) && Number(urlPage[1]) >= 1) {
      page = Math.floor(Number(urlPage[1]));
    }
    const explicitPage = Number((payload as any).p);
    if (Number.isFinite(explicitPage) && explicitPage >= 1) page = Math.floor(explicitPage);

    const platform = (payload.platform || 'auto') as Platform;
    const mediaRequested = (payload.media || 'both') as MediaMode;
    const identity = this.sourceRegistry.identifyInput(url, platform);
    const jobPayload: any = {
      platform: identity.platform,
      mediaRequested,
      vid: identity.vid,
      url,
      title: payload.title,
      page,
    };
    const job = await this.downloadService.enqueue(jobPayload);
    return { jobId: job.id };
  }

  @Get('status/:id')
  @ApiOperation(downloadSwagger.get.getStatus.operation)
  @ApiParam(downloadSwagger.get.getStatus.idParam)
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
  @ApiOperation(downloadSwagger.post.resume.operation)
  @ApiParam(downloadSwagger.post.resume.idParam)
  async resume(@Param('id') id: string) {
    const job = await this.downloadQueue.getJob(id as any);
    if (!job) throw new NotFoundException('job not found');
    const data = job.data || {};
    const vid = data.vid;
    if (!vid) throw new NotFoundException('vid missing from job data');

    const cfgDataDir = String(this.runtimeConfig.getGlobal('download.dataDir') || path.join(process.cwd(), 'data'));
    const DATA_DIR = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    const stopKey = `job:stop:${id}`;
    const cancelKey = `job:cancel:${id}`;

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
    await this.history.appendEvent(id, { state: 'cancel-requested', progress: 0 });

    // Also emit cancellation for the old job so part-jobs stop quickly.
    try {
      const client: any = (this.downloadQueue as any).client;
      if (client && typeof client.set === 'function') {
        await client.set(cancelKey, String(Date.now()), 'EX', 120);
        if (typeof client.publish === 'function') {
          try { await client.publish(cancelKey, '1'); } catch (e) { }
        }
      }
    } catch (e) {
      // ignore redis errors
    }
    try {
      if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
      fs.writeFileSync(path.join(DATA_DIR, `${id}.cancel`), String(Date.now()));
    } catch (e) {
      // ignore fs errors
    }

    // best-effort: drop queued part jobs of old master so migrated job can proceed cleanly
    await this.removeQueuedPartJobsForMaster(id);

    // wait for worker to acknowledge stop/cancel
    const timeout = 15000; // ms
    const interval = 500; // ms
    const start = Date.now();
    let acknowledged = false;
    while (Date.now() - start < timeout) {
      const hist = await this.history.getHistory(id);
      if (hist.find(h => h.state === 'stopped' || h.state === 'cancelled')) { acknowledged = true; break; }
      try {
        const current = await this.downloadQueue.getJob(id as any);
        if (current) {
          const state = await current.getState();
          if (state !== 'active' && state !== 'waiting' && state !== 'delayed') { acknowledged = true; break; }
        } else {
          acknowledged = true;
          break;
        }
      } catch (e) {
        // ignore transient queue errors during ack polling
      }
      await new Promise(r => setTimeout(r, interval));
    }

    if (!acknowledged) {
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
        const rawPlatform = String((data as any)?.platform || 'generic').toLowerCase();
        const platform: ConcretePlatform = rawPlatform === 'bilibili' || rawPlatform === 'youtube' || rawPlatform === 'generic'
          ? (rawPlatform as ConcretePlatform)
          : 'generic';
        const retryCount = Number(this.runtimeConfig.getForSource(platform, 'download.retryCount') ?? 3);
        const retryBackoffMs = Number(this.runtimeConfig.getForSource(platform, 'download.retryBackoffMs') ?? 5000);
        const prevIds: string[] = [];
        if ((data as any)?.resumeFromJobId) prevIds.push(String((data as any).resumeFromJobId));
        if (Array.isArray((data as any)?.resumeFromJobIds)) {
          for (const v of (data as any).resumeFromJobIds) {
            if (v !== undefined && v !== null && String(v).trim()) prevIds.push(String(v));
          }
        }
        const resumeFromJobIds = Array.from(new Set([String(id), ...prevIds]));
        const resumePayload = { ...data, resumeFromJobId: String(id), resumeFromJobIds };
        newJob = await this.downloadQueue.add(resumePayload, { attempts: retryCount, backoff: retryBackoffMs });
        await this.history.appendEvent(id, { state: 'resume-requested', progress: 1 });
      } catch (e) {
      await this.history.appendEvent(id, { state: 'requeue-failed', progress: 0, message: String(e?.message || e) });
      throw e;
    }

    return { status: 'resume-enqueued', newJobId: newJob?.id || null };
  }

  private async removeQueuedPartJobsForMaster(jobId: string): Promise<void> {
    try {
      const candidates = await this.partsQueue.getJobs(['waiting', 'delayed', 'paused'], 0, 10000);
      for (const partJob of candidates) {
        const partJobId = String((partJob as any)?.data?.jobId || '');
        if (partJobId !== String(jobId)) continue;
        try {
          await partJob.remove();
        } catch (e) {
          // ignore remove races
        }
      }
    } catch (e) {
      // ignore queue errors; worker-side cancel checks still protect correctness
    }
  }

  @Post(':id/cancel')
  @ApiOperation(downloadSwagger.post.cancel.operation)
  @ApiParam(downloadSwagger.post.cancel.idParam)
  async cancel(@Param('id') id: string) {
    const job = await this.downloadQueue.getJob(id as any);
    const cfgDataDir = String(this.runtimeConfig.getGlobal('download.dataDir') || path.join(process.cwd(), 'data'));
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
  @ApiOperation(downloadSwagger.post.mergePartial.operation)
  @ApiParam(downloadSwagger.post.mergePartial.idParam)
  async mergePartial(@Param('id') id: string) {
    const jobId = String(id);
    const cfgDataDir = String(this.runtimeConfig.getGlobal('download.dataDir') || path.join(process.cwd(), 'data'));
    const DATA_DIR = path.isAbsolute(cfgDataDir) ? cfgDataDir : path.resolve(process.cwd(), cfgDataDir);
    const cfgResultDir = String(this.runtimeConfig.getGlobal('download.resultDir') || path.join(process.cwd(), 'result'));
    const RESULT_DIR = path.isAbsolute(cfgResultDir) ? cfgResultDir : path.resolve(process.cwd(), cfgResultDir);
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

    const vid: string = manifest?.vid;
    if (!vid) throw new BadRequestException('manifest missing vid');
    const platform: string = String(manifest?.platform || 'generic');

    const title: string = manifest?.title || vid;
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

    const finalDir = path.join(RESULT_DIR, platform, safeTitle);
    try {
      if (!fs.existsSync(finalDir)) fs.mkdirSync(finalDir, { recursive: true });
    } catch (e) {
      // ignore
    }
    const finalPath = fs.existsSync(finalDir)
      ? path.join(finalDir, `${vid}-${jobId}-partial.mp4`)
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
        vid,
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
