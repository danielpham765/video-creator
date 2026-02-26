import { Injectable, Logger } from '@nestjs/common';
import { Processor, Process, InjectQueue } from '@nestjs/bull';
import { Job } from 'bull';
import { PlayurlService } from '../playurl/playurl.service';
import { FfmpegService } from '../ffmpeg/ffmpeg.service';
import { JobHistoryService } from '../jobs/job-history.service';
import { Queue } from 'bull';
import * as fs from 'fs';
import * as path from 'path';
import resumeDownload from '../utils/resume-download';

const DATA_DIR = path.resolve(process.cwd(), 'data');

@Processor('downloads')
@Injectable()
export class WorkerProcessor {
  private readonly logger = new Logger(WorkerProcessor.name);

  constructor(
    private readonly playurl: PlayurlService,
    private readonly ffmpeg: FfmpegService,
    private readonly history: JobHistoryService,
    @InjectQueue('downloads') private readonly downloadQueue?: Queue,
  ) {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  @Process()
  async handleJob(job: Job) {
    this.logger.log('Handling job ' + job.id);
    const payload = job.data || {};
    const bvid = payload.bvid;
    if (!bvid) throw new Error('missing bvid');
    const cookies = payload.cookies;
    try {
      await job.progress(5);
      await this.history.appendEvent(job.id.toString(), { state: 'queued', progress: 5 });
      const cid = await this.playurl.getCidFromBvid(bvid);
      if (!cid) throw new Error('cannot get cid');
      await job.progress(10);
      await this.history.appendEvent(job.id.toString(), { state: 'resolving', progress: 10 });
      const play = await this.playurl.getPlayurl(bvid, cid, cookies);
      this.logger.log('Playurl response code: ' + play?.code);
      if (play?.code !== 0) throw new Error(`playurl failed: ${play?.message || 'unknown'}`);

      const cancelFile = path.join(DATA_DIR, `${job.id}.cancel`);
      const stopFile = path.join(DATA_DIR, `${job.id}.stop`);
      const stopKey = `job:stop:${job.id}`;
      let stopRequested = false;
      let _poll: any = null;
      let subClient: any = null;
      const redisClient: any = (this.downloadQueue && (this.downloadQueue as any).client) ? (this.downloadQueue as any).client : null;
      if (redisClient) {
        try {
          // Prefer pub/sub: create a dedicated subscriber if possible
          if (typeof redisClient.duplicate === 'function') {
            try {
              subClient = redisClient.duplicate();
              if (typeof subClient.connect === 'function') await subClient.connect();
              // node-redis v4: subscribe with callback
              if (typeof subClient.subscribe === 'function') {
                await subClient.subscribe(stopKey, (message: any) => {
                  if (message) { stopRequested = true; }
                });
              } else if (typeof subClient.on === 'function') {
                subClient.on('message', (ch: any, message: any) => { if (ch === stopKey && message) stopRequested = true; });
                if (typeof subClient.subscribe === 'function') await subClient.subscribe(stopKey);
              }
            } catch (e) {
              subClient = null;
            }
          }
          // fallback to polling if no pub/sub available
          if (!subClient) {
            if (typeof redisClient.get === 'function') {
              _poll = setInterval(async () => {
                try {
                  const v = await redisClient.get(stopKey);
                  if (v) { stopRequested = true; clearInterval(_poll); }
                } catch (e) {
                  // ignore redis errors
                }
              }, 250);
            }
          }
        } catch (e) {
          // ignore redis setup errors
        }
      }

      if (play.data?.dash) {
        const videoUrl = play.data.dash.video?.[0]?.baseUrl;
        const audioUrl = play.data.dash.audio?.[0]?.baseUrl;
        if (!videoUrl || !audioUrl) throw new Error('dash urls missing');
        const videoTmp = path.join(DATA_DIR, `${job.id}-video`);
        const audioTmp = path.join(DATA_DIR, `${job.id}-audio`);
        const outFile = path.join(DATA_DIR, `${bvid}-${job.id}.mp4`);
        if (fs.existsSync(cancelFile)) {
          await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
          this.logger.log(`Job ${job.id} cancelled before download start`);
          return { cancelled: true };
        }
        if (fs.existsSync(stopFile) || stopRequested) {
          await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
          this.logger.log(`Job ${job.id} stopped before download start`);
          try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
          try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) { }
          if (_poll) clearInterval(_poll);
          try {
            if (subClient) {
              if (typeof subClient.unsubscribe === 'function') await subClient.unsubscribe(stopKey);
              if (typeof subClient.disconnect === 'function') await subClient.disconnect();
              if (typeof subClient.quit === 'function') await subClient.quit();
            }
          } catch (e) {}
          return { stopped: true };
        }
        await job.progress(20);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading-video', progress: 20 });
        try {
          await resumeDownload(videoUrl, videoTmp, { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, cancelFile, () => stopRequested);
        } catch (err: any) {
          if (String(err?.message || '').toLowerCase().includes('stopped')) {
            await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
            this.logger.log(`Job ${job.id} stopped during video download`);
            try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
            try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) { }
            if (_poll) clearInterval(_poll);
            try {
              if (subClient) {
                if (typeof subClient.unsubscribe === 'function') await subClient.unsubscribe(stopKey);
                if (typeof subClient.disconnect === 'function') await subClient.disconnect();
                if (typeof subClient.quit === 'function') await subClient.quit();
              }
            } catch (e) {}
            return { stopped: true };
          }
          throw err;
        }
        await job.progress(50);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading-audio', progress: 50 });
        try {
          await resumeDownload(audioUrl, audioTmp, { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, cancelFile, () => stopRequested);
        } catch (err: any) {
          if (String(err?.message || '').toLowerCase().includes('stopped')) {
            await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
            this.logger.log(`Job ${job.id} stopped during audio download`);
            try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
            try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) { }
            if (_poll) clearInterval(_poll);
            return { stopped: true };
          }
          throw err;
        }
        await job.progress(70);
        await this.history.appendEvent(job.id.toString(), { state: 'merging', progress: 70 });
        await this.ffmpeg.merge(videoTmp, audioTmp, outFile);
        await job.progress(95);
        try { fs.unlinkSync(videoTmp); } catch (e) { }
        try { fs.unlinkSync(audioTmp); } catch (e) { }
          try {
            if (fs.existsSync(stopFile) || stopRequested) {
              await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
              this.logger.log(`Job ${job.id} stopped during merge`);
              try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
              try { fs.unlinkSync(stopFile); } catch (e) { }
              if (_poll) clearInterval(_poll);
              return { stopped: true };
            }
          } catch (e) {}
          try { if (fs.existsSync(cancelFile)) fs.unlinkSync(cancelFile); } catch (e) { }
        await job.progress(100);
        await this.history.appendEvent(job.id.toString(), { state: 'finished', progress: 100, result: { path: outFile } });
        this.logger.log('Job finished, output: ' + outFile);
        return { path: outFile };
      }

      if (play.data?.durl?.length) {
        const url = play.data.durl[0].url;
        const outFile = path.join(DATA_DIR, `${bvid}-${job.id}.mp4`);
        if (fs.existsSync(cancelFile)) {
          await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 });
          this.logger.log(`Job ${job.id} cancelled before download start`);
          return { cancelled: true };
        }
        await job.progress(30);
        await this.history.appendEvent(job.id.toString(), { state: 'downloading', progress: 30 });
        await resumeDownload(url, outFile, { Referer: `https://www.bilibili.com/video/${bvid}`, Cookie: cookies }, cancelFile);
        await job.progress(100);
        try {
          if (fs.existsSync(stopFile) || stopRequested) {
            await this.history.appendEvent(job.id.toString(), { state: 'stopped', progress: 0 });
            this.logger.log(`Job ${job.id} stopped during download`);
            try { if (redisClient) await redisClient.del(stopKey); } catch (e) { }
            try { if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile); } catch (e) { }
            if (_poll) clearInterval(_poll);
            return { stopped: true };
          }
        } catch (e) {}
        try { if (fs.existsSync(cancelFile)) { await this.history.appendEvent(job.id.toString(), { state: 'cancelled', progress: 0 }); this.logger.log(`Job ${job.id} cancelled during download`); return { cancelled: true }; } } catch (e) {}
        await this.history.appendEvent(job.id.toString(), { state: 'finished', progress: 100, result: { path: outFile } });
        try { if (fs.existsSync(cancelFile)) fs.unlinkSync(cancelFile); } catch (e) { }
        this.logger.log('Job finished, output: ' + outFile);
        return { path: outFile };
      }

      throw new Error('No downloadable urls found or DRM-protected');
    } catch (err: any) {
      this.logger.error(`Job ${job.id} failed: ${err?.message || err}`);
      try { await job.progress(0); } catch (e) { }
      try { await this.history.appendEvent(job.id.toString(), { state: 'failed', progress: 0, message: err?.message }); } catch (e) { }
      throw err;
    }
  }
}
