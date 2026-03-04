import { InjectQueue } from '@nestjs/bull';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Queue } from 'bull';
import { RuntimeConfigService } from '../config/runtime-config.service';
import { JobArchiveRepository } from './job-archive.repository';
import { JobHistoryService } from './job-history.service';

const MASTER_STATE_KEYS = ['completed', 'failed', 'wait', 'active', 'paused', 'delayed'] as const;
const PART_STATE_KEYS = ['completed', 'failed', 'wait', 'active', 'paused', 'delayed'] as const;

@Injectable()
export class JobArchiveService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(JobArchiveService.name);
  private sweepTimer: NodeJS.Timeout | null = null;

  constructor(
    @InjectQueue('downloads') private readonly downloadQueue: Queue,
    @InjectQueue('download-parts') private readonly partsQueue: Queue,
    private readonly runtimeConfig: RuntimeConfigService,
    private readonly history: JobHistoryService,
    private readonly repo: JobArchiveRepository,
  ) {}

  onModuleInit(): void {
    if (!this.isArchiveEnabled()) return;

    const onTerminal = async (jobId: string, source: string) => {
      try {
        await this.archiveMasterJob(jobId, { reason: source });
      } catch (e: any) {
        this.logger.warn(`archive hook failed for job ${jobId}: ${String(e?.message || e)}`);
      }
    };

    this.downloadQueue.on('global:completed', async (jobId: string) => onTerminal(String(jobId), 'global:completed'));
    this.downloadQueue.on('global:failed', async (jobId: string) => onTerminal(String(jobId), 'global:failed'));

    const sweepOnStartup = Boolean(this.runtimeConfig.getGlobal('archive.sweepOnStartup') ?? true);
    if (sweepOnStartup) {
      setTimeout(() => {
        void this.sweepTerminalJobs(this.getBatchSize(), false, 'startup');
      }, 2500);
    }

    const intervalSec = Number(this.runtimeConfig.getGlobal('archive.sweepIntervalSec') ?? 60);
    if (Number.isFinite(intervalSec) && intervalSec > 0) {
      this.sweepTimer = setInterval(() => {
        void this.sweepTerminalJobs(this.getBatchSize(), false, 'interval');
      }, Math.floor(intervalSec * 1000));
    }
  }

  onModuleDestroy(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
  }

  async getArchivedJob(jobId: string): Promise<any | null> {
    if (!this.repo.isReady()) return null;
    try {
      return await this.repo.getByJobId(String(jobId));
    } catch {
      return null;
    }
  }

  async sweepTerminalJobs(limit = this.getBatchSize(), dryRun = false, reason = 'manual'): Promise<{ scanned: number; archived: number }> {
    const batch = Math.max(1, Number(limit) || 50);
    if (!this.isArchiveEnabled() || !this.repo.isReady()) return { scanned: 0, archived: 0 };

    const jobs = await this.downloadQueue.getJobs(['completed', 'failed'], 0, batch - 1);
    let archived = 0;
    for (const job of jobs) {
      const r = await this.archiveMasterJob(String(job.id), { dryRun, reason: `${reason}:sweep` });
      if (r.archived) archived += 1;
    }
    return { scanned: jobs.length, archived };
  }

  async archiveMasterJob(
    masterJobId: string,
    opts?: { dryRun?: boolean; reason?: string; forceTerminalState?: 'finished' | 'failed' | 'cancelled' },
  ): Promise<{ archived: boolean; skippedReason?: string }> {
    const jobId = String(masterJobId);
    if (!this.isArchiveEnabled()) return { archived: false, skippedReason: 'archive-disabled' };
    if (!this.repo.isReady()) return { archived: false, skippedReason: 'db-unavailable' };

    const redisClient: any = (this.downloadQueue as any).client;
    if (!redisClient) return { archived: false, skippedReason: 'redis-unavailable' };

    const masterJob = await this.downloadQueue.getJob(jobId as any);
    const masterRaw = await this.safeHgetAll(redisClient, `bull:downloads:${jobId}`);
    if (!masterJob && !masterRaw) {
      const existing = await this.getArchivedJob(jobId);
      if (existing) return { archived: false, skippedReason: 'already-archived' };
      return { archived: false, skippedReason: 'master-missing' };
    }

    const history = await this.history.getHistory(jobId);
    const terminalState = await this.resolveTerminalState(jobId, masterJob, masterRaw || {}, history, opts?.forceTerminalState);
    if (!terminalState) return { archived: false, skippedReason: 'not-terminal' };

    const masterStatus = this.buildMasterStatus(jobId, masterJob, masterRaw || {}, history, terminalState);
    const partsProgressRaw = (await this.safeHgetAll(redisClient, `job:parts:${jobId}`)) || {};

    const partIds = await this.findRelatedPartJobIds(jobId, redisClient);
    const partJobsRaw = await this.readPartJobs(partIds, redisClient);
    const partJobsSummary = this.summarizePartJobs(partJobsRaw);

    const redisKeysArchived = [
      `bull:downloads:${jobId}`,
      `job:parts:${jobId}`,
      `job:cancel:${jobId}`,
      `job:stop:${jobId}`,
      ...partJobsRaw.map((p) => `bull:download-parts:${p.partJobId}`),
      ...partJobsRaw.map((p) => `job:part:lock:${jobId}:${p.role || 'video'}:${p.partIndex ?? 'unknown'}`),
    ];

    await this.repo.upsert({
      jobId,
      terminalState,
      masterRaw: masterRaw || {},
      masterStatus,
      history,
      partsProgressRaw,
      partJobsRaw,
      partJobsSummary,
      redisKeysArchived,
    });

    if (opts?.dryRun) return { archived: true };

    await this.cleanupRedisArtifacts(jobId, masterJob, partJobsRaw, redisClient);
    this.logger.log(`Archived master job ${jobId} (${terminalState}) reason=${opts?.reason || 'n/a'}`);
    return { archived: true };
  }

  private isArchiveEnabled(): boolean {
    const enabled = this.runtimeConfig.getGlobal('archive.enabled');
    if (enabled === undefined || enabled === null) return true;
    return String(enabled).toLowerCase() !== 'false';
  }

  private getBatchSize(): number {
    const n = Number(this.runtimeConfig.getGlobal('archive.batchSize') ?? 50);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : 50;
  }

  private async resolveTerminalState(
    jobId: string,
    masterJob: any,
    masterRaw: Record<string, any>,
    history: any[],
    forced?: 'finished' | 'failed' | 'cancelled',
  ): Promise<'finished' | 'failed' | 'cancelled' | null> {
    if (forced) return forced;

    const hasCancelledHistory = Array.isArray(history) && history.some((h) => String(h?.state || '') === 'cancelled');
    if (hasCancelledHistory) return 'cancelled';

    if (masterJob) {
      try {
        const state = String(await masterJob.getState());
        if (state === 'failed') return 'failed';
        if (state === 'completed') {
          const rv = masterJob.returnvalue || {};
          if (rv?.cancelled === true) return 'cancelled';
          return 'finished';
        }
      } catch {}
    }

    if (masterRaw.failedReason) return 'failed';

    const redisClient: any = (this.downloadQueue as any).client;
    if (!redisClient) return null;
    const inCompleted = await redisClient.zscore('bull:downloads:completed', jobId);
    if (inCompleted !== null && inCompleted !== undefined) {
      const rv = this.safeJsonParse(masterRaw.returnvalue, {});
      if (rv?.cancelled === true) return 'cancelled';
      return 'finished';
    }
    const inFailed = await redisClient.zscore('bull:downloads:failed', jobId);
    if (inFailed !== null && inFailed !== undefined) return 'failed';
    return null;
  }

  private buildMasterStatus(
    jobId: string,
    masterJob: any,
    masterRaw: Record<string, any>,
    history: any[],
    state: 'finished' | 'failed' | 'cancelled',
  ): Record<string, any> {
    const data = masterJob?.data || this.safeJsonParse(masterRaw?.data, {});
    const result = masterJob?.returnvalue || this.safeJsonParse(masterRaw?.returnvalue, null);
    const failedReason = masterJob?.failedReason || masterRaw?.failedReason || null;
    const progressRaw = masterJob ? masterJob._progress : masterRaw?.progress;
    const progress = Number(progressRaw || 0) || 0;
    return {
      id: jobId,
      state,
      progress,
      failedReason,
      result,
      data,
      history: Array.isArray(history) ? history : [],
      attemptsMade: Number(masterJob?.attemptsMade || 0) || 0,
    };
  }

  private async findRelatedPartJobIds(jobId: string, redisClient: any): Promise<string[]> {
    const ids = new Set<string>();

    const indexKey = `job:parts:index:${jobId}`;
    try {
      if (typeof redisClient.smembers === 'function') {
        const indexed: string[] = await redisClient.smembers(indexKey);
        for (const id of indexed || []) {
          const s = String(id || '').trim();
          if (s) ids.add(s);
        }
      }
    } catch {}

    if (ids.size > 0) return Array.from(ids);

    let cursor = '0';
    do {
      const [next, keys] = await redisClient.scan(cursor, 'MATCH', 'bull:download-parts:*', 'COUNT', 1000);
      cursor = String(next || '0');
      const candidates = (Array.isArray(keys) ? keys : []).filter((k: string) => /^bull:download-parts:\d+$/.test(String(k || '')));
      if (candidates.length === 0) continue;

      const pipe = redisClient.pipeline();
      for (const key of candidates) pipe.hget(key, 'data');
      const rows = await pipe.exec();
      for (let i = 0; i < candidates.length; i++) {
        const key = candidates[i];
        const partId = key.split(':').pop() || '';
        const rawData = rows?.[i]?.[1];
        const data = this.safeJsonParse(rawData, {});
        if (String(data?.jobId || '') === jobId && partId) ids.add(partId);
      }
    } while (cursor !== '0');

    return Array.from(ids);
  }

  private async readPartJobs(partIds: string[], redisClient: any): Promise<any[]> {
    const out: any[] = [];
    if (!partIds.length) return out;

    for (const partId of partIds) {
      const key = `bull:download-parts:${partId}`;
      const raw = await this.safeHgetAll(redisClient, key);
      if (!raw) continue;
      const data = this.safeJsonParse(raw.data, {});
      const returnvalue = this.safeJsonParse(raw.returnvalue, null);
      const memberships = await this.resolvePartMemberships(partId, redisClient);
      out.push({
        partJobId: partId,
        raw,
        data,
        role: data?.role || 'video',
        partIndex: data?.partIndex,
        returnvalue,
        progress: Number(raw?.progress || 0) || 0,
        failedReason: raw?.failedReason || null,
        memberships,
      });
    }

    return out;
  }

  private async resolvePartMemberships(partId: string, redisClient: any): Promise<string[]> {
    const states: string[] = [];
    try {
      const pipe = redisClient.pipeline();
      for (const state of PART_STATE_KEYS) {
        pipe.zscore(`bull:download-parts:${state}`, partId);
      }
      const rows = await pipe.exec();
      for (let i = 0; i < PART_STATE_KEYS.length; i++) {
        const score = rows?.[i]?.[1];
        if (score !== null && score !== undefined) states.push(PART_STATE_KEYS[i]);
      }
    } catch {}
    return states;
  }

  private summarizePartJobs(partJobsRaw: any[]): Record<string, any> {
    const stateCounts: Record<string, number> = {};
    let total = 0;
    let failed = 0;
    for (const p of partJobsRaw) {
      total += 1;
      const states: string[] = Array.isArray(p?.memberships) ? p.memberships : [];
      const key = states[0] || (p?.failedReason ? 'failed' : 'unknown');
      stateCounts[key] = (stateCounts[key] || 0) + 1;
      if (p?.failedReason) failed += 1;
    }
    return { total, failed, stateCounts };
  }

  private async cleanupRedisArtifacts(jobId: string, masterJob: any, partJobsRaw: any[], redisClient: any): Promise<void> {
    if (masterJob) {
      try {
        await masterJob.remove();
      } catch {}
    }

    for (const p of partJobsRaw) {
      const partId = String(p?.partJobId || '');
      if (!partId) continue;
      try {
        const partJob = await this.partsQueue.getJob(partId as any);
        if (partJob) {
          try { await partJob.remove(); } catch {}
        }
      } catch {}

      try { await redisClient.del(`bull:download-parts:${partId}`); } catch {}
      for (const state of PART_STATE_KEYS) {
        try { await redisClient.zrem(`bull:download-parts:${state}`, partId); } catch {}
      }
    }

    try { await redisClient.del(`job:parts:${jobId}`); } catch {}
    try { await redisClient.del(`job:parts:index:${jobId}`); } catch {}
    try { await redisClient.del(`job:stop:${jobId}`); } catch {}
    try { await redisClient.del(`job:cancel:${jobId}`); } catch {}

    try { await redisClient.del(`bull:downloads:${jobId}`); } catch {}
    for (const state of MASTER_STATE_KEYS) {
      try { await redisClient.zrem(`bull:downloads:${state}`, jobId); } catch {}
    }
  }

  private safeJsonParse(raw: any, fallback: any): any {
    if (raw === null || raw === undefined || raw === '') return fallback;
    if (typeof raw !== 'string') return raw;
    try {
      return JSON.parse(raw);
    } catch {
      return fallback;
    }
  }

  private async safeHgetAll(redisClient: any, key: string): Promise<Record<string, any> | null> {
    try {
      const data = await redisClient.hgetall(key);
      if (!data || typeof data !== 'object' || Object.keys(data).length === 0) return null;
      return data as Record<string, any>;
    } catch {
      return null;
    }
  }
}
