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
  private static readonly SWEEP_LOCK_KEY = 'job:archive:sweep:lock';
  private static readonly RUN_ID_KEY = 'job:archive:run-id';
  private runId = this.generateRunId();

  constructor(
    @InjectQueue('downloads') private readonly downloadQueue: Queue,
    @InjectQueue('download-parts') private readonly partsQueue: Queue,
    private readonly runtimeConfig: RuntimeConfigService,
    private readonly history: JobHistoryService,
    private readonly repo: JobArchiveRepository,
  ) {}

  async onModuleInit(): Promise<void> {
    if (!this.isArchiveEnabled()) return;
    await this.initializeRunId();

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

    const redisClient: any = (this.downloadQueue as any).client;
    if (!redisClient) return { scanned: 0, archived: 0 };
    const sweepLockToken = await this.acquireRedisLock(redisClient, JobArchiveService.SWEEP_LOCK_KEY, this.getSweepLockMs());
    if (!sweepLockToken) return { scanned: 0, archived: 0 };

    const jobs = await this.downloadQueue.getJobs(['completed', 'failed'], 0, batch - 1);
    try {
      let archived = 0;
      const processedMasterIds = new Set<string>();
      for (const job of jobs) {
        const id = String(job.id);
        processedMasterIds.add(id);
        const r = await this.archiveMasterJob(id, { dryRun, reason: `${reason}:sweep` });
        if (r.archived) archived += 1;
      }
      const orphanMasterIds = await this.findOrphanMasterIds(redisClient, batch, processedMasterIds);
      for (const orphanId of orphanMasterIds) {
        const r = await this.archiveMasterJob(orphanId, { dryRun, reason: `${reason}:orphan-sweep` });
        if (r.archived) archived += 1;
      }
      return { scanned: jobs.length + orphanMasterIds.length, archived };
    } finally {
      await this.releaseRedisLock(redisClient, JobArchiveService.SWEEP_LOCK_KEY, sweepLockToken);
    }
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
    const lockKey = `job:archive:lock:${jobId}`;
    const lockToken = await this.acquireRedisLock(redisClient, lockKey, this.getJobLockMs());
    if (!lockToken) return { archived: false, skippedReason: 'locked' };

    try {
      const masterJob = await this.downloadQueue.getJob(jobId as any);
      const masterRaw = await this.safeHgetAll(redisClient, `bull:downloads:${jobId}`);
      const queueState = await this.resolveMasterQueueState(jobId, masterJob, masterRaw || {}, redisClient);
      if (this.hasRetryPending(masterJob, masterRaw || {}, queueState)) {
        return { archived: false, skippedReason: 'retry-pending' };
      }
      const history = await this.history.getHistory(jobId);
      const partsProgressRaw = (await this.safeHgetAll(redisClient, `job:parts:${jobId}`)) || {};
      const partIds = await this.findRelatedPartJobIds(jobId, redisClient);
      const partJobsRaw = await this.readPartJobs(partIds, redisClient);
      if (this.hasInFlightPartJobs(partJobsRaw)) {
        return { archived: false, skippedReason: 'parts-inflight' };
      }
      const hasRecoverableArtifacts = Boolean(
        (Array.isArray(history) && history.length > 0)
        || Object.keys(partsProgressRaw).length > 0
        || partJobsRaw.length > 0,
      );

      if (!masterJob && !masterRaw && !hasRecoverableArtifacts) {
        const existing = await this.getArchivedJob(jobId);
        if (existing) return { archived: false, skippedReason: 'already-archived' };
        return { archived: false, skippedReason: 'master-missing' };
      }

      let terminalState = await this.resolveTerminalState(jobId, masterJob, masterRaw || {}, history, opts?.forceTerminalState);
      if (!terminalState && hasRecoverableArtifacts) {
        terminalState = this.inferTerminalStateFromArtifacts(history, partJobsRaw);
      }
      if (!terminalState) return { archived: false, skippedReason: 'not-terminal' };

      const masterStatus = this.buildMasterStatus(jobId, masterJob, masterRaw || {}, history, terminalState);
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
        archiveKey: `${this.runId}:${jobId}`,
        runId: this.runId,
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
    } finally {
      await this.releaseRedisLock(redisClient, lockKey, lockToken);
    }
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

  private getJobLockMs(): number {
    const n = Number(this.runtimeConfig.getGlobal('archive.jobLockMs') ?? 120000);
    return Number.isFinite(n) && n >= 1000 ? Math.floor(n) : 120000;
  }

  private getSweepLockMs(): number {
    const n = Number(this.runtimeConfig.getGlobal('archive.sweepLockMs') ?? 55000);
    return Number.isFinite(n) && n >= 1000 ? Math.floor(n) : 55000;
  }

  private async initializeRunId(): Promise<void> {
    const fromConfig = String(this.runtimeConfig.getGlobal('archive.runId') || process.env.ARCHIVE_RUN_ID || '').trim();
    if (fromConfig) {
      this.runId = this.normalizeRunId(fromConfig);
      this.logger.log(`Archive run id (configured): ${this.runId}`);
      return;
    }

    const redisClient: any = (this.downloadQueue as any).client;
    if (!redisClient) {
      this.runId = this.normalizeRunId(this.generateRunId());
      this.logger.warn(`Archive run id fallback (no redis): ${this.runId}`);
      return;
    }

    const candidate = this.normalizeRunId(this.generateRunId());
    try {
      await redisClient.set(JobArchiveService.RUN_ID_KEY, candidate, 'NX');
      const shared = String((await redisClient.get(JobArchiveService.RUN_ID_KEY)) || '').trim();
      this.runId = this.normalizeRunId(shared || candidate);
      this.logger.log(`Archive run id: ${this.runId}`);
      return;
    } catch {
      this.runId = candidate;
      this.logger.warn(`Archive run id fallback (redis error): ${this.runId}`);
    }
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
    const failedReasonRaw = masterJob?.failedReason || masterRaw?.failedReason || null;
    const failedReason = state === 'failed' ? failedReasonRaw : null;
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

  private async findOrphanMasterIds(redisClient: any, limit: number, exclude = new Set<string>()): Promise<string[]> {
    const ids = new Set<string>();
    let cursor = '0';
    const max = Math.max(1, Number(limit) || 50);
    do {
      const [next, keys] = await redisClient.scan(cursor, 'MATCH', 'job:parts:*', 'COUNT', 500);
      cursor = String(next || '0');
      for (const key of Array.isArray(keys) ? keys : []) {
        const m = /^job:parts:(.+)$/.exec(String(key || ''));
        if (!m) continue;
        const jobId = String(m[1] || '').trim();
        if (!jobId || jobId.startsWith('index:') || exclude.has(jobId)) continue;
        const exists = await redisClient.exists(`bull:downloads:${jobId}`);
        if (!exists) ids.add(jobId);
        if (ids.size >= max) return Array.from(ids);
      }
    } while (cursor !== '0');

    if (ids.size >= max) return Array.from(ids);

    cursor = '0';
    do {
      const [next, keys] = await redisClient.scan(cursor, 'MATCH', 'bull:download-parts:*', 'COUNT', 500);
      cursor = String(next || '0');
      const partKeys = (Array.isArray(keys) ? keys : []).filter((k: string) => /^bull:download-parts:\d+$/.test(String(k || '')));
      if (!partKeys.length) continue;

      const pipe = redisClient.pipeline();
      for (const key of partKeys) pipe.hget(key, 'data');
      const rows = await pipe.exec();

      for (let i = 0; i < partKeys.length; i++) {
        const rawData = rows?.[i]?.[1];
        const data = this.safeJsonParse(rawData, {});
        const jobId = String(data?.jobId || '').trim();
        if (!jobId || exclude.has(jobId)) continue;
        const exists = await redisClient.exists(`bull:downloads:${jobId}`);
        if (!exists) ids.add(jobId);
        if (ids.size >= max) return Array.from(ids);
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

  private hasInFlightPartJobs(partJobsRaw: any[]): boolean {
    if (!Array.isArray(partJobsRaw) || partJobsRaw.length === 0) return false;
    for (const p of partJobsRaw) {
      const states: string[] = Array.isArray(p?.memberships) ? p.memberships.map((s: any) => String(s || '').toLowerCase()) : [];
      if (states.includes('active') || states.includes('wait') || states.includes('delayed') || states.includes('paused')) {
        return true;
      }
    }
    return false;
  }

  private inferTerminalStateFromArtifacts(
    history: any[],
    partJobsRaw: any[],
  ): 'finished' | 'failed' | 'cancelled' | null {
    if (Array.isArray(history) && history.length > 0) {
      for (let i = history.length - 1; i >= 0; i--) {
        const s = String(history[i]?.state || '').toLowerCase();
        if (s === 'cancelled') return 'cancelled';
        if (s === 'failed' || s === 'error') return 'failed';
        if (s === 'finished' || s === 'completed') return 'finished';
      }
    }

    if (Array.isArray(partJobsRaw) && partJobsRaw.length > 0) {
      const hasFailed = partJobsRaw.some((p) => Boolean(p?.failedReason) || (Array.isArray(p?.memberships) && p.memberships.includes('failed')));
      if (hasFailed) return 'failed';
      const allCompleted = partJobsRaw.every((p) => Array.isArray(p?.memberships) && p.memberships.includes('completed'));
      if (allCompleted) return 'finished';
    }

    return null;
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

  private hasRetryPending(masterJob: any, masterRaw: Record<string, any>, queueState: string | null): boolean {
    const fromJobAttempts = Number(masterJob?.opts?.attempts);
    const fromRawOpts = this.safeJsonParse(masterRaw?.opts, {});
    const fromRawAttempts = Number(fromRawOpts?.attempts);
    const attempts = Number.isFinite(fromJobAttempts) && fromJobAttempts > 0
      ? fromJobAttempts
      : (Number.isFinite(fromRawAttempts) && fromRawAttempts > 0 ? fromRawAttempts : 1);

    const fromJobAttemptsMade = Number(masterJob?.attemptsMade);
    const fromRawAttemptsMade = Number(masterRaw?.attemptsMade);
    const attemptsMade = Number.isFinite(fromJobAttemptsMade) && fromJobAttemptsMade >= 0
      ? fromJobAttemptsMade
      : (Number.isFinite(fromRawAttemptsMade) && fromRawAttemptsMade >= 0 ? fromRawAttemptsMade : 0);

    if (attemptsMade >= attempts) return false;
    const s = String(queueState || '').toLowerCase();
    return s === 'failed' || s === 'delayed' || s === 'wait' || s === 'active' || s === 'paused';
  }

  private async resolveMasterQueueState(
    jobId: string,
    masterJob: any,
    masterRaw: Record<string, any>,
    redisClient: any,
  ): Promise<string | null> {
    if (masterJob) {
      try {
        const state = String(await masterJob.getState());
        if (state) return state;
      } catch {}
    }

    if (!redisClient) return null;
    for (const state of MASTER_STATE_KEYS) {
      try {
        const score = await redisClient.zscore(`bull:downloads:${state}`, jobId);
        if (score !== null && score !== undefined) return state;
      } catch {}
    }

    if (masterRaw?.failedReason) return 'failed';
    return null;
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

  private generateRunId(): string {
    const stamp = new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 17);
    const rnd = Math.random().toString(36).slice(2, 10);
    return `r${stamp}-${rnd}`;
  }

  private normalizeRunId(v: string): string {
    const compact = String(v || '').trim().replace(/[^a-zA-Z0-9:_-]/g, '-').slice(0, 80);
    return compact || this.generateRunId();
  }

  private async acquireRedisLock(redisClient: any, key: string, ttlMs: number): Promise<string | null> {
    const token = `${process.pid}:${Date.now()}:${Math.random().toString(36).slice(2, 10)}`;
    try {
      const ok = await redisClient.set(key, token, 'PX', Math.max(1000, ttlMs), 'NX');
      return ok ? token : null;
    } catch {
      return null;
    }
  }

  private async releaseRedisLock(redisClient: any, key: string, token: string): Promise<void> {
    if (!token) return;
    const lua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;
    try {
      await redisClient.eval(lua, 1, key, token);
    } catch {
      // ignore lock release errors
    }
  }
}
