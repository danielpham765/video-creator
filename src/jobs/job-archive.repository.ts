import { Injectable } from '@nestjs/common';
import { PostgresService } from '../db/postgres.service';

export interface ArchivedJobRecord {
  jobId: string;
  terminalState: 'finished' | 'failed' | 'cancelled';
  masterRaw: Record<string, any>;
  masterStatus: Record<string, any>;
  history: any[];
  partsProgressRaw: Record<string, any>;
  partJobsRaw: any[];
  partJobsSummary: Record<string, any>;
  redisKeysArchived: string[];
}

@Injectable()
export class JobArchiveRepository {
  constructor(private readonly postgres: PostgresService) {}

  isReady(): boolean {
    return this.postgres.isReady();
  }

  async upsert(record: ArchivedJobRecord): Promise<void> {
    await this.postgres.query(
      `
      INSERT INTO download_job_archive (
        job_id,
        terminal_state,
        master_raw,
        master_status,
        history,
        parts_progress_raw,
        part_jobs_raw,
        part_jobs_summary,
        redis_keys_archived,
        archived_at,
        updated_at
      )
      VALUES ($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,NOW(),NOW())
      ON CONFLICT (job_id)
      DO UPDATE SET
        terminal_state = EXCLUDED.terminal_state,
        master_raw = EXCLUDED.master_raw,
        master_status = EXCLUDED.master_status,
        history = EXCLUDED.history,
        parts_progress_raw = EXCLUDED.parts_progress_raw,
        part_jobs_raw = EXCLUDED.part_jobs_raw,
        part_jobs_summary = EXCLUDED.part_jobs_summary,
        redis_keys_archived = EXCLUDED.redis_keys_archived,
        updated_at = NOW()
      `,
      [
        record.jobId,
        record.terminalState,
        JSON.stringify(record.masterRaw || {}),
        JSON.stringify(record.masterStatus || {}),
        JSON.stringify(record.history || []),
        JSON.stringify(record.partsProgressRaw || {}),
        JSON.stringify(record.partJobsRaw || []),
        JSON.stringify(record.partJobsSummary || {}),
        JSON.stringify(record.redisKeysArchived || []),
      ],
    );
  }

  async getByJobId(jobId: string): Promise<ArchivedJobRecord | null> {
    const { rows } = await this.postgres.query<any>(
      `SELECT job_id, terminal_state, master_raw, master_status, history, parts_progress_raw, part_jobs_raw, part_jobs_summary, redis_keys_archived
       FROM download_job_archive WHERE job_id = $1 LIMIT 1`,
      [jobId],
    );
    const row = rows[0];
    if (!row) return null;
    return {
      jobId: String(row.job_id),
      terminalState: row.terminal_state,
      masterRaw: row.master_raw || {},
      masterStatus: row.master_status || {},
      history: Array.isArray(row.history) ? row.history : [],
      partsProgressRaw: row.parts_progress_raw || {},
      partJobsRaw: Array.isArray(row.part_jobs_raw) ? row.part_jobs_raw : [],
      partJobsSummary: row.part_jobs_summary || {},
      redisKeysArchived: Array.isArray(row.redis_keys_archived) ? row.redis_keys_archived : [],
    };
  }
}
