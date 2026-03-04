import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';

@Injectable()
export class PostgresService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PostgresService.name);
  private readonly client: string;
  private readonly host: string;
  private readonly port: number;
  private readonly user: string;
  private readonly password: string;
  private readonly db: string;
  private pool: Pool | null = null;

  constructor(private readonly config: ConfigService) {
    this.client = String(this.config.get<string>('database.client') || process.env.DATABASE_CLIENT || 'postgres').trim().toLowerCase();
    this.host = String(this.config.get<string>('database.host') || process.env.DATABASE_HOST || '').trim();
    this.port = Number(this.config.get<number>('database.port') || process.env.DATABASE_PORT || 5432);
    this.user = String(this.config.get<string>('database.user') || process.env.DATABASE_USER || '').trim();
    this.password = String(this.config.get<string>('database.password') || process.env.DATABASE_PASSWORD || '').trim();
    this.db = String(this.config.get<string>('database.db') || process.env.DATABASE_DB || '').trim();
  }

  async onModuleInit(): Promise<void> {
    if (this.client !== 'postgres') {
      this.logger.warn(`Database client "${this.client}" is not supported for archive; Redis-only mode remains active.`);
      return;
    }
    if (!this.host || !this.port || !this.user || !this.db) {
      this.logger.warn('Database config is incomplete (database.host/port/user/db); Redis-only mode remains active.');
      return;
    }
    try {
      this.pool = new Pool({
        host: this.host,
        port: this.port,
        user: this.user,
        password: this.password,
        database: this.db,
        max: 10,
      });
      await this.pool.query('SELECT 1');
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS download_job_archive (
          job_id TEXT PRIMARY KEY,
          terminal_state TEXT NOT NULL,
          master_raw JSONB NOT NULL,
          master_status JSONB NOT NULL,
          history JSONB NOT NULL,
          parts_progress_raw JSONB NOT NULL,
          part_jobs_raw JSONB NOT NULL,
          part_jobs_summary JSONB NOT NULL,
          redis_keys_archived JSONB NOT NULL,
          archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await this.pool.query('CREATE INDEX IF NOT EXISTS idx_download_job_archive_state_archived_at ON download_job_archive (terminal_state, archived_at DESC)');
      await this.pool.query('CREATE INDEX IF NOT EXISTS idx_download_job_archive_archived_at ON download_job_archive (archived_at DESC)');
      this.logger.log('Postgres archive table is ready');
    } catch (err: any) {
      this.logger.error(`Failed to initialize Postgres archive: ${String(err?.message || err)}`);
      if (this.pool) {
        try { await this.pool.end(); } catch {}
      }
      this.pool = null;
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.pool) {
      try { await this.pool.end(); } catch {}
      this.pool = null;
    }
  }

  isReady(): boolean {
    return Boolean(this.pool);
  }

  async isHealthy(): Promise<boolean> {
    if (!this.pool) return false;
    try {
      await this.pool.query('SELECT 1');
      return true;
    } catch {
      return false;
    }
  }

  async query<T = any>(sql: string, params: any[] = []): Promise<{ rows: T[] }> {
    if (!this.pool) throw new Error('postgres-unavailable');
    const res = await this.pool.query(sql, params);
    return { rows: res.rows as T[] };
  }
}
