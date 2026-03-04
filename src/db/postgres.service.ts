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
          archive_key TEXT PRIMARY KEY,
          run_id TEXT NOT NULL DEFAULT 'legacy',
          job_id TEXT NOT NULL,
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
      await this.ensureArchiveSchema();
      await this.pool.query('CREATE UNIQUE INDEX IF NOT EXISTS idx_download_job_archive_archive_key ON download_job_archive (archive_key)');
      await this.pool.query('CREATE INDEX IF NOT EXISTS idx_download_job_archive_job_id_updated_at ON download_job_archive (job_id, updated_at DESC)');
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

  private async ensureArchiveSchema(): Promise<void> {
    if (!this.pool) return;
    await this.pool.query(`ALTER TABLE download_job_archive ADD COLUMN IF NOT EXISTS archive_key TEXT`);
    await this.pool.query(`ALTER TABLE download_job_archive ADD COLUMN IF NOT EXISTS run_id TEXT NOT NULL DEFAULT 'legacy'`);
    await this.pool.query(`ALTER TABLE download_job_archive ALTER COLUMN job_id SET NOT NULL`);
    await this.pool.query(`
      UPDATE download_job_archive
      SET run_id = 'legacy'
      WHERE run_id IS NULL OR run_id = ''
    `);
    await this.pool.query(`
      UPDATE download_job_archive
      SET archive_key = 'legacy:' || job_id
      WHERE archive_key IS NULL OR archive_key = ''
    `);
    await this.pool.query(`ALTER TABLE download_job_archive ALTER COLUMN archive_key SET NOT NULL`);

    const { rows: pkRows } = await this.pool.query<{ constraint_name: string; column_name: string }>(
      `
      SELECT tc.constraint_name, kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
      WHERE tc.table_schema = 'public'
        AND tc.table_name = 'download_job_archive'
        AND tc.constraint_type = 'PRIMARY KEY'
      `,
    );
    const pkColumns = new Set((pkRows || []).map((r) => String(r.column_name)));
    const pkName = pkRows?.[0]?.constraint_name ? String(pkRows[0].constraint_name) : '';
    if (pkColumns.has('job_id') && pkName) {
      await this.pool.query(`ALTER TABLE download_job_archive DROP CONSTRAINT ${this.quoteIdent(pkName)}`);
    }

    const { rows: hasArchivePkRows } = await this.pool.query<{ ok: number }>(
      `
      SELECT 1 AS ok
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
      WHERE tc.table_schema = 'public'
        AND tc.table_name = 'download_job_archive'
        AND tc.constraint_type = 'PRIMARY KEY'
        AND kcu.column_name = 'archive_key'
      LIMIT 1
      `,
    );
    if (!hasArchivePkRows?.length) {
      await this.pool.query(`ALTER TABLE download_job_archive ADD CONSTRAINT download_job_archive_pkey PRIMARY KEY (archive_key)`);
    }
  }

  private quoteIdent(v: string): string {
    return `"${String(v).replace(/"/g, '""')}"`;
  }
}
