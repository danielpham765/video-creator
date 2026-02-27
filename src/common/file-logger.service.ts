import { LoggerService } from '@nestjs/common';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import loadYamlConfig from '../config/config.loader';

type Level = 'info' | 'warn' | 'error' | 'debug';

type ArchiveDestination = 'local' | 's3';

interface RotateS3Config {
  bucket: string;
  region: string;
  prefix: string;
  endpoint?: string;
  forcePathStyle?: boolean;
  accessKeyId?: string;
  secretAccessKey?: string;
  sessionToken?: string;
  retentionDays: number;
}

interface RotateLocalConfig {
  maxFiles: number;
  retentionDays: number;
}

interface RotateConfig {
  enabled: boolean;
  maxFileSizeBytes: number;
  checkIntervalMs: number;
  destination: ArchiveDestination;
  local: RotateLocalConfig;
  s3: RotateS3Config;
}

export class FileLoggerService implements LoggerService {
  private readonly isWorker: boolean;
  private readonly workerLabel: string | null;
  private readonly logDir: string;
  private readonly infoPath: string;
  private readonly warnPath: string;
  private readonly errorPath: string;
  private readonly debugPath: string;
  private readonly aggregateInfoPath: string | null;
  private readonly aggregateWarnPath: string | null;
  private readonly aggregateErrorPath: string | null;
  private readonly aggregateDebugPath: string | null;
  private readonly archiveDir: string;
  private readonly rotate: RotateConfig;

  private lastRotateCheckAt = 0;
  private isRotating = false;

  constructor() {
    const cfg = loadYamlConfig() as any;
    this.rotate = this.resolveRotateConfig(cfg);

    const root = path.resolve(process.cwd(), 'logs');
    this.archiveDir = path.join(root, 'archived');
    fs.mkdirSync(this.archiveDir, { recursive: true });

    this.isWorker = String(process.env.WORKER || '').toLowerCase() === 'true';
    this.workerLabel = this.isWorker ? this.resolveWorkerLabel() : null;
    this.logDir = this.isWorker ? path.join(root, 'worker', this.workerLabel || 'worker-unknown') : path.join(root, 'api');

    fs.mkdirSync(this.logDir, { recursive: true });
    this.infoPath = path.join(this.logDir, 'app.info.log');
    this.warnPath = path.join(this.logDir, 'app.warn.log');
    this.errorPath = path.join(this.logDir, 'app.error.log');
    this.debugPath = path.join(this.logDir, 'app.debug.log');

    if (this.isWorker) {
      const workerRoot = path.join(root, 'worker');
      fs.mkdirSync(workerRoot, { recursive: true });
      this.aggregateInfoPath = path.join(workerRoot, 'app.info.log');
      this.aggregateWarnPath = path.join(workerRoot, 'app.warn.log');
      this.aggregateErrorPath = path.join(workerRoot, 'app.error.log');
      this.aggregateDebugPath = path.join(workerRoot, 'app.debug.log');
    } else {
      this.aggregateInfoPath = null;
      this.aggregateWarnPath = null;
      this.aggregateErrorPath = null;
      this.aggregateDebugPath = null;
    }

    this.ensureLogFiles();
  }

  log(message: any, ...optionalParams: any[]) {
    this.write('info', message, this.extractContext(optionalParams));
  }

  warn(message: any, ...optionalParams: any[]) {
    this.write('warn', message, this.extractContext(optionalParams));
  }

  error(message: any, ...optionalParams: any[]) {
    const context = this.extractContext(optionalParams);
    const trace = this.extractTrace(optionalParams, context);
    const payload = trace ? `${this.stringify(message)} | trace=${trace}` : message;
    this.write('error', payload, context);
  }

  debug(message: any, ...optionalParams: any[]) {
    this.write('debug', message, this.extractContext(optionalParams));
  }

  verbose(message: any, ...optionalParams: any[]) {
    this.write('debug', message, this.extractContext(optionalParams));
  }

  private write(level: Level, message: any, context?: string): void {
    const line = this.formatLine(level, message, context);

    // Console only prints info logs.
    if (level === 'info') {
      // eslint-disable-next-line no-console
      console.log(line);
    }

    // app.debug.log keeps all levels.
    this.appendLine(this.debugPath, line);
    if (this.aggregateDebugPath) this.appendLine(this.aggregateDebugPath, line);

    // app.info.log keeps info + warn + error.
    if (level === 'info' || level === 'warn' || level === 'error') {
      this.appendLine(this.infoPath, line);
      if (this.aggregateInfoPath) this.appendLine(this.aggregateInfoPath, line);
    }

    // app.warn.log keeps warn + error.
    if (level === 'warn' || level === 'error') {
      this.appendLine(this.warnPath, line);
      if (this.aggregateWarnPath) this.appendLine(this.aggregateWarnPath, line);
    }

    // app.error.log keeps error only.
    if (level === 'error') {
      this.appendLine(this.errorPath, line);
      if (this.aggregateErrorPath) this.appendLine(this.aggregateErrorPath, line);
    }

    this.checkRotateIfNeeded();
  }

  private appendLine(filePath: string, line: string): void {
    try {
      fs.appendFileSync(filePath, `${line}\n`, 'utf8');
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error(`[logger] failed writing ${filePath}: ${this.stringify(e)}`);
    }
  }

  private ensureLogFiles(): void {
    const paths = this.getManagedLogFiles();
    for (const p of paths) {
      try {
        fs.closeSync(fs.openSync(p, 'a'));
      } catch (e) {
        // ignore
      }
    }
  }

  private getManagedLogFiles(): string[] {
    const paths = [this.infoPath, this.warnPath, this.errorPath, this.debugPath];
    if (this.aggregateInfoPath) paths.push(this.aggregateInfoPath);
    if (this.aggregateWarnPath) paths.push(this.aggregateWarnPath);
    if (this.aggregateErrorPath) paths.push(this.aggregateErrorPath);
    if (this.aggregateDebugPath) paths.push(this.aggregateDebugPath);
    return Array.from(new Set(paths));
  }

  private checkRotateIfNeeded(): void {
    if (!this.rotate.enabled || this.isRotating) return;
    const now = Date.now();
    if (now - this.lastRotateCheckAt < this.rotate.checkIntervalMs) return;
    this.lastRotateCheckAt = now;

    const files = this.getManagedLogFiles();
    const exceeded = files.some((f) => {
      try {
        return fs.existsSync(f) && fs.statSync(f).size >= this.rotate.maxFileSizeBytes;
      } catch (e) {
        return false;
      }
    });

    if (!exceeded) return;

    this.isRotating = true;
    void this.rotateAllLogs()
      .catch((err) => {
        // eslint-disable-next-line no-console
        console.error(`[logger] rotate failed: ${this.stringify(err)}`);
      })
      .finally(() => {
        this.isRotating = false;
      });
  }

  private async rotateAllLogs(): Promise<void> {
    const files = this.getManagedLogFiles().filter((f) => {
      try {
        return fs.existsSync(f) && fs.statSync(f).size > 0;
      } catch (e) {
        return false;
      }
    });
    if (!files.length) return;

    const stamp = this.formatArchiveTimestamp(new Date());

    if (this.rotate.destination === 's3') {
      const tmpZip = this.resolveArchivePath(os.tmpdir(), stamp);
      await this.createZip(tmpZip, files);
      await this.uploadArchiveToS3(tmpZip);
      try { fs.unlinkSync(tmpZip); } catch (e) {}
      this.truncateLogFiles(files);
      await this.cleanupS3Archives();
      return;
    }

    const zipPath = this.resolveArchivePath(this.archiveDir, stamp);
    await this.createZip(zipPath, files);
    this.truncateLogFiles(files);
    this.cleanupLocalArchives();
  }

  private resolveArchivePath(dir: string, stamp: string): string {
    const base = path.join(dir, `${stamp}.zip`);
    if (!fs.existsSync(base)) return base;
    for (let i = 1; i <= 99; i++) {
      const p = path.join(dir, `${stamp}_${i}.zip`);
      if (!fs.existsSync(p)) return p;
    }
    return path.join(dir, `${stamp}_${Date.now()}.zip`);
  }

  private truncateLogFiles(files: string[]): void {
    for (const filePath of files) {
      try {
        fs.writeFileSync(filePath, '', 'utf8');
      } catch (e) {
        // ignore truncate failures
      }
    }
  }

  private async createZip(zipPath: string, files: string[]): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const archiver = require('archiver');

    await new Promise<void>((resolve, reject) => {
      const output = fs.createWriteStream(zipPath);
      const archive = archiver('zip', { zlib: { level: 9 } });

      output.on('close', () => resolve());
      output.on('error', (err: any) => reject(err));
      archive.on('error', (err: any) => reject(err));

      archive.pipe(output);

      for (const filePath of files) {
        const name = this.toArchiveEntryName(filePath);
        archive.file(filePath, { name });
      }

      try {
        archive.finalize();
      } catch (err) {
        reject(err);
      }
    });
  }

  private toArchiveEntryName(filePath: string): string {
    const logsRoot = path.resolve(process.cwd(), 'logs');
    const rel = path.relative(logsRoot, filePath).replace(/\\/g, '/');
    if (rel && !rel.startsWith('..')) return rel;
    return path.basename(filePath);
  }

  private cleanupLocalArchives(): void {
    try {
      const files = fs
        .readdirSync(this.archiveDir)
        .filter((f) => f.toLowerCase().endsWith('.zip'))
        .map((name) => {
          const p = path.join(this.archiveDir, name);
          const stat = fs.statSync(p);
          return { name, path: p, mtimeMs: stat.mtimeMs };
        })
        .sort((a, b) => b.mtimeMs - a.mtimeMs);

      if (this.rotate.local.retentionDays > 0) {
        const cutoff = Date.now() - this.rotate.local.retentionDays * 24 * 60 * 60 * 1000;
        for (const f of files) {
          if (f.mtimeMs < cutoff) {
            try { fs.unlinkSync(f.path); } catch (e) {}
          }
        }
      }

      if (this.rotate.local.maxFiles > 0) {
        const refreshed = fs
          .readdirSync(this.archiveDir)
          .filter((f) => f.toLowerCase().endsWith('.zip'))
          .map((name) => {
            const p = path.join(this.archiveDir, name);
            const stat = fs.statSync(p);
            return { name, path: p, mtimeMs: stat.mtimeMs };
          })
          .sort((a, b) => b.mtimeMs - a.mtimeMs);

        for (let i = this.rotate.local.maxFiles; i < refreshed.length; i++) {
          try { fs.unlinkSync(refreshed[i].path); } catch (e) {}
        }
      }
    } catch (e) {
      // ignore cleanup failure
    }
  }

  private async uploadArchiveToS3(zipPath: string): Promise<void> {
    const s3 = this.rotate.s3;
    if (!s3.bucket) {
      throw new Error('logging.rotate.s3.bucket is required when destination=s3');
    }

    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

    const client = new S3Client({
      region: s3.region,
      endpoint: s3.endpoint || undefined,
      forcePathStyle: Boolean(s3.forcePathStyle),
      credentials: s3.accessKeyId && s3.secretAccessKey
        ? {
            accessKeyId: s3.accessKeyId,
            secretAccessKey: s3.secretAccessKey,
            sessionToken: s3.sessionToken || undefined,
          }
        : undefined,
    });

    const keyPrefix = String(s3.prefix || '').replace(/^\/+|\/+$/g, '');
    const objectKey = keyPrefix ? `${keyPrefix}/${path.basename(zipPath)}` : path.basename(zipPath);

    await client.send(
      new PutObjectCommand({
        Bucket: s3.bucket,
        Key: objectKey,
        Body: fs.createReadStream(zipPath),
        ContentType: 'application/zip',
      }),
    );
  }

  private async cleanupS3Archives(): Promise<void> {
    const s3 = this.rotate.s3;
    if (!s3.bucket || s3.retentionDays <= 0) return;

    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { S3Client, ListObjectsV2Command, DeleteObjectsCommand } = require('@aws-sdk/client-s3');

    const client = new S3Client({
      region: s3.region,
      endpoint: s3.endpoint || undefined,
      forcePathStyle: Boolean(s3.forcePathStyle),
      credentials: s3.accessKeyId && s3.secretAccessKey
        ? {
            accessKeyId: s3.accessKeyId,
            secretAccessKey: s3.secretAccessKey,
            sessionToken: s3.sessionToken || undefined,
          }
        : undefined,
    });

    const cutoff = Date.now() - s3.retentionDays * 24 * 60 * 60 * 1000;
    const keyPrefix = String(s3.prefix || '').replace(/^\/+|\/+$/g, '');

    let continuationToken: string | undefined;
    do {
      const listed = await client.send(
        new ListObjectsV2Command({
          Bucket: s3.bucket,
          Prefix: keyPrefix || undefined,
          ContinuationToken: continuationToken,
        }),
      );

      const stale = (listed.Contents || [])
        .filter((o: any) => o.Key && o.LastModified && new Date(o.LastModified).getTime() < cutoff)
        .map((o: any) => ({ Key: o.Key }));

      if (stale.length) {
        await client.send(
          new DeleteObjectsCommand({
            Bucket: s3.bucket,
            Delete: { Objects: stale, Quiet: true },
          }),
        );
      }

      continuationToken = listed.IsTruncated ? listed.NextContinuationToken : undefined;
    } while (continuationToken);
  }

  private resolveRotateConfig(cfg: any): RotateConfig {
    const rotateCfg = cfg?.logging?.rotate || {};

    const enabled = this.getEnvBoolean('LOG_ROTATE_ENABLED', rotateCfg.enabled, true);
    const maxFileSizeMb = this.getEnvNumber('LOG_ROTATE_MAX_FILE_SIZE_MB', rotateCfg.maxFileSizeMb, 5);
    const checkIntervalMs = this.getEnvNumber('LOG_ROTATE_CHECK_INTERVAL_MS', rotateCfg.checkIntervalMs, 5000);

    const destinationRaw = String(process.env.LOG_ROTATE_DESTINATION || rotateCfg.destination || 'local').toLowerCase();
    const destination: ArchiveDestination = destinationRaw === 's3' ? 's3' : 'local';

    const localCfg = rotateCfg.local || {};
    const local: RotateLocalConfig = {
      maxFiles: this.getEnvNumber('LOG_ROTATE_LOCAL_MAX_FILES', localCfg.maxFiles, 20),
      retentionDays: this.getEnvNumber('LOG_ROTATE_LOCAL_RETENTION_DAYS', localCfg.retentionDays, 0),
    };

    const s3Cfg = rotateCfg.s3 || {};
    const s3: RotateS3Config = {
      bucket: String(process.env.LOG_ROTATE_S3_BUCKET || s3Cfg.bucket || ''),
      region: String(process.env.LOG_ROTATE_S3_REGION || s3Cfg.region || 'ap-southeast-1'),
      prefix: String(process.env.LOG_ROTATE_S3_PREFIX || s3Cfg.prefix || 'logs/archived'),
      endpoint: String(process.env.LOG_ROTATE_S3_ENDPOINT || s3Cfg.endpoint || ''),
      forcePathStyle: this.getEnvBoolean('LOG_ROTATE_S3_FORCE_PATH_STYLE', s3Cfg.forcePathStyle, false),
      accessKeyId: String(process.env.LOG_ROTATE_S3_ACCESS_KEY_ID || s3Cfg.accessKeyId || ''),
      secretAccessKey: String(process.env.LOG_ROTATE_S3_SECRET_ACCESS_KEY || s3Cfg.secretAccessKey || ''),
      sessionToken: String(process.env.LOG_ROTATE_S3_SESSION_TOKEN || s3Cfg.sessionToken || ''),
      retentionDays: this.getEnvNumber('LOG_ROTATE_S3_RETENTION_DAYS', s3Cfg.retentionDays, 30),
    };

    return {
      enabled,
      maxFileSizeBytes: Math.max(1, maxFileSizeMb) * 1024 * 1024,
      checkIntervalMs: Math.max(1000, checkIntervalMs),
      destination,
      local,
      s3,
    };
  }

  private getEnvNumber(envKey: string, cfgValue: any, fallback: number): number {
    const raw = process.env[envKey] ?? cfgValue;
    const n = Number(raw);
    if (!Number.isFinite(n)) return fallback;
    return n;
  }

  private getEnvBoolean(envKey: string, cfgValue: any, fallback: boolean): boolean {
    const raw = process.env[envKey];
    if (typeof raw === 'string') {
      const v = raw.trim().toLowerCase();
      return v === '1' || v === 'true' || v === 'yes' || v === 'on';
    }
    if (typeof cfgValue === 'boolean') return cfgValue;
    if (typeof cfgValue === 'string') {
      const v = cfgValue.trim().toLowerCase();
      return v === '1' || v === 'true' || v === 'yes' || v === 'on';
    }
    return fallback;
  }

  private formatLine(level: Level, message: any, context?: string): string {
    const ts = new Date().toISOString();
    const ctx = context ? `[${context}]` : '[App]';
    const base = `${ts} [${level.toUpperCase()}] ${ctx} ${this.stringify(message)}`;
    if (this.isWorker && this.workerLabel) return `${this.workerLabel}  | ${base}`;
    return base;
  }

  private stringify(value: any): string {
    if (typeof value === 'string') return value;
    if (value instanceof Error) return value.stack || value.message;
    try {
      return JSON.stringify(value);
    } catch (e) {
      return String(value);
    }
  }

  private resolveWorkerLabel(): string {
    const raw = process.env.WORKER_INDEX || process.env.NODE_APP_INSTANCE || process.env.HOSTNAME || os.hostname() || '';
    const m = String(raw).match(/worker-(\d+)$/);
    if (m) return `worker-${m[1]}`;
    const safe = String(raw).trim().replace(/[^a-zA-Z0-9]/g, '');
    if (safe) {
      const short = safe.slice(-2).toLowerCase();
      return `worker-${short}`;
    }
    return 'worker-unknown';
  }

  private formatArchiveTimestamp(d: Date): string {
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}`;
  }

  private extractContext(optionalParams: any[]): string | undefined {
    if (!optionalParams.length) return undefined;
    const last = optionalParams[optionalParams.length - 1];
    return typeof last === 'string' ? last : undefined;
  }

  private extractTrace(optionalParams: any[], context?: string): string | undefined {
    if (!optionalParams.length) return undefined;
    const first = optionalParams[0];
    if (typeof first !== 'string') return undefined;
    if (optionalParams.length === 1 && context === first) return undefined;
    return first;
  }
}
