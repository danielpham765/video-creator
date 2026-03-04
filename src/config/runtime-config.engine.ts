import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';
import { ConcretePlatform } from '../source/source.types';
import { RuntimeConfigListener, RuntimeConfigSnapshot, RuntimeReloadStatus } from './runtime-config.types';

const KNOWN_SOURCES: ConcretePlatform[] = ['bilibili', 'youtube', 'generic'];
const DEFAULT_DEBOUNCE_MS = 10_000;
const PENDING_RESTART_PATHS = [
  'redis',
  'worker.queuePrefix',
];

export class RuntimeConfigEngine {
  private readonly configDir: string;
  private readonly cookiesDir: string;
  private readonly debounceMs: number;
  private readonly listeners = new Set<RuntimeConfigListener>();
  private readonly pendingRestartKeys = new Set<string>();

  private watchers: fs.FSWatcher[] = [];
  private started = false;
  private debounceTimer: NodeJS.Timeout | null = null;
  private reloading = false;
  private queuedReload = false;

  private snapshot: RuntimeConfigSnapshot;
  private status: RuntimeReloadStatus;

  constructor(configRoot = path.join(process.cwd(), 'config'), debounceMs = DEFAULT_DEBOUNCE_MS) {
    this.configDir = path.resolve(configRoot);
    this.cookiesDir = path.join(this.configDir, 'cookies');
    this.debounceMs = Math.max(0, Number(debounceMs) || DEFAULT_DEBOUNCE_MS);

    const now = new Date().toISOString();
    this.snapshot = {
      globalDefault: {},
      sourceOverrides: {},
      effectiveBySource: {
        bilibili: {},
        youtube: {},
        generic: {},
      },
      cookiesBySource: {},
      version: 0,
      loadedAt: now,
      sourceFileMeta: {},
    };
    this.status = {
      version: 0,
      loadedAt: now,
      watchedFiles: [],
      pendingRestartKeys: [],
    };
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.ensureDirs();
    void this.reloadNow('startup');
    this.watchers.push(this.watchDir(this.configDir));
    this.watchers.push(this.watchDir(this.cookiesDir));
  }

  stop(): void {
    this.started = false;
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    for (const watcher of this.watchers) {
      try {
        watcher.close();
      } catch {
        // ignore close errors
      }
    }
    this.watchers = [];
  }

  subscribe(listener: RuntimeConfigListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  getGlobal(pathExpr?: string): any {
    if (!pathExpr) return this.snapshot.globalDefault;
    return this.deepGet(this.snapshot.globalDefault, pathExpr);
  }

  getForSource(source: ConcretePlatform, pathExpr?: string): any {
    const cfg = this.snapshot.effectiveBySource[source] || this.snapshot.globalDefault;
    if (!pathExpr) return cfg;
    return this.deepGet(cfg, pathExpr);
  }

  getCookies(source: ConcretePlatform): string {
    return this.snapshot.cookiesBySource[source] || '';
  }

  getSnapshotVersion(): number {
    return this.snapshot.version;
  }

  getLastReloadStatus(): RuntimeReloadStatus {
    return {
      ...this.status,
      pendingRestartKeys: [...this.pendingRestartKeys].sort(),
      watchedFiles: [...this.status.watchedFiles],
    };
  }

  getSnapshot(): RuntimeConfigSnapshot {
    return this.snapshot;
  }

  async reloadNow(reason = 'manual'): Promise<boolean> {
    if (this.reloading) {
      this.queuedReload = true;
      return false;
    }
    this.reloading = true;
    try {
      const candidate = this.buildCandidateSnapshot();
      this.validateSnapshot(candidate);
      const changes = this.buildChangeList(this.snapshot, candidate);
      this.detectPendingRestartChanges(this.snapshot, candidate);
      this.snapshot = candidate;
      this.status = {
        version: candidate.version,
        loadedAt: candidate.loadedAt,
        watchedFiles: Object.keys(candidate.sourceFileMeta).sort(),
        pendingRestartKeys: [...this.pendingRestartKeys].sort(),
      };
      this.emitUpdate(candidate, changes);
      return true;
    } catch (err: any) {
      const message = `runtime config reload failed (${reason}): ${String(err?.message || err)}`;
      // eslint-disable-next-line no-console
      console.warn(`[runtime-config] ${message}`);
      this.status = {
        version: this.snapshot.version,
        loadedAt: this.snapshot.loadedAt,
        error: message,
        watchedFiles: Object.keys(this.snapshot.sourceFileMeta).sort(),
        pendingRestartKeys: [...this.pendingRestartKeys].sort(),
      };
      return false;
    } finally {
      this.reloading = false;
      if (this.queuedReload) {
        this.queuedReload = false;
        void this.reloadNow('queued');
      }
    }
  }

  private emitUpdate(snapshot: RuntimeConfigSnapshot, changes: string[]): void {
    for (const listener of this.listeners) {
      try {
        listener({ version: snapshot.version, loadedAt: snapshot.loadedAt, changes });
      } catch {
        // ignore listener failures
      }
    }
  }

  private buildChangeList(current: RuntimeConfigSnapshot, next: RuntimeConfigSnapshot): string[] {
    const out: string[] = [];
    this.diffObjects('global', current.globalDefault || {}, next.globalDefault || {}, out, 300);
    this.diffObjects('sourceOverrides', current.sourceOverrides || {}, next.sourceOverrides || {}, out, 300);

    const cookieSources = new Set<string>([
      ...Object.keys(current.cookiesBySource || {}),
      ...Object.keys(next.cookiesBySource || {}),
    ]);
    for (const source of cookieSources) {
      const before = String(current.cookiesBySource?.[source] || '');
      const after = String(next.cookiesBySource?.[source] || '');
      if (before !== after) {
        out.push(`cookies.${source}: [len ${before.length}] -> [len ${after.length}]`);
      }
    }
    return out;
  }

  private diffObjects(
    prefix: string,
    beforeObj: Record<string, any>,
    afterObj: Record<string, any>,
    out: string[],
    maxEntries: number,
  ): void {
    if (out.length >= maxEntries) return;
    const keys = new Set<string>([
      ...Object.keys(beforeObj || {}),
      ...Object.keys(afterObj || {}),
    ]);
    for (const key of keys) {
      if (out.length >= maxEntries) break;
      const before = beforeObj?.[key];
      const after = afterObj?.[key];
      const path = `${prefix}.${key}`;
      const beforeIsObj = this.isPlainObject(before);
      const afterIsObj = this.isPlainObject(after);
      if (beforeIsObj && afterIsObj) {
        this.diffObjects(path, before as Record<string, any>, after as Record<string, any>, out, maxEntries);
        continue;
      }
      if (JSON.stringify(before) === JSON.stringify(after)) continue;
      out.push(`${path}: ${this.shortValue(before)} -> ${this.shortValue(after)}`);
    }
  }

  private shortValue(v: any): string {
    if (v === undefined) return 'undefined';
    if (v === null) return 'null';
    if (typeof v === 'string') {
      const compact = v.length > 80 ? `${v.slice(0, 77)}...` : v;
      return JSON.stringify(compact);
    }
    try {
      const s = JSON.stringify(v);
      if (!s) return String(v);
      return s.length > 120 ? `${s.slice(0, 117)}...` : s;
    } catch {
      return String(v);
    }
  }

  private ensureDirs(): void {
    fs.mkdirSync(this.configDir, { recursive: true });
    fs.mkdirSync(this.cookiesDir, { recursive: true });
  }

  private watchDir(dir: string): fs.FSWatcher {
    return fs.watch(dir, (_eventType, filename) => {
      const name = String(filename || '').trim();
      if (name && this.shouldIgnoreEventFile(name)) return;
      this.scheduleReload();
    });
  }

  private shouldIgnoreEventFile(name: string): boolean {
    const base = path.basename(name);
    if (!base) return true;
    if (base.startsWith('.')) return true;
    if (base.endsWith('~')) return true;
    if (/\.swp$|\.tmp$|\.temp$|\.bak$/i.test(base)) return true;
    if (base.includes('.#')) return true;
    return false;
  }

  private scheduleReload(): void {
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = null;
      void this.reloadNow('watch');
    }, this.debounceMs);
  }

  private buildCandidateSnapshot(): RuntimeConfigSnapshot {
    const sourceFileMeta: Record<string, { mtimeMs: number; size: number }> = {};

    const defaultPath = path.join(this.configDir, 'config.default.yaml');
    if (!fs.existsSync(defaultPath)) {
      throw new Error(`missing required config file: ${defaultPath}`);
    }
    const globalDefault = this.readYamlFile(defaultPath);
    sourceFileMeta[defaultPath] = this.statFile(defaultPath);

    const sourceOverrides: Record<string, Record<string, any>> = {};
    const configFiles = fs.readdirSync(this.configDir)
      .filter((name) => /^config\.[a-z0-9_-]+\.ya?ml$/i.test(name) && !/^config\.default\.ya?ml$/i.test(name))
      .filter((name) => !/\.example\.ya?ml$/i.test(name));

    for (const fileName of configFiles) {
      const source = fileName.replace(/^config\./i, '').replace(/\.ya?ml$/i, '').toLowerCase();
      const fullPath = path.join(this.configDir, fileName);
      sourceOverrides[source] = this.readYamlFile(fullPath);
      sourceFileMeta[fullPath] = this.statFile(fullPath);
    }

    const cookiesBySource: Record<string, string> = {};
    if (fs.existsSync(this.cookiesDir)) {
      const cookieFiles = fs.readdirSync(this.cookiesDir)
        .filter((name) => /\.json$/i.test(name))
        .filter((name) => !/\.example\.json$/i.test(name));
      for (const fileName of cookieFiles) {
        const source = fileName.replace(/\.json$/i, '').toLowerCase();
        const fullPath = path.join(this.cookiesDir, fileName);
        cookiesBySource[source] = this.readCookieFile(fullPath);
        sourceFileMeta[fullPath] = this.statFile(fullPath);
      }
    }

    const effectiveBySource: Record<string, Record<string, any>> = {};
    const sources = new Set<string>([...KNOWN_SOURCES, ...Object.keys(sourceOverrides), ...Object.keys(cookiesBySource)]);
    for (const source of sources) {
      effectiveBySource[source] = this.deepMerge(globalDefault, sourceOverrides[source] || {});
    }

    return {
      globalDefault,
      sourceOverrides,
      effectiveBySource,
      cookiesBySource,
      version: this.snapshot.version + 1,
      loadedAt: new Date().toISOString(),
      sourceFileMeta,
    };
  }

  private validateSnapshot(snapshot: RuntimeConfigSnapshot): void {
    this.assertPositiveInt(snapshot.globalDefault, 'download.partSizeBytes');
    this.assertPositiveInt(snapshot.globalDefault, 'download.timeoutMs');
    this.assertNonNegativeInt(snapshot.globalDefault, 'download.retryCount');
    this.assertNonNegativeInt(snapshot.globalDefault, 'download.retryBackoffMs');
    this.assertNonNegativeInt(snapshot.globalDefault, 'download.minVideoQn');
    this.assertPositiveInt(snapshot.globalDefault, 'playurl.timeoutMs');
    this.assertPositiveInt(snapshot.globalDefault, 'logging.rotate.maxFileSizeMb');
    this.assertPositiveInt(snapshot.globalDefault, 'logging.rotate.checkIntervalMs');

    for (const [source, cfg] of Object.entries(snapshot.effectiveBySource)) {
      this.assertPositiveInt(cfg, 'download.partSizeBytes', source);
      this.assertPositiveInt(cfg, 'download.timeoutMs', source);
      this.assertNonNegativeInt(cfg, 'download.retryCount', source);
      this.assertNonNegativeInt(cfg, 'download.retryBackoffMs', source);
      this.assertNonNegativeInt(cfg, 'download.minVideoQn', source);
      this.assertPositiveInt(cfg, 'playurl.timeoutMs', source);
      this.assertPositiveInt(cfg, 'logging.rotate.maxFileSizeMb', source);
      this.assertPositiveInt(cfg, 'logging.rotate.checkIntervalMs', source);
    }
  }

  private detectPendingRestartChanges(current: RuntimeConfigSnapshot, next: RuntimeConfigSnapshot): void {
    for (const key of PENDING_RESTART_PATHS) {
      const before = this.deepGet(current.globalDefault, key);
      const after = this.deepGet(next.globalDefault, key);
      if (JSON.stringify(before) !== JSON.stringify(after)) {
        this.pendingRestartKeys.add(key);
      }
    }
  }

  private assertPositiveInt(obj: Record<string, any>, pathExpr: string, source?: string): void {
    const value = this.deepGet(obj, pathExpr);
    if (value === undefined || value === null || value === '') return;
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) {
      const hint = source ? ` for source ${source}` : '';
      throw new Error(`invalid ${pathExpr}${hint}: expected positive number, got ${String(value)}`);
    }
  }

  private assertNonNegativeInt(obj: Record<string, any>, pathExpr: string, source?: string): void {
    const value = this.deepGet(obj, pathExpr);
    if (value === undefined || value === null || value === '') return;
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) {
      const hint = source ? ` for source ${source}` : '';
      throw new Error(`invalid ${pathExpr}${hint}: expected non-negative number, got ${String(value)}`);
    }
  }

  private readYamlFile(filePath: string): Record<string, any> {
    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = yaml.load(raw);
    if (!parsed) return {};
    if (typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error(`invalid yaml object in ${filePath}`);
    }
    return parsed as Record<string, any>;
  }

  private readCookieFile(filePath: string): string {
    const raw = fs.readFileSync(filePath, 'utf8').trim();
    if (!raw) return '';

    if (raw.startsWith('[')) {
      const arr = JSON.parse(raw);
      if (!Array.isArray(arr)) throw new Error(`invalid cookie array in ${filePath}`);
      return arr
        .map((item: any) => {
          const name = String(item?.name || '').trim();
          const value = String(item?.value || '').trim();
          if (!name) return '';
          return `${name}=${value}`;
        })
        .filter(Boolean)
        .join('; ');
    }

    if (raw.startsWith('{')) {
      const obj = JSON.parse(raw);
      if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
        throw new Error(`invalid cookie object in ${filePath}`);
      }
      return Object.entries(obj)
        .map(([k, v]) => `${String(k).trim()}=${String(v ?? '').trim()}`)
        .filter((s) => !s.startsWith('='))
        .join('; ');
    }

    return raw;
  }

  private statFile(filePath: string): { mtimeMs: number; size: number } {
    const stat = fs.statSync(filePath);
    return { mtimeMs: stat.mtimeMs, size: stat.size };
  }

  private deepGet(obj: any, pathExpr: string): any {
    if (!pathExpr) return obj;
    const parts = pathExpr.split('.').map((p) => p.trim()).filter(Boolean);
    let cur = obj;
    for (const part of parts) {
      if (!cur || typeof cur !== 'object') return undefined;
      cur = cur[part];
    }
    return cur;
  }

  private deepMerge(base: Record<string, any>, override: Record<string, any>): Record<string, any> {
    const out: Record<string, any> = { ...base };
    for (const [k, v] of Object.entries(override || {})) {
      const baseV = out[k];
      if (this.isPlainObject(baseV) && this.isPlainObject(v)) {
        out[k] = this.deepMerge(baseV, v as Record<string, any>);
      } else {
        out[k] = v;
      }
    }
    return out;
  }

  private isPlainObject(value: any): boolean {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
  }
}

export const runtimeConfigEngine = new RuntimeConfigEngine();
