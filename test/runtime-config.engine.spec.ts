import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { RuntimeConfigEngine } from '../src/config/runtime-config.engine';

describe('RuntimeConfigEngine', () => {
  let tempRoot: string;

  const write = (filePath: string, content: string) => {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, content, 'utf8');
  };

  beforeEach(() => {
    tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'runtime-config-'));
    write(path.join(tempRoot, 'config.default.yaml'), `download:\n  partSizeBytes: 8\n  timeoutMs: 30000\n  retryCount: 3\n  retryBackoffMs: 5000\n  preferVideoQuality: 1080p\nplayurl:\n  timeoutMs: 5000\nlogging:\n  rotate:\n    maxFileSizeMb: 5\n    checkIntervalMs: 5000\n`);
    write(path.join(tempRoot, 'config.youtube.yaml'), `download:\n  partSizeBytes: 32\n`);
    write(path.join(tempRoot, 'cookies', 'youtube.json'), `[{"name":"SID","value":"abc"}]`);
  });

  afterEach(() => {
    try {
      fs.rmSync(tempRoot, { recursive: true, force: true });
    } catch {
      // ignore
    }
    jest.useRealTimers();
  });

  it('loads and deep-merges source overrides + cookies', async () => {
    const engine = new RuntimeConfigEngine(tempRoot, 20);
    const ok = await engine.reloadNow('test');
    expect(ok).toBe(true);
    expect(engine.getSnapshotVersion()).toBe(1);
    expect(engine.getForSource('youtube', 'download.partSizeBytes')).toBe(32);
    expect(engine.getForSource('bilibili', 'download.partSizeBytes')).toBe(8);
    expect(engine.getCookies('youtube')).toContain('SID=abc');
  });

  it('keeps old snapshot when parsing candidate fails', async () => {
    const engine = new RuntimeConfigEngine(tempRoot, 20);
    expect(await engine.reloadNow('initial')).toBe(true);
    const before = engine.getSnapshotVersion();

    write(path.join(tempRoot, 'config.youtube.yaml'), `download:\n  partSizeBytes: [bad`);
    expect(await engine.reloadNow('broken')).toBe(false);

    expect(engine.getSnapshotVersion()).toBe(before);
    expect(engine.getForSource('youtube', 'download.partSizeBytes')).toBe(32);
    expect(engine.getLastReloadStatus().error).toContain('runtime config reload failed');
  });

  it('tracks pending-restart keys when non-hot paths change', async () => {
    const engine = new RuntimeConfigEngine(tempRoot, 20);
    expect(await engine.reloadNow('initial')).toBe(true);

    write(
      path.join(tempRoot, 'config.default.yaml'),
      `download:\n  partSizeBytes: 8\n  timeoutMs: 30000\n  retryCount: 3\n  retryBackoffMs: 5000\n  preferVideoQuality: 1080p\nplayurl:\n  timeoutMs: 5000\nlogging:\n  rotate:\n    maxFileSizeMb: 5\n    checkIntervalMs: 5000\nredis:\n  url: redis://127.0.0.1:6380\n`,
    );
    expect(await engine.reloadNow('redis-change')).toBe(true);

    expect(engine.getLastReloadStatus().pendingRestartKeys).toContain('redis');
  });

  it('debounces watch-triggered reloads and coalesces bursts', () => {
    jest.useFakeTimers();
    const engine = new RuntimeConfigEngine(tempRoot, 1000);
    const spy = jest.spyOn(engine, 'reloadNow').mockResolvedValue(true as any);

    (engine as any).scheduleReload();
    (engine as any).scheduleReload();
    (engine as any).scheduleReload();

    jest.advanceTimersByTime(999);
    expect(spy).toHaveBeenCalledTimes(0);

    jest.advanceTimersByTime(1);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});
