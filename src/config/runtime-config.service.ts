import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConcretePlatform } from '../source/source.types';
import { runtimeConfigEngine } from './runtime-config.engine';
import { RuntimeConfigListener, RuntimeReloadStatus } from './runtime-config.types';

@Injectable()
export class RuntimeConfigService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RuntimeConfigService.name);
  private unsubscribeReload: (() => void) | null = null;

  constructor() {
    runtimeConfigEngine.start();
  }

  onModuleInit(): void {
    const status = runtimeConfigEngine.getLastReloadStatus();
    if (status.error) {
      this.logger.warn(status.error);
    } else {
      this.logger.log(`Runtime config started (version=${status.version}, loadedAt=${status.loadedAt})`);
    }
    this.unsubscribeReload = runtimeConfigEngine.subscribe(({ version, loadedAt, changes }) => {
      const latest = runtimeConfigEngine.getLastReloadStatus();
      const pending = latest.pendingRestartKeys.length
        ? ` pendingRestartKeys=${latest.pendingRestartKeys.join(',')}`
        : '';
      this.logger.log(`Runtime config reloaded (version=${version}, loadedAt=${loadedAt})${pending}`);
      if (changes.length === 0) {
        this.logger.debug('Runtime config changes: no effective value changes');
      } else {
        const preview = changes.slice(0, 80);
        for (const line of preview) {
          this.logger.debug(`Runtime config change: ${line}`);
        }
        if (changes.length > preview.length) {
          this.logger.debug(`Runtime config change: ... and ${changes.length - preview.length} more`);
        }
      }
    });
  }

  onModuleDestroy(): void {
    if (this.unsubscribeReload) {
      this.unsubscribeReload();
      this.unsubscribeReload = null;
    }
    runtimeConfigEngine.stop();
  }

  getGlobal(pathExpr?: string): any {
    return runtimeConfigEngine.getGlobal(pathExpr);
  }

  getForSource(source: ConcretePlatform, pathExpr?: string): any {
    return runtimeConfigEngine.getForSource(source, pathExpr);
  }

  getCookies(source: ConcretePlatform): string {
    return runtimeConfigEngine.getCookies(source);
  }

  getSnapshotVersion(): number {
    return runtimeConfigEngine.getSnapshotVersion();
  }

  subscribe(listener: RuntimeConfigListener): () => void {
    return runtimeConfigEngine.subscribe(listener);
  }

  getLastReloadStatus(): RuntimeReloadStatus {
    return runtimeConfigEngine.getLastReloadStatus();
  }
}
