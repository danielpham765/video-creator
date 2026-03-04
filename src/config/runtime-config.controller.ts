import { Controller, Get } from '@nestjs/common';
import { RuntimeConfigService } from './runtime-config.service';

@Controller('config')
export class RuntimeConfigController {
  constructor(private readonly runtimeConfig: RuntimeConfigService) {}

  @Get('runtime')
  getRuntimeConfigStatus() {
    const status = this.runtimeConfig.getLastReloadStatus();
    return {
      version: status.version,
      loadedAt: status.loadedAt,
      watchedFiles: status.watchedFiles,
      pendingRestartKeys: status.pendingRestartKeys,
      error: status.error || null,
    };
  }
}
