import { Module, DynamicModule } from '@nestjs/common';
import { BullModule } from '@nestjs/bull';
import { ConfigService } from '@nestjs/config';
import { DownloadModule } from './download/download.module';
import { AppConfigModule } from './config/config.module';

// Import WorkerModule lazily to allow running the same image either as API or worker.
export class AppModule {
  static register(): DynamicModule {
    const isWorker = String(process.env.WORKER || '').toLowerCase() === 'true';
    if (isWorker) {
      // require here so it's only loaded when running in worker mode
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { WorkerModule } = require('./worker/worker.module');
      return {
        module: AppModule,
        imports: [WorkerModule],
      };
    }

    const imports: any[] = [AppConfigModule];
    // Ensure Bull (Redis) root config is registered so API can enqueue jobs
    imports.push(
      BullModule.forRootAsync({
        imports: [AppConfigModule],
        useFactory: (config: ConfigService) => {
          const redisUrl = config.get<string>('redis.url') || process.env.REDIS_URL || 'redis://redis:6379';
          return { redis: redisUrl } as any;
        },
        inject: [ConfigService],
      }),
    );
    imports.push(DownloadModule);
    return {
      module: AppModule,
      imports,
    };
  }
}
