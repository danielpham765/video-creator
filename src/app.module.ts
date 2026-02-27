import { Module, DynamicModule } from '@nestjs/common';
import { BullModule } from '@nestjs/bull';
import { ConfigService } from '@nestjs/config';
import { DownloadModule } from './download/download.module';
import { JobsModule } from './jobs/jobs.module';
import { PlayurlModule } from './playurl/playurl.module';
import { FfmpegModule } from './ffmpeg/ffmpeg.module';
import { AppConfigModule } from './config/config.module';

// Import WorkerModule lazily to allow running the same image either as API or worker.
export class AppModule {
  static register(): DynamicModule {
    const imports: any[] = [AppConfigModule, DownloadModule, JobsModule, PlayurlModule, FfmpegModule];
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
    if (String(process.env.WORKER || '').toLowerCase() === 'true') {
      // require here so it's only loaded when running in worker mode
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { WorkerModule } = require('./worker/worker.module');
      imports.push(WorkerModule);
    }
    return {
      module: AppModule,
      imports,
    };
  }
}

