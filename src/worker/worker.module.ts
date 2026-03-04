import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bull';
import { WorkerProcessor } from './worker.processor';
import { PartsProcessor } from './parts.processor';
import { PlayurlModule } from '../playurl/playurl.module';
import { FfmpegModule } from '../ffmpeg/ffmpeg.module';
import { JobsModule } from '../jobs/jobs.module';
import { JobArchiveService } from '../jobs/job-archive.service';
import { AppConfigModule } from '../config/config.module';
import { ConfigService } from '@nestjs/config';
import { SourceModule } from '../source/source.module';

@Module({
  imports: [
    AppConfigModule,
    BullModule.forRootAsync({
      imports: [AppConfigModule],
      useFactory: (config: ConfigService) => {
        const redisUrl = config.get<string>('redis.url') || process.env.REDIS_URL || 'redis://redis:6379';
        return { redis: redisUrl } as any;
      },
      inject: [ConfigService],
    }),
    PlayurlModule,
    SourceModule,
    FfmpegModule,
    BullModule.registerQueue({ name: 'downloads' }, { name: 'download-parts' }),
    JobsModule,
  ],
  providers: [WorkerProcessor, PartsProcessor, JobArchiveService],
  exports: [],
})
export class WorkerModule {}
