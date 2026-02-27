import { Module } from '@nestjs/common';
import { DownloadController } from './download.controller';
import { DownloadService } from './download.service';
import { BullModule } from '@nestjs/bull';
import { JobsModule } from '../jobs/jobs.module';
import { PlayurlModule } from '../playurl/playurl.module';
import { FfmpegModule } from '../ffmpeg/ffmpeg.module';

@Module({
  imports: [
    BullModule.registerQueue(
      { name: 'downloads' },
      { name: 'download-parts' },
    ),
    JobsModule,
    PlayurlModule,
    FfmpegModule,
  ],
  controllers: [DownloadController],
  providers: [DownloadService],
  exports: [JobsModule],
})
export class DownloadModule {}
