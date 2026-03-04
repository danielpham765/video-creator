import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bull';
import { AppConfigModule } from '../config/config.module';
import { DbModule } from '../db/db.module';
import { JobHistoryService } from './job-history.service';
import { JobArchiveRepository } from './job-archive.repository';
import { JobArchiveService } from './job-archive.service';

@Module({
  imports: [
    AppConfigModule,
    DbModule,
    BullModule.registerQueue({ name: 'downloads' }, { name: 'download-parts' }),
  ],
  providers: [JobHistoryService, JobArchiveRepository, JobArchiveService],
  exports: [JobHistoryService, JobArchiveService],
})
export class JobsModule {}
