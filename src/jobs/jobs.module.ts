import { Module } from '@nestjs/common';
import { JobHistoryService } from './job-history.service';

@Module({
  providers: [JobHistoryService],
  exports: [JobHistoryService],
})
export class JobsModule {}
