import { Module } from '@nestjs/common';
import { PlayurlService } from './playurl.service';
import { AppConfigModule } from '../config/config.module';

@Module({
  imports: [AppConfigModule],
  providers: [PlayurlService],
  exports: [PlayurlService],
})
export class PlayurlModule {}
