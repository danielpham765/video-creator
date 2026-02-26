import { Module } from '@nestjs/common';
import { PlayurlService } from './playurl.service';

@Module({
  providers: [PlayurlService],
  exports: [PlayurlService],
})
export class PlayurlModule {}
