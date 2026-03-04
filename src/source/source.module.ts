import { Module } from '@nestjs/common';
import { PlayurlModule } from '../playurl/playurl.module';
import { AppConfigModule } from '../config/config.module';
import { MediaPlannerService } from './media-planner.service';
import { BilibiliResolver } from './resolvers/bilibili.resolver';
import { GenericResolver } from './resolvers/generic.resolver';
import { YouTubeResolver } from './resolvers/youtube.resolver';
import { SourceRegistryService } from './source-registry.service';

@Module({
  imports: [AppConfigModule, PlayurlModule],
  providers: [
    MediaPlannerService,
    SourceRegistryService,
    BilibiliResolver,
    YouTubeResolver,
    GenericResolver,
  ],
  exports: [MediaPlannerService, SourceRegistryService],
})
export class SourceModule {}
