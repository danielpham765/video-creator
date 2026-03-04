import { Module } from '@nestjs/common';
import { AppConfigModule } from '../config/config.module';
import { MediaPlannerService } from './media-planner.service';
import { BilibiliResolver } from './resolvers/bilibili.resolver';
import { GenericResolver } from './resolvers/generic.resolver';
import { YouTubeResolver } from './resolvers/youtube.resolver';
import { SourceRegistryService } from './source-registry.service';
import { BilibiliPlayurlProvider } from './providers/bilibili-playurl.provider';
import { BILIBILI_PLAYURL_PROVIDER, SOURCE_RESOLVERS } from './source.tokens';
import { SourceResolver } from './source.types';

@Module({
  imports: [AppConfigModule],
  providers: [
    MediaPlannerService,
    SourceRegistryService,
    BilibiliPlayurlProvider,
    {
      provide: BILIBILI_PLAYURL_PROVIDER,
      useExisting: BilibiliPlayurlProvider,
    },
    BilibiliResolver,
    YouTubeResolver,
    GenericResolver,
    {
      provide: SOURCE_RESOLVERS,
      useFactory: (
        bilibiliResolver: BilibiliResolver,
        youtubeResolver: YouTubeResolver,
        genericResolver: GenericResolver,
      ): SourceResolver[] => [bilibiliResolver, youtubeResolver, genericResolver],
      inject: [BilibiliResolver, YouTubeResolver, GenericResolver],
    },
  ],
  exports: [MediaPlannerService, SourceRegistryService],
})
export class SourceModule {}
