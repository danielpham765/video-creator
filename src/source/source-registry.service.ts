import { Injectable } from '@nestjs/common';
import { BilibiliResolver } from './resolvers/bilibili.resolver';
import { GenericResolver } from './resolvers/generic.resolver';
import { YouTubeResolver } from './resolvers/youtube.resolver';
import { ConcretePlatform, Platform, ResolveSourceInput, ResolvedInputIdentity, ResolvedSource } from './source.types';

@Injectable()
export class SourceRegistryService {
  constructor(
    private readonly bilibiliResolver: BilibiliResolver,
    private readonly youtubeResolver: YouTubeResolver,
    private readonly genericResolver: GenericResolver,
  ) {}

  identifyInput(url: string, forcedPlatform: Platform = 'auto'): ResolvedInputIdentity {
    const platform = this.selectPlatform(url, forcedPlatform);
    if (platform === 'bilibili') {
      const m = String(url || '').match(/BV[0-9A-Za-z]+/);
      return { platform, vid: m?.[0] || 'unknown-bilibili-vid' };
    }
    if (platform === 'youtube') {
      try {
        const u = new URL(String(url || ''));
        const v = u.searchParams.get('v') || u.pathname.split('/').filter(Boolean).pop() || 'unknown-youtube-vid';
        return { platform, vid: v };
      } catch {
        return { platform, vid: 'unknown-youtube-vid' };
      }
    }
    return { platform, vid: this.genericId(url) };
  }

  async resolve(input: ResolveSourceInput): Promise<ResolvedSource> {
    const platform = this.selectPlatform(input.url, input.platform || 'auto');
    if (platform === 'bilibili') return this.bilibiliResolver.resolve({ ...input, platform: 'bilibili' });
    if (platform === 'youtube') return this.youtubeResolver.resolve({ ...input, platform: 'youtube' });
    return this.genericResolver.resolve({ ...input, platform: 'generic' });
  }

  private selectPlatform(url: string, forced: Platform): ConcretePlatform {
    if (forced && forced !== 'auto') return forced;
    if (this.bilibiliResolver.canResolve(url)) return 'bilibili';
    if (this.youtubeResolver.canResolve(url)) return 'youtube';
    return 'generic';
  }

  private genericId(url: string): string {
    const raw = String(url || '').trim();
    return Buffer.from(raw).toString('base64url').slice(0, 16) || 'generic';
  }
}
