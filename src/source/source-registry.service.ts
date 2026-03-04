import { Inject, Injectable } from '@nestjs/common';
import { SOURCE_RESOLVERS } from './source.tokens';
import { ConcretePlatform, Platform, ResolveSourceInput, ResolvedInputIdentity, ResolvedSource, SourceResolver } from './source.types';

@Injectable()
export class SourceRegistryService {
  constructor(@Inject(SOURCE_RESOLVERS) private readonly resolvers: SourceResolver[]) {}

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
    const resolver = this.getResolverByPlatform(platform);
    return resolver.resolve({ ...input, platform });
  }

  private selectPlatform(url: string, forced: Platform): ConcretePlatform {
    if (forced && forced !== 'auto') return forced;
    const matched = this.resolvers.find((resolver) => resolver.platform !== 'generic' && resolver.canResolve(url));
    return matched?.platform || 'generic';
  }

  private getResolverByPlatform(platform: ConcretePlatform): SourceResolver {
    const resolver = this.resolvers.find((item) => item.platform === platform);
    if (resolver) return resolver;
    throw new Error(`resolver not configured for platform=${platform}`);
  }

  private genericId(url: string): string {
    const raw = String(url || '').trim();
    return Buffer.from(raw).toString('base64url').slice(0, 16) || 'generic';
  }
}
