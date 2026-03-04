import { Injectable } from '@nestjs/common';
import { ResolveSourceInput, ResolvedSource, SourceResolver } from '../source.types';

@Injectable()
export class YouTubeResolver implements SourceResolver {
  readonly platform = 'youtube' as const;

  canResolve(url: string): boolean {
    const host = this.host(url);
    return /(?:^|\.)youtube\.com$/i.test(host) || /(?:^|\.)youtu\.be$/i.test(host);
  }

  async resolve(input: ResolveSourceInput): Promise<ResolvedSource> {
    const rawUrl = String(input.url || '').trim();
    const vid = this.extractVideoId(rawUrl);
    if (!vid) throw new Error('unsupported youtube url: missing video id');

    const canonicalUrl = `https://www.youtube.com/watch?v=${vid}`;

    const ytdlClients = this.loadYtdlClients();
    const ytdlOptions = this.buildYtdlOptions(input);
    let info: any = null;
    const errors: string[] = [];
    for (const client of ytdlClients) {
      const name = String(client?.name || 'ytdl-client');
      try {
        info = await this.getInfoWithTimeout(client.client, canonicalUrl, ytdlOptions, 20000);
        break;
      } catch (e: any) {
        errors.push(`${name}: ${String(e?.message || e)}`);
      }
    }
    if (!info) {
      throw new Error(`youtube resolver failed to extract stream info (${errors.join(' | ')})`);
    }
    const formats: any[] = Array.isArray(info?.formats) ? info.formats : [];

    const audioOnly = formats
      .filter((f) => f?.hasAudio && !f?.hasVideo && f?.url)
      .sort((a, b) => Number(b?.audioBitrate || 0) - Number(a?.audioBitrate || 0))[0];

    const videoOnly = formats
      .filter((f) => f?.hasVideo && !f?.hasAudio && f?.url)
      .sort((a, b) => this.videoRank(b) - this.videoRank(a))[0];

    const muxedBoth = formats
      .filter((f) => f?.hasVideo && f?.hasAudio && f?.url)
      .sort((a, b) => this.videoRank(b) - this.videoRank(a))[0];

    return {
      platform: 'youtube',
      vid,
      canonicalUrl,
      title: input.title || String(info?.videoDetails?.title || '').trim() || vid,
      headers: {},
      qualityMeta: {
        videoQualityLabel: String(videoOnly?.qualityLabel || muxedBoth?.qualityLabel || '').trim() || undefined,
      },
      streams: {
        dashPair: videoOnly?.url && audioOnly?.url ? { videoUrl: videoOnly.url, audioUrl: audioOnly.url } : undefined,
        videoOnly: videoOnly?.url ? { url: videoOnly.url } : undefined,
        audioOnly: audioOnly?.url ? { url: audioOnly.url } : undefined,
        muxedBoth: muxedBoth?.url ? { url: muxedBoth.url } : undefined,
      },
    };
  }

  private async getInfoWithTimeout(ytdl: any, url: string, options: any, timeoutMs: number): Promise<any> {
    return await Promise.race([
      ytdl.getInfo(url, options),
      new Promise((_, reject) => setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs)),
    ]);
  }

  private buildYtdlOptions(input: ResolveSourceInput): any {
    const headers: Record<string, string> = {
      'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      Accept: '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
      Origin: 'https://www.youtube.com',
      Referer: 'https://www.youtube.com/',
    };
    if (input.cookies) headers.cookie = String(input.cookies);

    return {
      requestOptions: { headers },
      playerClients: ['WEB', 'ANDROID', 'IOS'],
      lang: 'en',
    };
  }

  private loadYtdlClients(): Array<{ name: string; client: any }> {
    const clients: Array<{ name: string; client: any }> = [];
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      clients.push({ name: '@distube/ytdl-core', client: require('@distube/ytdl-core') });
    } catch (e) {
      // ignore
    }
    try {
      try {
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        clients.push({ name: 'ytdl-core', client: require('ytdl-core') });
      } catch {
        // ignore
      }
    } catch {
      // ignore
    }
    if (!clients.length) {
      throw new Error('youtube resolver requires @distube/ytdl-core or ytdl-core dependency');
    }
    return clients;
  }

  private extractVideoId(url: string): string | null {
    try {
      const u = new URL(url);
      const host = u.hostname.toLowerCase();
      if (host.endsWith('youtu.be')) {
        const id = u.pathname.split('/').filter(Boolean)[0];
        return id || null;
      }
      const queryId = u.searchParams.get('v');
      if (queryId) return queryId;
      const paths = u.pathname.split('/').filter(Boolean);
      if (paths[0] === 'shorts' && paths[1]) return paths[1];
      return null;
    } catch {
      return null;
    }
  }

  private videoRank(format: any): number {
    const qualityLabel = String(format?.qualityLabel || '0').toLowerCase();
    const qualityNumber = parseInt(qualityLabel.replace(/[^0-9]/g, ''), 10) || 0;
    return qualityNumber * 1000 + (Number(format?.fps || 0) * 10) + Number(format?.bitrate || 0) / 1000;
  }

  private host(url: string): string {
    try {
      return new URL(url).hostname.toLowerCase();
    } catch {
      return '';
    }
  }
}
