import { Inject, Injectable } from '@nestjs/common';
import axios from 'axios';
import { BILIBILI_PLAYURL_PROVIDER } from '../source.tokens';
import { ResolveSourceInput, ResolvedSource, SourcePlayurlProvider, SourceResolver } from '../source.types';
import { RuntimeConfigService } from '../../config/runtime-config.service';
import { buildVideoQualityPolicy } from '../video-quality';

@Injectable()
export class BilibiliResolver implements SourceResolver {
  readonly platform = 'bilibili' as const;

  constructor(
    @Inject(BILIBILI_PLAYURL_PROVIDER)
    private readonly playurlProvider: SourcePlayurlProvider,
    private readonly runtimeConfig: RuntimeConfigService,
  ) {}

  canResolve(url: string): boolean {
    return /(?:^|\.)bilibili\.com$/i.test(this.host(url)) || /BV[0-9A-Za-z]+/.test(url);
  }

  async resolve(input: ResolveSourceInput): Promise<ResolvedSource> {
    const url = String(input.url || '').trim();
    const vid = this.extractVid(url);
    if (!vid) {
      throw new Error('unsupported bilibili url: missing BV video id');
    }

    const page = Number.isFinite(Number(input.page)) && Number(input.page) >= 1 ? Math.floor(Number(input.page)) : 1;
    const cookies = input.cookies;
    const resolved = await this.playurlProvider.resolvePlayurl({ vid, page, cookies });
    const play = resolved.play;
    const dash = this.selectDashTracks(play?.data?.dash);
    const durl = String(play?.data?.durl?.[0]?.url || '').trim();
    const headers: Record<string, string> = { Referer: `https://www.bilibili.com/video/${vid}` };
    if (cookies) headers.Cookie = cookies;

    return {
      platform: 'bilibili',
      vid,
      canonicalUrl: `https://www.bilibili.com/video/${vid}?p=${page}`,
      title: input.title || (await this.fetchTitle(vid, cookies)) || vid,
      headers,
      qualityMeta: { qn: Number(play?.data?.quality || 0) || 0 },
      streams: {
        dashPair: dash.videoUrl && dash.audioUrl ? { videoUrl: dash.videoUrl, audioUrl: dash.audioUrl } : undefined,
        videoOnly: dash.videoUrl ? { url: dash.videoUrl } : undefined,
        audioOnly: dash.audioUrl ? { url: dash.audioUrl } : undefined,
        muxedBoth: durl ? { url: durl } : undefined,
      },
    };
  }

  private extractVid(raw: string): string | null {
    const m1 = raw.match(/BV[0-9A-Za-z]+/);
    if (m1) return m1[0];
    const m2 = raw.match(/[?&]bvid=(BV[0-9A-Za-z]+)/);
    if (m2) return m2[1];
    return null;
  }

  private selectDashTracks(dash: any): { videoUrl: string; audioUrl: string } {
    const policy = buildVideoQualityPolicy(
      this.runtimeConfig.getForSource('bilibili', 'download.preferVideoQuality'),
    );
    const videos = Array.isArray(dash?.video) ? dash.video : [];
    const audios = Array.isArray(dash?.audio) ? dash.audio : [];

    const sortedVideos = [...videos].sort((a: any, b: any) => Number(b?.id || 0) - Number(a?.id || 0));
    const selectedVideo = sortedVideos.find((v: any) => {
      const qn = Number(v?.id || 0);
      return qn <= policy.preferredQn && qn >= policy.minAcceptableQn;
    });
    if (!selectedVideo) {
      const offered = sortedVideos.map((v: any) => Number(v?.id || 0)).filter((n: number) => n > 0).join(',');
      throw new Error(
        `no DASH video track matches prefer=${policy.preferredLabel} (required qn ${policy.minAcceptableQn}-${policy.preferredQn}, offered=[${offered}])`,
      );
    }
    const selectedAudio = audios.sort((a: any, b: any) => Number(b?.id || 0) - Number(a?.id || 0))[0];

    const videoUrl = String(selectedVideo?.baseUrl || selectedVideo?.base_url || '').trim();
    const audioUrl = String(selectedAudio?.baseUrl || selectedAudio?.base_url || '').trim();
    return { videoUrl, audioUrl };
  }

  private async fetchTitle(vid: string, cookies?: string): Promise<string | null> {
    try {
      const headers: Record<string, string> = {};
      if (cookies) headers.Cookie = cookies;
      const resp = await axios.get('https://api.bilibili.com/x/web-interface/view', {
        params: { bvid: vid },
        headers,
        timeout: 5000,
      });
      const title = String(resp?.data?.data?.title || '').trim();
      return title || null;
    } catch {
      return null;
    }
  }

  private host(url: string): string {
    try {
      return new URL(url).hostname.toLowerCase();
    } catch {
      return '';
    }
  }
}
