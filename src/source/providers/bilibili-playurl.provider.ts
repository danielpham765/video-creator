import { Injectable } from '@nestjs/common';
import axios from 'axios';
import { RuntimeConfigService } from '../../config/runtime-config.service';
import { PlayurlResolveInput, PlayurlResolveResult, SourcePlayurlProvider } from '../source.types';
import { buildVideoQualityPolicy } from '../video-quality';

@Injectable()
export class BilibiliPlayurlProvider implements SourcePlayurlProvider {
  readonly platform = 'bilibili' as const;

  constructor(private readonly runtimeConfig: RuntimeConfigService) {}

  async resolvePlayurl(input: PlayurlResolveInput): Promise<PlayurlResolveResult> {
    const vid = String(input.vid || '').trim();
    if (!vid) throw new Error('missing bilibili vid');

    const page = Number.isFinite(Number(input.page)) && Number(input.page) >= 1 ? Math.floor(Number(input.page)) : 1;
    const cookies = String(input.cookies || '').trim();
    const cid = await this.getCidFromBvid(vid, page);
    if (!cid) throw new Error('cannot resolve bilibili cid');
    const play = await this.getPlayurl(vid, cid, cookies);
    return { cid, play };
  }

  private async getCidFromBvid(bvid: string, page = 1): Promise<string | number | null> {
    const base = String(this.runtimeConfig.getForSource('bilibili', 'playurl.baseUrl') || 'https://api.bilibili.com');
    const url = `${base.replace(/\/$/, '')}/x/player/pagelist?bvid=${bvid}`;
    const timeout = Number(this.runtimeConfig.getForSource('bilibili', 'playurl.timeoutMs') ?? 5000);
    const r = await axios.get(url, { timeout });
    const list = Array.isArray(r.data?.data) ? r.data.data : [];
    if (!list.length) return null;
    const safePage = Number.isFinite(page) && page >= 1 ? Math.floor(page) : 1;
    const byPage = list.find((item: any) => Number(item?.page) === safePage);
    if (byPage?.cid) return byPage.cid;
    return list[0]?.cid ?? null;
  }

  private async getPlayurl(bvid: string, cid: string | number, cookies?: string): Promise<any> {
    const base = String(this.runtimeConfig.getForSource('bilibili', 'playurl.baseUrl') || 'https://api.bilibili.com');
    const qualityPolicy = buildVideoQualityPolicy(
      this.runtimeConfig.getForSource('bilibili', 'download.preferVideoQuality'),
    );
    const url = `${base.replace(/\/$/, '')}/x/player/playurl?bvid=${bvid}&cid=${cid}&qn=${qualityPolicy.preferredQn}&fnval=16`;
    const headers: Record<string, string> = { Referer: `https://www.bilibili.com/video/${bvid}` };
    if (cookies) headers.Cookie = cookies;
    const timeout = Number(this.runtimeConfig.getForSource('bilibili', 'playurl.timeoutMs') ?? 5000);
    const r = await axios.get(url, { headers, timeout });
    return r.data;
  }
}
