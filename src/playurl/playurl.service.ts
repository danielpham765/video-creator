import { Injectable } from '@nestjs/common';
import axios from 'axios';
import { RuntimeConfigService } from '../config/runtime-config.service';
import { buildVideoQualityPolicy } from '../source/video-quality';

@Injectable()
export class PlayurlService {
  constructor(private readonly runtimeConfig: RuntimeConfigService) {}

  async getCidFromBvid(bvid: string, page = 1): Promise<any> {
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

  async getPlayurl(bvid: string, cid: any, cookies?: string): Promise<any> {
    const base = String(this.runtimeConfig.getForSource('bilibili', 'playurl.baseUrl') || 'https://api.bilibili.com');
    // Requested qn comes directly from preferred quality policy.
    const qualityPolicy = buildVideoQualityPolicy(
      this.runtimeConfig.getForSource('bilibili', 'download.preferVideoQuality'),
    );
    const url = `${base.replace(/\/$/, '')}/x/player/playurl?bvid=${bvid}&cid=${cid}&qn=${qualityPolicy.preferredQn}&fnval=16`;
    const headers: any = { Referer: `https://www.bilibili.com/video/${bvid}` };
    if (cookies) headers.Cookie = cookies;
    const timeout = Number(this.runtimeConfig.getForSource('bilibili', 'playurl.timeoutMs') ?? 5000);
    const r = await axios.get(url, { headers, timeout });
    return r.data;
  }
}
