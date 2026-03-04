import { Injectable } from '@nestjs/common';
import axios from 'axios';
import { RuntimeConfigService } from '../config/runtime-config.service';

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
    // Bilibili quality code:
    // - 80: 1080p (Full HD)
    // - 112/116/120/125/126/127: higher than 1080p depending on content/account
    // Request a high target quality, then worker validates minimum acceptable quality.
    const targetQn = Number(this.runtimeConfig.getForSource('bilibili', 'playurl.targetQn') ?? process.env.PLAYURL_TARGET_QN ?? 116);
    const url = `${base.replace(/\/$/, '')}/x/player/playurl?bvid=${bvid}&cid=${cid}&qn=${targetQn}&fnval=16`;
    const headers: any = { Referer: `https://www.bilibili.com/video/${bvid}` };
    if (cookies) headers.Cookie = cookies;
    const timeout = Number(this.runtimeConfig.getForSource('bilibili', 'playurl.timeoutMs') ?? 5000);
    const r = await axios.get(url, { headers, timeout });
    return r.data;
  }
}
