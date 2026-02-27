import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';

@Injectable()
export class PlayurlService {
  constructor(private readonly config: ConfigService) {}

  async getCidFromBvid(bvid: string): Promise<any> {
    const url = `https://api.bilibili.com/x/player/pagelist?bvid=${bvid}`;
    const timeout = Number(this.config.get('playurl.timeoutMs') ?? 5000);
    const r = await axios.get(url, { timeout });
    return r.data?.data?.[0]?.cid;
  }

  async getPlayurl(bvid: string, cid: any, cookies?: string): Promise<any> {
    const url = `https://api.bilibili.com/x/player/playurl?bvid=${bvid}&cid=${cid}&qn=116&fnval=16`;
    const headers: any = { Referer: `https://www.bilibili.com/video/${bvid}` };
    if (cookies) headers.Cookie = cookies;
    const timeout = Number(this.config.get('playurl.timeoutMs') ?? 5000);
    const r = await axios.get(url, { headers, timeout });
    return r.data;
  }
}
