import { Injectable } from '@nestjs/common';
import axios from 'axios';
import { execFile } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs';
import * as path from 'path';
import { RuntimeConfigService } from '../../config/runtime-config.service';
import { ResolveSourceInput, ResolvedSource, SourceResolver } from '../source.types';
import { buildVideoQualityPolicy } from '../video-quality';

@Injectable()
export class YouTubeResolver implements SourceResolver {
  readonly platform = 'youtube' as const;
  private readonly execFileAsync = promisify(execFile);
  constructor(private readonly runtimeConfig: RuntimeConfigService) {}

  canResolve(url: string): boolean {
    const host = this.host(url);
    return /(?:^|\.)youtube\.com$/i.test(host) || /(?:^|\.)youtu\.be$/i.test(host);
  }

  async resolve(input: ResolveSourceInput): Promise<ResolvedSource> {
    this.cleanupPlayerScriptArtifacts();
    try {
      const rawUrl = String(input.url || '').trim();
      const vid = this.extractVideoId(rawUrl);
      if (!vid) throw new Error('unsupported youtube url: missing video id');

      const canonicalUrl = `https://www.youtube.com/watch?v=${vid}`;

      const ytdlClients = this.loadYtdlClients();
      const requestHeaders = this.buildRequestHeaders(input);
      const ytdlOptions = this.buildYtdlOptions(requestHeaders);
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

    const qualityPolicy = buildVideoQualityPolicy(
      this.runtimeConfig.getForSource('youtube', 'download.preferVideoQuality'),
    );
    const videoOnly = this.selectPreferredVideoFormat(
      formats.filter((f) => f?.hasVideo && !f?.hasAudio && f?.url),
      qualityPolicy.preferredQn,
      qualityPolicy.minAcceptableQn,
    );

    const muxedBoth = this.selectPreferredVideoFormat(
      formats.filter((f) => f?.hasVideo && f?.hasAudio && f?.url),
      qualityPolicy.preferredQn,
      qualityPolicy.minAcceptableQn,
    );
    if (!videoOnly && !muxedBoth) {
      throw new Error(
        `youtube resolver: no stream matches prefer=${qualityPolicy.preferredLabel} ` +
          `(required qn ${qualityPolicy.minAcceptableQn}-${qualityPolicy.preferredQn})`,
      );
    }

    const selectedQualityLabel = String(videoOnly?.qualityLabel || muxedBoth?.qualityLabel || '').trim();
    const selectedHeight = Number(videoOnly?.height || muxedBoth?.height || 0) || 0;
    const resolvedQn = this.toQnFromYouTubeQuality(selectedQualityLabel, selectedHeight);

    const canUseYtdl = await this.hasUsableStreamUrl(
      String(muxedBoth?.url || videoOnly?.url || audioOnly?.url || '').trim(),
      requestHeaders,
    );
    if (!canUseYtdl) {
      const viaYtDlp = await this.resolveViaYtDlp(canonicalUrl, vid, input.title);
      if (viaYtDlp) return viaYtDlp;
      throw new Error('youtube resolver could not obtain a usable stream url (ytdl and yt-dlp both failed)');
    }

      return {
        platform: 'youtube',
        vid,
        canonicalUrl,
        title: input.title || String(info?.videoDetails?.title || '').trim() || vid,
        headers: requestHeaders,
        qualityMeta: {
          qn: resolvedQn || undefined,
          videoQualityLabel: selectedQualityLabel || undefined,
        },
        streams: {
          dashPair: videoOnly?.url && audioOnly?.url ? { videoUrl: videoOnly.url, audioUrl: audioOnly.url } : undefined,
          videoOnly: videoOnly?.url ? { url: videoOnly.url } : undefined,
          audioOnly: audioOnly?.url ? { url: audioOnly.url } : undefined,
          muxedBoth: muxedBoth?.url ? { url: muxedBoth.url } : undefined,
        },
      };
    } finally {
      this.cleanupPlayerScriptArtifacts();
    }
  }

  private cleanupPlayerScriptArtifacts(): void {
    try {
      const cwd = process.cwd();
      const entries = fs.readdirSync(cwd);
      for (const name of entries) {
        if (!/^\d{10,}-player-script\.js$/.test(name)) continue;
        const p = path.join(cwd, name);
        try {
          const st = fs.statSync(p);
          if (!st.isFile()) continue;
          fs.unlinkSync(p);
        } catch {
          // ignore per-file cleanup errors
        }
      }
    } catch {
      // ignore cleanup errors
    }
  }

  private async resolveViaYtDlp(canonicalUrl: string, vid: string, preferredTitle?: string): Promise<ResolvedSource | null> {
    try {
      const { stdout } = await this.execFileAsync('/usr/local/bin/yt-dlp', [
        '-J',
        '--no-playlist',
        '--no-warnings',
        '-f',
        'b[ext=mp4]/b',
        canonicalUrl,
      ], { timeout: 30000, maxBuffer: 10 * 1024 * 1024 });
      const parsed: any = JSON.parse(String(stdout || '{}'));
      const muxedUrl = String(parsed?.url || '').trim();
      if (!muxedUrl) return null;

      const headers: Record<string, string> = {};
      const sourceHeaders = parsed?.http_headers && typeof parsed.http_headers === 'object' ? parsed.http_headers : {};
      for (const [key, value] of Object.entries(sourceHeaders)) {
        const k = String(key || '').trim();
        const v = String(value || '').trim();
        if (!k || !v) continue;
        headers[k] = v;
      }

      const usable = await this.hasUsableStreamUrl(muxedUrl, headers);
      if (!usable) return null;

      return {
        platform: 'youtube',
        vid,
        canonicalUrl,
        title: preferredTitle || String(parsed?.title || '').trim() || vid,
        headers,
        qualityMeta: {
          qn: this.toQnFromYouTubeQuality(
            String(parsed?.format_note || parsed?.format || '').trim(),
            Number(parsed?.height || 0) || 0,
          ) || undefined,
          videoQualityLabel: String(parsed?.format_note || parsed?.format || '').trim() || undefined,
        },
        streams: {
          muxedBoth: { url: muxedUrl },
        },
      };
    } catch {
      return null;
    }
  }

  private async hasUsableStreamUrl(url: string, headers: Record<string, string>): Promise<boolean> {
    const target = String(url || '').trim();
    if (!target) return false;
    try {
      const resp = await axios.get(target, {
        headers: { ...headers, Range: 'bytes=0-1' },
        responseType: 'stream',
        timeout: 10000,
        validateStatus: (status: number) => status === 200 || status === 206,
      });
      try {
        if (resp?.data && typeof resp.data.destroy === 'function') resp.data.destroy();
      } catch {
        // ignore stream close errors
      }
      return resp.status === 200 || resp.status === 206;
    } catch {
      return false;
    }
  }

  private async getInfoWithTimeout(ytdl: any, url: string, options: any, timeoutMs: number): Promise<any> {
    return await Promise.race([
      ytdl.getInfo(url, options),
      new Promise((_, reject) => setTimeout(() => reject(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs)),
    ]);
  }

  private buildRequestHeaders(input: ResolveSourceInput): Record<string, string> {
    const headers: Record<string, string> = {
      'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      Accept: '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
      Origin: 'https://www.youtube.com',
      Referer: 'https://www.youtube.com/',
    };
    if (input.cookies) headers.Cookie = String(input.cookies);
    return headers;
  }

  private buildYtdlOptions(headers: Record<string, string>): any {

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

  private toQnFromYouTubeQuality(qualityLabel: string, height: number): number {
    const fromLabel = parseInt(String(qualityLabel || '').replace(/[^0-9]/g, ''), 10) || 0;
    const px = Math.max(fromLabel, Number(height || 0) || 0);
    if (px >= 2160) return 120;
    if (px >= 1440) return 116;
    if (px >= 1080) return 80;
    if (px >= 720) return 64;
    if (px >= 480) return 32;
    if (px >= 360) return 16;
    return 0;
  }

  private selectPreferredVideoFormat(formats: any[], preferredQn: number, minAcceptableQn: number): any | undefined {
    return [...formats]
      .map((format) => ({ format, qn: this.toQnFromYouTubeQuality(String(format?.qualityLabel || ''), Number(format?.height || 0) || 0) }))
      .filter((item) => item.qn <= preferredQn && item.qn >= minAcceptableQn)
      .sort((a, b) => {
        if (b.qn !== a.qn) return b.qn - a.qn;
        return this.videoRank(b.format) - this.videoRank(a.format);
      })[0]?.format;
  }

  private host(url: string): string {
    try {
      return new URL(url).hostname.toLowerCase();
    } catch {
      return '';
    }
  }
}
