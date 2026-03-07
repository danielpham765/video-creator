import { Injectable } from '@nestjs/common';
import axios from 'axios';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { BilibiliResolver } from './bilibili.resolver';
import { YouTubeResolver } from './youtube.resolver';
import { ResolveSourceInput, ResolvedSource, SourceResolver } from '../source.types';
import { createYtDlpCookieFile, safeUnlink } from '../../utils/yt-dlp-cookies';

@Injectable()
export class GenericResolver implements SourceResolver {
  readonly platform = 'generic' as const;
  private readonly execFileAsync = promisify(execFile);

  constructor(
    private readonly bilibiliResolver: BilibiliResolver,
    private readonly youtubeResolver: YouTubeResolver,
  ) {}

  canResolve(_url: string): boolean {
    return true;
  }

  async resolve(input: ResolveSourceInput): Promise<ResolvedSource> {
    const url = String(input.url || '').trim();
    if (!url) throw new Error('url is required');

    if (this.isDirectMediaUrl(url)) {
      return this.buildDirectMediaSource(url, input.title);
    }

    const resp = await axios.get(url, {
      timeout: 8000,
      responseType: 'text',
      maxContentLength: 1024 * 1024,
      maxBodyLength: 1024 * 1024,
      validateStatus: (s) => s >= 200 && s < 400,
    });

    const contentType = String(resp.headers?.['content-type'] || '').toLowerCase();
    if (contentType.startsWith('video/') || contentType.startsWith('audio/')) {
      return this.buildDirectMediaSource(url, input.title, contentType);
    }

    const html = String(resp.data || '');

    const embeddedYoutube = this.matchFirst(html, [
      /https?:\/\/(?:www\.)?youtube\.com\/watch\?v=[A-Za-z0-9_-]{6,}/i,
      /https?:\/\/(?:www\.)?youtu\.be\/[A-Za-z0-9_-]{6,}/i,
    ]);
    if (embeddedYoutube) {
      return this.youtubeResolver.resolve({ ...input, url: embeddedYoutube, platform: 'youtube' });
    }

    const embeddedBilibili = this.matchFirst(html, [
      /https?:\/\/(?:www\.)?bilibili\.com\/video\/BV[0-9A-Za-z]+[^"'\s]*/i,
    ]);
    if (embeddedBilibili) {
      return this.bilibiliResolver.resolve({ ...input, url: embeddedBilibili, platform: 'bilibili' });
    }

    const viaYtDlp = await this.resolveViaYtDlp(url, input.title, input.cookies);
    if (viaYtDlp) return viaYtDlp;

    const mediaSrc = this.matchFirst(html, [
      /<meta[^>]+property=["']og:video["'][^>]+content=["']([^"']+)["']/i,
      /<source[^>]+src=["']([^"']+)["']/i,
      /<video[^>]+src=["']([^"']+)["']/i,
    ], true);

    if (mediaSrc) {
      const absolute = this.toAbsoluteUrl(url, mediaSrc);
      if (absolute) return this.buildDirectMediaSource(absolute, input.title);
    }

    throw new Error('unsupported source: unable to resolve playable media streams from URL/page');
  }

  private async resolveViaYtDlp(url: string, title?: string, cookies?: string): Promise<ResolvedSource | null> {
    const candidates = ['/usr/local/bin/yt-dlp', '/usr/bin/yt-dlp', 'yt-dlp'];
    for (const bin of candidates) {
      let cookieFilePath: string | null = null;
      try {
        const args = ['-J', '--no-playlist', '--no-warnings'];
        const cookieHeader = String(cookies || '').trim();
        if (cookieHeader) {
          cookieFilePath = createYtDlpCookieFile({
            cookieHeader,
            targetUrl: url,
            outputDir: process.cwd(),
            filePrefix: 'yt-dlp-cookies-generic',
          });
          if (cookieFilePath) args.push('--cookies', cookieFilePath);
        }
        args.push(url);

        const { stdout } = await this.execFileAsync(bin, args, {
          timeout: 30000,
          maxBuffer: 10 * 1024 * 1024,
        });
        const parsed: any = JSON.parse(String(stdout || '{}'));
        const canonicalUrl = String(parsed?.webpage_url || url).trim() || url;
        const resolvedTitle = String(title || parsed?.title || this.defaultTitle(canonicalUrl)).trim();
        const headers = this.toHeaderRecord(parsed?.http_headers);

        const requestedFormats = Array.isArray(parsed?.requested_formats) ? parsed.requested_formats : [];
        const videoFormat = requestedFormats.find((f: any) => String(f?.vcodec || 'none') !== 'none');
        const audioFormat = requestedFormats.find(
          (f: any) => String(f?.acodec || 'none') !== 'none' && String(f?.vcodec || 'none') === 'none',
        );
        const dashVideoUrl = String(videoFormat?.url || '').trim();
        const dashAudioUrl = String(audioFormat?.url || '').trim();
        const muxedUrl = String(parsed?.url || '').trim();

        if (dashVideoUrl && dashAudioUrl) {
          return {
            platform: 'generic',
            vid: this.fingerprint(canonicalUrl),
            canonicalUrl,
            title: resolvedTitle,
            headers,
            streams: {
              dashPair: { videoUrl: dashVideoUrl, audioUrl: dashAudioUrl },
              videoOnly: { url: dashVideoUrl },
              audioOnly: { url: dashAudioUrl },
            },
          };
        }
        if (muxedUrl) {
          return {
            platform: 'generic',
            vid: this.fingerprint(canonicalUrl),
            canonicalUrl,
            title: resolvedTitle,
            headers,
            streams: {
              muxedBoth: { url: muxedUrl },
              videoOnly: { url: muxedUrl },
            },
          };
        }
      } catch (e: any) {
        if (String(e?.code || '') === 'ENOENT') continue;
        return null;
      } finally {
        safeUnlink(cookieFilePath);
      }
    }
    return null;
  }

  private toHeaderRecord(input: any): Record<string, string> {
    const out: Record<string, string> = {};
    if (!input || typeof input !== 'object' || Array.isArray(input)) return out;
    for (const [key, value] of Object.entries(input)) {
      const k = String(key || '').trim();
      const v = String(value || '').trim();
      if (!k || !v) continue;
      out[k] = v;
    }
    return out;
  }

  private buildDirectMediaSource(url: string, title?: string, contentType?: string): ResolvedSource {
    const ext = this.fileExt(url, contentType);
    const isAudio = ['m4a', 'mp3', 'aac', 'ogg', 'wav'].includes(ext);
    const isVideo = ['mp4', 'webm', 'mkv', 'mov', 'm3u8'].includes(ext);
    const base = {
      platform: 'generic' as const,
      vid: this.fingerprint(url),
      canonicalUrl: url,
      title: title || this.defaultTitle(url),
      headers: {},
    };

    if (isAudio) {
      return {
        ...base,
        streams: { audioOnly: { url } },
      };
    }
    if (isVideo) {
      return {
        ...base,
        streams: { muxedBoth: { url }, videoOnly: { url } },
      };
    }
    return {
      ...base,
      streams: { muxedBoth: { url } },
    };
  }

  private isDirectMediaUrl(url: string): boolean {
    return /(\.mp4|\.m4a|\.mp3|\.webm|\.mkv|\.mov|\.m3u8)(?:\?|#|$)/i.test(url);
  }

  private defaultTitle(url: string): string {
    try {
      const u = new URL(url);
      return u.hostname;
    } catch {
      return 'generic-media';
    }
  }

  private fileExt(url: string, contentType?: string): string {
    const ct = String(contentType || '').toLowerCase();
    if (ct.includes('audio/')) return ct.split('audio/')[1].split(';')[0].trim();
    if (ct.includes('video/')) return ct.split('video/')[1].split(';')[0].trim();
    try {
      const pathname = new URL(url).pathname.toLowerCase();
      const m = pathname.match(/\.([a-z0-9]+)$/);
      return m ? m[1] : '';
    } catch {
      return '';
    }
  }

  private fingerprint(url: string): string {
    return Buffer.from(url).toString('base64url').slice(0, 16) || 'generic';
  }

  private toAbsoluteUrl(base: string, candidate: string): string | null {
    try {
      return new URL(candidate, base).href;
    } catch {
      return null;
    }
  }

  private matchFirst(html: string, patterns: RegExp[], captureGroup = false): string | null {
    for (const pattern of patterns) {
      const m = html.match(pattern);
      if (!m) continue;
      const value = captureGroup ? m[1] : m[0];
      if (value) return value;
    }
    return null;
  }
}
