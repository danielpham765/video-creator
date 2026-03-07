import { Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import { execFile } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs';
import * as path from 'path';
import { RuntimeConfigService } from '../../config/runtime-config.service';
import { ResolveSourceInput, ResolvedSource, SourceResolver } from '../source.types';
import { buildVideoQualityPolicy } from '../video-quality';
import { createYtDlpCookieFile, safeUnlink } from '../../utils/yt-dlp-cookies';

@Injectable()
export class YouTubeResolver implements SourceResolver {
  readonly platform = 'youtube' as const;
  private readonly logger = new Logger(YouTubeResolver.name);
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
      const requestHeaders = this.buildRequestHeaders(input);
      const qualityPolicy = buildVideoQualityPolicy(
        this.runtimeConfig.getForSource('youtube', 'download.preferVideoQuality'),
      );
      const viaYtDlp = await this.resolveViaYtDlp(
        canonicalUrl,
        vid,
        input.title,
        qualityPolicy.preferredQn,
        qualityPolicy.minAcceptableQn,
        requestHeaders,
      );
      if (viaYtDlp.source) return viaYtDlp.source;
      const reason = viaYtDlp.failureReason || 'unknown reason';
      this.logger.warn(`youtube resolve failed for vid=${vid}: ${reason}`);
      throw new Error(`youtube resolver could not obtain a usable stream url (yt-dlp failed: ${reason})`);
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

  private async resolveViaYtDlp(
    canonicalUrl: string,
    vid: string,
    preferredTitle?: string,
    preferredQn = 80,
    minAcceptableQn = 64,
    requestHeaders?: Record<string, string>,
  ): Promise<{ source: ResolvedSource | null; failureReason: string | null }> {
    const preferredHeight = this.toHeightFromQn(preferredQn);
    const minAcceptableHeight = this.toHeightFromQn(minAcceptableQn);
    const formatSelector =
      `bv*[height<=${preferredHeight}][height>=${minAcceptableHeight}]+ba/` +
      `b[height<=${preferredHeight}][height>=${minAcceptableHeight}]/` +
      `bv*[height<=${preferredHeight}][height>=${minAcceptableHeight}]+ba/` +
      `b[height<=${preferredHeight}][height>=${minAcceptableHeight}]`;
    const candidates = ['/usr/local/bin/yt-dlp', '/usr/bin/yt-dlp', 'yt-dlp'];
    const extractionErrors: string[] = [];
    const probeFailures: string[] = [];
    let parsed: any = null;
    const cookieHeader = String(requestHeaders?.Cookie || '').trim();
    const cookieVariants: Array<{ label: string; cookieHeader: string }> = cookieHeader
      ? [
          { label: 'with-cookie', cookieHeader },
          { label: 'without-cookie', cookieHeader: '' },
        ]
      : [{ label: 'no-cookie', cookieHeader: '' }];
    try {
      for (const bin of candidates) {
        for (const cookieVariant of cookieVariants) {
          let cookieFilePath: string | null = null;
          let args: string[] = [];
          try {
            args = ['-J', '--no-playlist', '--no-warnings', '-f', formatSelector];
            const userAgentHeader = String(requestHeaders?.['User-Agent'] || '').trim();
            const refererHeader = String(requestHeaders?.Referer || '').trim();
            const originHeader = String(requestHeaders?.Origin || '').trim();
            if (cookieVariant.cookieHeader) {
              cookieFilePath = createYtDlpCookieFile({
                cookieHeader: cookieVariant.cookieHeader,
                targetUrl: canonicalUrl,
                outputDir: process.cwd(),
                filePrefix: `yt-dlp-cookies-${vid}`,
              });
              if (cookieFilePath) args.push('--cookies', cookieFilePath);
            }
            if (userAgentHeader) {
              args.push('--add-header', `User-Agent: ${userAgentHeader}`);
            }
            if (refererHeader) {
              args.push('--add-header', `Referer: ${refererHeader}`);
            }
            if (originHeader) {
              args.push('--add-header', `Origin: ${originHeader}`);
            }
            args.push(canonicalUrl);
            this.logger.debug(`youtube yt-dlp command: ${this.renderShellCommand(bin, args)}`);
            const { stdout } = await this.execFileAsync(
              bin,
              args,
              { timeout: 30000, maxBuffer: 10 * 1024 * 1024 },
            );
            parsed = JSON.parse(String(stdout || '{}'));
            if (!parsed || typeof parsed !== 'object') {
              extractionErrors.push(`${bin}: invalid JSON payload from yt-dlp (cookieMode=${cookieVariant.label})`);
              parsed = null;
              continue;
            }
            break;
          } catch (e: any) {
            if (String(e?.code || '') === 'ENOENT') {
              extractionErrors.push(`${bin}: binary not found (ENOENT)`);
              break;
            }
            const cmdText = args.length > 0 ? this.renderShellCommand(bin, args) : `${bin} <args unavailable>`;
            extractionErrors.push(
              `${bin}: ${this.summarizeExecFailure(e)} | cookieMode=${cookieVariant.label} | cmd=${cmdText}`,
            );
            if (cookieVariant.label === 'with-cookie') {
              this.logger.warn(`youtube resolver vid=${vid}: yt-dlp failed with cookies, retrying without cookies`);
            }
          } finally {
            safeUnlink(cookieFilePath);
          }
          if (parsed) break;
        }
        if (parsed) break;
      }
      if (!parsed) {
        return {
          source: null,
          failureReason: extractionErrors.length
            ? extractionErrors.join(' | ')
            : 'yt-dlp returned no metadata',
        };
      }

      const headers: Record<string, string> = {};
      const sourceHeaders = parsed?.http_headers && typeof parsed.http_headers === 'object' ? parsed.http_headers : {};
      for (const [key, value] of Object.entries(sourceHeaders)) {
        const k = String(key || '').trim();
        const v = String(value || '').trim();
        if (!k || !v) continue;
        headers[k] = v;
      }

      const requestedFormats = Array.isArray(parsed?.requested_formats) ? parsed.requested_formats : [];
      const videoFormat = requestedFormats.find((f: any) => String(f?.vcodec || 'none') !== 'none');
      const audioFormat = requestedFormats.find(
        (f: any) => String(f?.acodec || 'none') !== 'none' && String(f?.vcodec || 'none') === 'none',
      );
      const dashVideoUrl = String(videoFormat?.url || '').trim();
      const dashAudioUrl = String(audioFormat?.url || '').trim();
      const muxedUrl = String(parsed?.url || '').trim();

      if (dashVideoUrl && dashAudioUrl) {
        const [videoProbe, audioProbe] = await Promise.all([
          this.hasUsableStreamUrl(dashVideoUrl, headers),
          this.hasUsableStreamUrl(dashAudioUrl, headers),
        ]);
        if (!videoProbe.ok || !audioProbe.ok) {
          this.logger.warn(
            `youtube stream probe blocked for vid=${vid}; continuing with yt-dlp extracted dash urls ` +
              `(video=${videoProbe.reason}; audio=${audioProbe.reason})`,
          );
        }
        return {
          source: {
            platform: 'youtube',
            vid,
            canonicalUrl,
            title: preferredTitle || String(parsed?.title || '').trim() || vid,
            headers,
            qualityMeta: {
              qn:
                this.toQnFromYouTubeQuality(
                  String(videoFormat?.format_note || videoFormat?.format || '').trim(),
                  Number(videoFormat?.height || 0) || 0,
                ) || undefined,
              videoQualityLabel: String(videoFormat?.format_note || videoFormat?.format || '').trim() || undefined,
            },
            streams: {
              dashPair: { videoUrl: dashVideoUrl, audioUrl: dashAudioUrl },
              videoOnly: { url: dashVideoUrl },
              audioOnly: { url: dashAudioUrl },
            },
          },
          failureReason: null,
        };
      }

      if (!muxedUrl) {
        return {
          source: null,
          failureReason: probeFailures.length
            ? probeFailures.join(' | ')
            : 'yt-dlp metadata has no usable dash pair or muxed url',
        };
      }
      const muxedProbe = await this.hasUsableStreamUrl(muxedUrl, headers);
      if (!muxedProbe.ok) {
        this.logger.warn(
          `youtube muxed probe blocked for vid=${vid}; continuing with yt-dlp extracted muxed url ` +
            `(${muxedProbe.reason})`,
        );
      }

      return {
        source: {
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
        },
        failureReason: null,
      };
    } catch (e: any) {
      return {
        source: null,
        failureReason: `unexpected resolver error: ${this.summarizeExecFailure(e)}`,
      };
    }
  }

  private async hasUsableStreamUrl(
    url: string,
    headers: Record<string, string>,
  ): Promise<{ ok: boolean; reason: string }> {
    const target = String(url || '').trim();
    if (!target) return { ok: false, reason: 'empty url' };
    try {
      const ranged = await axios.get(target, {
        headers: { ...headers, Range: 'bytes=0-1' },
        responseType: 'stream',
        timeout: 10000,
        validateStatus: (status: number) => status === 200 || status === 206,
      });
      try {
        if (ranged?.data && typeof ranged.data.destroy === 'function') ranged.data.destroy();
      } catch {
        // ignore stream close errors
      }
      return {
        ok: ranged.status === 200 || ranged.status === 206,
        reason: `ranged status=${ranged.status}`,
      };
    } catch (e: any) {
      const rangedReason = this.summarizeHttpFailure(e);
      // Some YouTube signed URLs reject byte-range probes (403) but still allow full GET.
      // Retry once without Range and only read headers/body init to confirm reachability.
      try {
        const plain = await axios.get(target, {
          headers: { ...headers },
          responseType: 'stream',
          timeout: 10000,
          maxRedirects: 5,
          validateStatus: (status: number) => status >= 200 && status < 300,
        });
        try {
          if (plain?.data && typeof plain.data.destroy === 'function') plain.data.destroy();
        } catch {
          // ignore stream close errors
        }
        return {
          ok: plain.status >= 200 && plain.status < 300,
          reason: `ranged failed (${rangedReason}); plain status=${plain.status}`,
        };
      } catch (plainErr: any) {
        return {
          ok: false,
          reason: `ranged failed (${rangedReason}); plain failed (${this.summarizeHttpFailure(plainErr)})`,
        };
      }
    }
  }

  private summarizeExecFailure(error: any): string {
    const message = this.redactSensitiveHeaders(String(error?.message || error || 'unknown').replace(/\s+/g, ' ').trim());
    const code = String(error?.code || '').trim();
    const signal = String(error?.signal || '').trim();
    const stderr = this.redactSensitiveHeaders(String(error?.stderr || ''))
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .slice(-3)
      .join(' | ');
    const parts = [message];
    if (code) parts.push(`code=${code}`);
    if (signal) parts.push(`signal=${signal}`);
    if (stderr) parts.push(`stderr=${stderr}`);
    return this.compactDetail(parts.join(', '), 600);
  }

  private summarizeHttpFailure(error: any): string {
    const status = Number(error?.response?.status || 0);
    const statusText = String(error?.response?.statusText || '').trim();
    const message = String(error?.message || error || 'unknown').replace(/\s+/g, ' ').trim();
    if (status > 0) {
      return `status=${status}${statusText ? ` ${statusText}` : ''}`;
    }
    return this.compactDetail(message, 240);
  }

  private redactSensitiveHeaders(raw: string): string {
    return String(raw || '')
      .replace(/(--add-header\s+Cookie:)[^\s]+/gi, '$1<redacted>')
      .replace(/(Cookie:)\s*[^|,\n\r]+/gi, '$1 <redacted>');
  }

  private compactDetail(raw: string, limit = 600): string {
    const s = String(raw || '').trim();
    if (s.length <= limit) return s;
    return `${s.slice(0, limit - 3)}...`;
  }

  private renderShellCommand(bin: string, args: string[]): string {
    return [bin, ...args].map((a) => this.quoteShellArg(a)).join(' ');
  }

  private quoteShellArg(value: string): string {
    const s = String(value ?? '');
    if (/^[A-Za-z0-9_./:@=-]+$/.test(s)) return s;
    return `'${s.replace(/'/g, `'\"'\"'`)}'`;
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

  private toHeightFromQn(qn: number): number {
    const normalized = Number(qn || 0) || 0;
    if (normalized >= 120) return 2160;
    if (normalized >= 116) return 1440;
    if (normalized >= 80) return 1080;
    if (normalized >= 64) return 720;
    if (normalized >= 32) return 480;
    if (normalized >= 16) return 360;
    return 0;
  }

  private host(url: string): string {
    try {
      return new URL(url).hostname.toLowerCase();
    } catch {
      return '';
    }
  }
}
