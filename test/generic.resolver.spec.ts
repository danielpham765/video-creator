import axios from 'axios';
import { GenericResolver } from '../src/source/resolvers/generic.resolver';
import { ResolvedSource } from '../src/source/source.types';

jest.mock('axios');

describe('GenericResolver', () => {
  const mockedAxios = axios as jest.Mocked<typeof axios>;
  const bilibiliResolver = { resolve: jest.fn() } as any;
  const youtubeResolver = { resolve: jest.fn() } as any;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('falls back to yt-dlp when page fetch fails (e.g. Douyin blocks/timeout)', async () => {
    mockedAxios.get.mockRejectedValue(new Error('timeout of 8000ms exceeded'));

    const resolvedViaYtDlp: ResolvedSource = {
      platform: 'generic',
      vid: 'douyin-abc',
      canonicalUrl: 'https://www.douyin.com/video/123',
      title: 'douyin-title',
      streams: {
        muxedBoth: { url: 'https://cdn.example.com/video.mp4' },
        videoOnly: { url: 'https://cdn.example.com/video.mp4' },
      },
    };

    const resolver = new GenericResolver(bilibiliResolver, youtubeResolver);
    const ytdlpSpy = jest.spyOn<any, any>(resolver as any, 'resolveViaYtDlp').mockResolvedValue(resolvedViaYtDlp);

    const output = await resolver.resolve({ url: 'https://www.douyin.com/video/123' });

    expect(ytdlpSpy).toHaveBeenCalledTimes(1);
    expect(output).toEqual(resolvedViaYtDlp);
  });

  it('includes fetch failure context when yt-dlp cannot resolve', async () => {
    mockedAxios.get.mockRejectedValue(new Error('timeout of 8000ms exceeded'));
    const resolver = new GenericResolver(bilibiliResolver, youtubeResolver);
    jest.spyOn<any, any>(resolver as any, 'resolveViaYtDlp').mockResolvedValue(null);

    await expect(resolver.resolve({ url: 'https://www.douyin.com/video/123' })).rejects.toThrow(
      /failed to fetch page metadata .*yt-dlp also could not resolve media streams/i,
    );
  });

  it('extracts douyin playable url from page html without invoking yt-dlp', async () => {
    mockedAxios.get.mockResolvedValue({
      headers: { 'content-type': 'text/html; charset=utf-8' },
      data:
        '<html><body><source src="https://www.douyin.com/aweme/v1/play/?aid=6383&amp;video_id=v0200f9&amp;ratio=1080p" /></body></html>',
    } as any);
    const resolver = new GenericResolver(bilibiliResolver, youtubeResolver);
    const ytdlpSpy = jest.spyOn<any, any>(resolver as any, 'resolveViaYtDlp').mockResolvedValue(null);

    const output = await resolver.resolve({ url: 'https://www.douyin.com/hashtag/abc?modal_id=1' });

    expect(ytdlpSpy).not.toHaveBeenCalled();
    expect(output.streams.muxedBoth?.url).toContain('https://www.douyin.com/aweme/v1/play/?aid=6383&video_id=v0200f9');
    expect(output.headers?.Referer).toBe('https://www.douyin.com/');
    expect(output.headers?.Origin).toBe('https://www.douyin.com');
  });
});
