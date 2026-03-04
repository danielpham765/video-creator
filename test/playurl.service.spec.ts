import axios from 'axios';
import { PlayurlService } from '../src/playurl/playurl.service';

jest.mock('axios');

describe('PlayurlService.getCidFromBvid', () => {
  const mockedAxios = axios as jest.Mocked<typeof axios>;
  const runtimeConfig = {
    getForSource: (_source: string, key: string) => {
      if (key === 'playurl.baseUrl') return 'https://api.bilibili.com';
      if (key === 'playurl.timeoutMs') return 5000;
      return undefined;
    },
  } as any;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('selects cid by requested page number', async () => {
    mockedAxios.get.mockResolvedValue({
      data: {
        data: [
          { page: 1, cid: 111 },
          { page: 2, cid: 222 },
        ],
      },
    } as any);

    const service = new PlayurlService(runtimeConfig);
    await expect(service.getCidFromBvid('BV1abc', 2)).resolves.toBe(222);
  });

  it('falls back to first cid when requested page does not exist', async () => {
    mockedAxios.get.mockResolvedValue({
      data: {
        data: [
          { page: 1, cid: 111 },
          { page: 3, cid: 333 },
        ],
      },
    } as any);

    const service = new PlayurlService(runtimeConfig);
    await expect(service.getCidFromBvid('BV1abc', 2)).resolves.toBe(111);
  });
});
