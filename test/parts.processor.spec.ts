import * as fs from 'fs';
import * as path from 'path';
import { Readable } from 'stream';
import axios from 'axios';
import { PartsProcessor } from '../src/worker/parts.processor';

jest.mock('axios');

describe('PartsProcessor', () => {
  const dataDir = path.join(process.cwd(), 'data');
  const jobId = 'testjob';
  beforeAll(() => { if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true }); });
  afterAll(() => { try { fs.rmSync(path.join(dataDir, jobId), { recursive: true }); } catch (e) {} });

  it('downloads a byte-range part and records Redis fields via queue.client', async () => {
    const content = Buffer.from('part-data');
    const stream = new Readable();
    stream.push(content);
    stream.push(null);
    (axios.get as jest.Mock).mockResolvedValue({
      data: stream,
      headers: { 'content-length': String(content.length) },
      status: 200,
    });

    const fakeQueue: any = { client: { hset: jest.fn().mockResolvedValue(1) } };
    const processor = new PartsProcessor(fakeQueue);

    const job: any = {
      data: {
        jobId,
        bvid: 'bv1',
        url: 'http://example.com/file',
        partIndex: 0,
        rangeStart: 0,
        rangeEnd: 100,
        expectedBytes: 100,
        headers: {},
      },
      progress: jest.fn().mockResolvedValue(undefined),
    };

    const result = await processor.handlePart(job as any);
    expect(result).toBeDefined();
    const partPath = path.join(dataDir, jobId, 'parts', 'part-0.bin');
    expect(fs.existsSync(partPath)).toBeTruthy();
    expect(fakeQueue.client.hset).toHaveBeenCalled();
  });

  it('downloads multiple segment URLs and concatenates into a part', async () => {
    const seg1 = Buffer.from('segment-1-');
    const seg2 = Buffer.from('segment-2');
    const stream1 = new Readable(); stream1.push(seg1); stream1.push(null);
    const stream2 = new Readable(); stream2.push(seg2); stream2.push(null);
    // Make axios.get return different streams per call
    (axios.get as jest.Mock)
      .mockImplementationOnce(() => Promise.resolve({ data: stream1, headers: { 'content-length': String(seg1.length) }, status: 200 }))
      .mockImplementationOnce(() => Promise.resolve({ data: stream2, headers: { 'content-length': String(seg2.length) }, status: 200 }));

    const fakeQueue: any = { client: { hset: jest.fn().mockResolvedValue(1) } };
    const processor = new PartsProcessor(fakeQueue);
    const job: any = {
      data: {
        jobId,
        bvid: 'bv1',
        segmentUrls: ['http://example.com/seg1', 'http://example.com/seg2'],
        partIndex: 1,
        expectedBytes: seg1.length + seg2.length,
        headers: {},
      },
      progress: jest.fn().mockResolvedValue(undefined),
    };

    const result = await processor.handlePart(job as any);
    expect(result).toBeDefined();
    const partPath = path.join(dataDir, jobId, 'parts', 'part-1.bin');
    expect(fs.existsSync(partPath)).toBeTruthy();
    const buf = fs.readFileSync(partPath);
    expect(buf.toString()).toEqual(Buffer.concat([seg1, seg2]).toString());
  });
});
