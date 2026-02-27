import * as fs from 'fs';
import * as path from 'path';
import { Readable } from 'stream';
import axios from 'axios';
import resumeDownload from '../src/utils/resume-download';

jest.mock('axios');

describe('resumeDownload', () => {
  const tmpDir = path.join(process.cwd(), 'data', 'test-temp');
  beforeAll(() => { if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true }); });
  afterAll(() => { try { fs.rmSync(tmpDir, { recursive: true }); } catch (e) {} });

  it('downloads a small stream and computes md5', async () => {
    const content = Buffer.from('hello world');
    const stream = new Readable();
    stream.push(content);
    stream.push(null);
    (axios.get as jest.Mock).mockResolvedValue({
      data: stream,
      headers: { 'content-length': String(content.length) },
      status: 200,
    });
    const dest = path.join(tmpDir, 'hello.bin');
    const res = await resumeDownload('http://example.com/hello', dest, undefined, undefined, undefined, undefined, true);
    expect(res.bytes).toBeGreaterThan(0);
    expect(res.md5).toBeDefined();
    const fileBuf = fs.readFileSync(dest);
    expect(fileBuf.toString()).toEqual(content.toString());
  });
});
