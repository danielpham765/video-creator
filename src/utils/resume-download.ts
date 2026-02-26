import * as fs from 'fs';
import axios from 'axios';

export async function resumeDownload(
  url: string,
  dest: string,
  headers?: Record<string, string>,
  cancelFilePath?: string,
  stopFileOrCheck?: string | (() => boolean),
): Promise<void> {
  const start = fs.existsSync(dest) ? fs.statSync(dest).size : 0;
  const reqHeaders: Record<string, string> = Object.assign({}, headers || {});
  if (start > 0) {
    reqHeaders.Range = `bytes=${start}-`;
    console.log(`[download] Resuming ${dest} from ${start}`);
  }

  const response = await axios.get(url, {
    responseType: 'stream',
    headers: reqHeaders,
    timeout: 30000,
    validateStatus: (status) => (status >= 200 && status < 300) || status === 206,
  });

  if (start > 0 && response.status === 200) {
    console.log(`[download] Server ignored Range; restarting ${dest}`);
    try { fs.unlinkSync(dest); } catch (e) { }
    return resumeDownload(url, dest, headers);
  }

  const contentLength = parseInt(response.headers['content-length'] || '0', 10) || 0;
  let total: number | null = null;
  if (response.status === 206 && start > 0) total = start + contentLength;
  else if (contentLength > 0) total = contentLength;

  const writer = fs.createWriteStream(dest, { flags: start > 0 ? 'a' : 'w' });
  let received = start;
  let lastLog = Date.now();

  response.data.on('data', (chunk: Buffer) => {
    received += chunk.length;
    const now = Date.now();
    // cooperative cancellation: if cancel file exists, abort the stream
    try {
      if (cancelFilePath && fs.existsSync(cancelFilePath)) {
        const err = new Error('download cancelled');
        response.data.destroy(err);
        try { writer.close(); } catch (e) { }
        return;
      }
      // support either a stop-file path or a stop-check function
      if (stopFileOrCheck) {
        if (typeof stopFileOrCheck === 'string') {
          if (fs.existsSync(stopFileOrCheck)) {
            const err = new Error('download stopped');
            response.data.destroy(err);
            try { writer.close(); } catch (e) { }
            return;
          }
        } else if (typeof stopFileOrCheck === 'function') {
          try {
            if (stopFileOrCheck()) {
              const err = new Error('download stopped');
              response.data.destroy(err);
              try { writer.close(); } catch (e) { }
              return;
            }
          } catch (e) {
            // ignore check errors
          }
        }
      }
    } catch (e) {
      // ignore fs errors
    }
    if (now - lastLog > 1000) {
      if (total) {
        const pct = ((received / total) * 100).toFixed(1);
        console.log(`[download] ${dest} received ${received}/${total} bytes (${pct}%)`);
      } else {
        console.log(`[download] ${dest} received ${received} bytes`);
      }
      lastLog = now;
    }
  });

  await new Promise<void>((resolve, reject) => {
    response.data.pipe(writer);
    let error: any = null;
    response.data.on('error', (err) => { error = err; try { writer.close(); } catch (e) { } ; reject(err); });
    writer.on('error', (err) => { error = err; try { writer.close(); } catch (e) { } ; reject(err); });
    writer.on('close', () => { if (!error) resolve(); else reject(error); });
  });
}

export default resumeDownload;
