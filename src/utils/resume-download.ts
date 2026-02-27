import * as fs from 'fs';
import axios from 'axios';
import * as crypto from 'crypto';

export type StopCheckFn = () => boolean;
export type ProgressCallback = (bytesDelta: number) => void;

export interface ResumeDownloadResult {
  bytes: number;
  md5?: string;
}

export async function resumeDownload(
  url: string,
  dest: string,
  headers?: Record<string, string>,
  cancelFilePath?: string,
  stopFileOrCheck?: string | StopCheckFn,
  onProgress?: ProgressCallback,
  computeMd5: boolean = false,
  options?: { timeoutMs?: number; proxy?: string },
): Promise<ResumeDownloadResult> {
  const start = fs.existsSync(dest) ? fs.statSync(dest).size : 0;
  const reqHeaders: Record<string, string> = Object.assign({}, headers || {});
  if (start > 0) {
    reqHeaders.Range = `bytes=${start}-`;
    console.log(`[download] Resuming ${dest} from ${start}`);
  }

  // debug: print what we're about to request
  try {
    console.debug && console.debug(`[download] request ${url} -> ${dest} headers=${JSON.stringify(reqHeaders)}`);
  } catch (e) {}

  // Allow overriding timeout and proxy via options (backwards-compatible)
  const timeout = (options && options.timeoutMs) ? Number(options.timeoutMs) : 30000;
  const axiosConfig: any = {
    responseType: 'stream',
    headers: reqHeaders,
    timeout,
    validateStatus: (status: number) => (status >= 200 && status < 300) || status === 206,
  };
  if (options && options.proxy) {
    try {
      const p = new URL(String(options.proxy));
      const proxyObj: any = { host: p.hostname, port: Number(p.port) || (p.protocol === 'https:' ? 443 : 80) };
      if (p.username || p.password) proxyObj.auth = { username: decodeURIComponent(p.username), password: decodeURIComponent(p.password) };
      axiosConfig.proxy = proxyObj;
    } catch (e) {
      // ignore invalid proxy URL and let axios use env proxies
    }
  }
  const response = await axios.get(url, axiosConfig);

  try {
    console.debug && console.debug(`[download] response status=${response.status} content-length=${response.headers['content-length'] || 'unknown'}`);
  } catch (e) {}

  if (start > 0 && response.status === 200) {
    console.log(`[download] Server ignored Range; restarting ${dest}`);
    try { fs.unlinkSync(dest); } catch (e) { }
    return resumeDownload(url, dest, headers, cancelFilePath, stopFileOrCheck, onProgress, computeMd5, options);
  }

  const contentLength = parseInt(response.headers['content-length'] || '0', 10) || 0;
  let total: number | null = null;
  if (response.status === 206 && start > 0) total = start + contentLength;
  else if (contentLength > 0) total = contentLength;

  const writer = fs.createWriteStream(dest, { flags: start > 0 ? 'a' : 'w' });
  let received = start;
  let lastLog = Date.now();
  const hash = computeMd5 ? crypto.createHash('md5') : null;

  response.data.on('data', (chunk: Buffer) => {
    received += chunk.length;
    if (hash) {
      try {
        hash.update(chunk);
      } catch (e) {
        // ignore hash errors
      }
    }
    if (onProgress) {
      try {
        onProgress(chunk.length);
      } catch (e) {
        // ignore progress callback errors
      }
    }
    const now = Date.now();
    // cooperative cancellation: if cancel file exists, abort the stream
    try {
      if (cancelFilePath && fs.existsSync(cancelFilePath)) {
        const err = new Error('download cancelled');
        response.data.destroy(err);
        try { writer.close(); } catch (e) { }
        try { console.debug && console.debug(`[download] cancelled via cancelFile=${cancelFilePath}`); } catch (e) {}
        return;
      }
      // support either a stop-file path or a stop-check function
      if (stopFileOrCheck) {
          if (typeof stopFileOrCheck === 'string') {
          if (fs.existsSync(stopFileOrCheck)) {
            const err = new Error('download stopped');
            response.data.destroy(err);
            try { writer.close(); } catch (e) { }
            try { console.debug && console.debug(`[download] stopped via stopFile=${stopFileOrCheck}`); } catch (e) {}
            return;
          }
        } else if (typeof stopFileOrCheck === 'function') {
          try {
            if (stopFileOrCheck()) {
              const err = new Error('download stopped');
              response.data.destroy(err);
              try { writer.close(); } catch (e) { }
              try { console.debug && console.debug(`[download] stopped via stop-check function for ${dest}`); } catch (e) {}
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

  const result: ResumeDownloadResult = { bytes: received };
  if (hash) {
    try {
      result.md5 = hash.digest('hex');
    } catch (e) {
      // ignore hash digest errors
    }
  }
  return result;
}

export default resumeDownload;
