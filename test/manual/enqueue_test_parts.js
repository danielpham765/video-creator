const Queue = require('bull');
const fs = require('fs');
const path = require('path');

async function main() {
  const partsQueue = new Queue('download-parts', { connection: { host: 'redis' } });
  const jobId = `segtest-${Date.now()}`;
  const dataDir = path.join(process.cwd(), 'data', jobId);
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

  // Use a small public URL for segments (HTML is fine for test)
  const segUrlA = 'https://example.com/';
  const segUrlB = 'https://www.google.com/robots.txt';
  const segUrlC = 'https://httpbin.org/bytes/1024';

  // Build a simple manifest and write to disk for visibility
  const manifest = {
    jobId,
    parts: [
      { partIndex: 0, segmentIndexes: [0], segmentUrls: [segUrlA], expectedBytes: 1500 },
      { partIndex: 1, segmentIndexes: [1], segmentUrls: [segUrlB], expectedBytes: 1200 },
      { partIndex: 2, segmentIndexes: [2,3], segmentUrls: [segUrlC, segUrlA], expectedBytes: 2500 },
    ]
  };
  fs.writeFileSync(path.join(dataDir, 'manifest.json'), JSON.stringify(manifest, null, 2));
  console.log('Wrote manifest to', path.join(dataDir, 'manifest.json'));

  // Enqueue part jobs
  for (const p of manifest.parts) {
    const jobData = {
      jobId,
      bvid: 'TEST-SEG',
      segmentUrls: p.segmentUrls,
      partIndex: p.partIndex,
      expectedBytes: p.expectedBytes,
      headers: {},
      role: 'video'
    };
    const j = await partsQueue.add(jobData, { attempts: 2, backoff: 1000 });
    console.log('Enqueued part job', p.partIndex, 'bullJobId=', j.id);
  }

  // close queue and exit
  await partsQueue.close();
  console.log('Done enqueuing parts for', jobId);
}

main().catch((err) => { console.error(err); process.exit(1); });
