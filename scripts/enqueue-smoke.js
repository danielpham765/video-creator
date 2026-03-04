const fs = require('fs');
const yaml = require('js-yaml');
const Queue = require('bull');

async function main() {
  try {
    const cfg = yaml.load(fs.readFileSync('config/config.yaml', 'utf8')) || {};
    const redisUrl = (cfg.redis && (cfg.redis.url || (cfg.redis.host ? `redis://${cfg.redis.host}:${cfg.redis.port||6379}` : null))) || process.env.REDIS_URL || 'redis://127.0.0.1:6379';
    console.log('Using redis:', redisUrl);
    const downloads = new Queue('downloads', redisUrl);
    const url = process.argv[2] || 'https://www.bilibili.com/video/BV1xx411c7mD';
    console.log('Enqueuing test job for url=', url);
    const job = await downloads.add({ url, platform: 'auto', mediaRequested: 'both' }, { attempts: 3, backoff: 5000 });
    console.log('Enqueued job id=', job.id);
    await downloads.close();
    process.exit(0);
  } catch (err) {
    console.error('Enqueue failed:', err);
    process.exit(1);
  }
}

main();
