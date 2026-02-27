const axios = require('axios');
const jobId = process.argv[2] || '2';
let lastProgress = null;
let lastUpdate = Date.now();
const pollInterval = 5000;
const noProgressLimit = 60000;

async function poll() {
  try {
    const r = await axios.get(`http://localhost:3000/download/status/${jobId}`, { timeout: 5000 });
    const s = r.data || {};
    const now = Date.now();
    const prog = typeof s.progress === 'number' ? s.progress : null;
    const state = s.state || '';
    console.log(new Date().toISOString(), 'state=', state, 'progress=', prog);
    if (lastProgress === null || prog !== lastProgress) {
      lastProgress = prog;
      lastUpdate = now;
    }
    if (state === 'finished' || state === 'failed' || state === 'error' || state === 'completed') {
      console.log('TERMINAL', state);
      process.exit(0);
    }
    if (now - lastUpdate > noProgressLimit) {
      console.log('NO_PROGRESS');
      process.exit(2);
    }
  } catch (e) {
    console.error('POLL_ERR', e.message);
  }
}

setInterval(poll, pollInterval);
poll();
