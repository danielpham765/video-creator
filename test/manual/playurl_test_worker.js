const fs = require('fs');
const axios = require('axios');

(async () => {
  try {
    const bvid = process.argv[2] || 'BV1Nif2BHE3d';
    const cookiesArr = JSON.parse(fs.readFileSync('config/cookies.json', 'utf8'));
    const cookieStr = cookiesArr.map(c => `${c.name}=${c.value}`).join('; ');

    console.log('Fetching pagelist for', bvid);
    const pagelist = await axios.get(`https://api.bilibili.com/x/player/pagelist?bvid=${bvid}`, {
      headers: { Referer: 'https://www.bilibili.com' },
      timeout: 15000,
    });
    console.log('pagelist.code=', pagelist.data && pagelist.data.code);
    const cid = pagelist.data && pagelist.data.data && pagelist.data.data[0] && pagelist.data.data[0].cid;
    console.log('cid=', cid);

    console.log('Calling playurl with cookie header (first 200 chars):', cookieStr.slice(0, 200));
    const playurl = await axios.get(
      `https://api.bilibili.com/x/player/playurl?bvid=${bvid}&cid=${cid}&qn=116&fnval=16`,
      {
        headers: { Referer: 'https://www.bilibili.com', Cookie: cookieStr },
        timeout: 15000,
      }
    );
    console.log('playurl.status=', playurl.status);
    console.log(JSON.stringify(playurl.data, null, 2));
  } catch (err) {
    console.error('ERROR', err.message);
    if (err.response) console.error('RESPONSE', err.response.status, JSON.stringify(err.response.data, null, 2));
    process.exit(1);
  }
})();
