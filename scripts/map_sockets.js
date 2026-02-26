const fs = require('fs');
const url = require('url');
const dns = require('dns');
const axios = require('axios');

function hexToIp(hex) {
  // hex little-endian e.g. '0100007F' -> 127.0.0.1
  const bytes = hex.match(/.{2}/g).reverse();
  return bytes.map(b => parseInt(b, 16)).join('.');
}

function hexToPort(hex) {
  return parseInt(hex, 16);
}

async function main() {
  try {
    const bvid = process.argv[2] || 'BV1Nif2BHE3d';
    const cookiesArr = JSON.parse(fs.readFileSync('config/cookies.json', 'utf8'));
    const cookieStr = cookiesArr.map(c => `${c.name}=${c.value}`).join('; ');

    // fetch cid from pagelist first
    const pagelist = await axios.get(`https://api.bilibili.com/x/player/pagelist?bvid=${bvid}`, {
      headers: { Referer: 'https://www.bilibili.com' },
      timeout: 15000,
    });
    const cid = pagelist.data && pagelist.data.data && pagelist.data.data[0] && pagelist.data.data[0].cid;
    if (!cid) {
      console.error('Unable to resolve cid from pagelist');
      process.exit(1);
    }
    const playurlRes = await axios.get(
      `https://api.bilibili.com/x/player/playurl?bvid=${bvid}&cid=${cid}&qn=116&fnval=16`,
      { headers: { Referer: 'https://www.bilibili.com', Cookie: cookieStr }, timeout: 15000 }
    );
    const data = playurlRes.data && playurlRes.data.data;
    if (!data) {
      console.error('No playurl data');
      process.exit(1);
    }

    const urls = [];
    if (data.dash) {
      const dash = data.dash;
      (dash.video || []).forEach(v => {
        if (v.baseUrl) urls.push(v.baseUrl);
        if (v.backupUrl) urls.push(...v.backupUrl);
      });
      (dash.audio || []).forEach(a => {
        if (a.baseUrl) urls.push(a.baseUrl);
        if (a.backupUrl) urls.push(...a.backupUrl);
      });
    }
    if (data.durl) {
      (data.durl || []).forEach(d => { if (d.url) urls.push(d.url); if (d.backup_url) urls.push(...d.backup_url); });
    }

    const hosts = Array.from(new Set(urls.map(u => url.parse(u).host).filter(Boolean)));
    console.log('Discovered hosts:', hosts);

    const hostIps = {};
    for (const h of hosts) {
      try {
        const ip = await new Promise((res, rej) => dns.lookup(h, (e, address) => e ? rej(e) : res(address)));
        hostIps[h] = ip;
      } catch (e) {
        hostIps[h] = `DNS_ERR:${e.message}`;
      }
    }

    console.log('Resolved hosts -> IPs:', hostIps);

    const procNet = fs.readFileSync('/proc/net/tcp', 'utf8').split('\n').slice(1).filter(Boolean);
    const tcpEntries = procNet.map(line => {
      const parts = line.trim().split(/\s+/);
      // parts: sl, local_address, rem_address, st, tx_queue, rx_queue, tr, tm->when, retrnsmt, uid, timeout, inode, ...
      const local = parts[1];
      const rem = parts[2];
      const inode = parts[9];
      const [lipHex, lportHex] = local.split(':');
      const [ripHex, rportHex] = rem.split(':');
      return {
        inode,
        local: `${hexToIp(lipHex)}:${hexToPort(lportHex)}`,
        remote: `${hexToIp(ripHex)}:${hexToPort(rportHex)}`,
      };
    });

    console.log('Parsed /proc/net/tcp entries count=', tcpEntries.length);

    const fdFiles = fs.readdirSync('/proc/1/fd');
    const socketFds = [];
    for (const fd of fdFiles) {
      try {
        const link = fs.readlinkSync(`/proc/1/fd/${fd}`);
        const m = link.match(/^socket:\[(\d+)\]$/);
        if (m) socketFds.push({ fd, inode: m[1] });
      } catch (e) {
        // ignore
      }
    }

    console.log('Open socket fds on PID 1:', socketFds.map(s => `${s.fd}->${s.inode}`).join(', ') || 'none');

    const matches = [];
    for (const s of socketFds) {
      const entry = tcpEntries.find(t => t.inode === s.inode);
      if (entry) {
        // see if remote matches any host IPs
        const [rip] = entry.remote.split(':');
        const matchedHosts = Object.keys(hostIps).filter(h => hostIps[h] === rip);
        matches.push({ fd: s.fd, inode: s.inode, remote: entry.remote, matchedHosts });
      } else {
        matches.push({ fd: s.fd, inode: s.inode, remote: null, matchedHosts: [] });
      }
    }

    console.log('Socket mapping results:');
    matches.forEach(m => console.log(JSON.stringify(m)));

    // Also print any tcp entries that match host IPs even if not open as fd
    const targetIps = new Set(Object.values(hostIps).filter(v => !v.startsWith('DNS_ERR')));
    const tcpToTargets = tcpEntries.filter(t => targetIps.has(t.remote.split(':')[0]));
    console.log('TCP connections to target IPs:', tcpToTargets);

    process.exit(0);
  } catch (err) {
    console.error('ERROR', err && err.message);
    process.exit(1);
  }
}

main();
