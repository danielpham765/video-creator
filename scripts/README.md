Developer scripts for ad-hoc debugging, diagnostics, and manual integration tasks.

Purpose
- A small collection of one-off Node scripts used by maintainers to debug or exercise the running system.
- These are NOT automated tests and should not be moved into the project `test` runner.

Files
- `map_sockets.js` — OS/container diagnostic that parses `/proc/net/tcp` and attempts to map socket inodes to resolved hosts (Linux-only).
- Note: the following scripts were moved to `test/manual/` because they are manual test helpers:
  - `test/manual/enqueue_test_parts.js`
  - `test/manual/playurl_test_worker.js`
  - `test/manual/poll_job.js`

Prerequisites
- Node.js (v14+ recommended)
- Network access to external URLs used by the scripts
- `config/cookies.json` present and formatted as an array of `{ name, value }` objects for Bilibili-related scripts
- Redis is required to run `enqueue_test_parts.js` (it was moved to `test/manual`)
- `map_sockets.js` requires access to `/proc` (Linux/container). It will not function on macOS directly.

Usage examples
- Run the socket mapping diagnostic (Linux/container):

```bash
node scripts/map_sockets.js [BV_ID]
```

- For manual test helpers see `test/manual` (examples below).

Guidance
- Keep `scripts/` for short, environment-specific diagnostics. Use `test/manual/` for manual integration helpers that are not suitable for automated test runners.
