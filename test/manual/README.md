Manual test scripts (moved from `scripts/`). These helpers are intended for local, developer-driven testing and debugging.

Overview
- This folder contains scripts that exercise parts of the application stack (queues, HTTP endpoints, third-party APIs) for manual verification.
- They are not automated unit tests and should not be executed by CI unless the CI environment provides required services.

Files and quick usage
- `enqueue_test_parts.js`
  - Purpose: enqueue example part jobs into the `download-parts` Bull queue and write a small `data/<jobId>/manifest.json` for visibility.
  - Prereqs: Redis reachable at the hostname `redis` (or adjust the connection in the script).
  - Run: `node test/manual/enqueue_test_parts.js`

- `playurl_test_worker.js`
  - Purpose: fetch Bilibili `pagelist` and `playurl` endpoints and print the JSON response.
  - Prereqs: `config/cookies/bilibili.json` must exist and contain an array of cookie objects.
  - Run: `node test/manual/playurl_test_worker.js [BV_ID]`

- `poll_job.js`
  - Purpose: poll local download status endpoint at `/download/status/:jobId` and print progress until completion or timeout.
  - Prereqs: local server listening on `http://localhost:3000` with the `download/status` route.
  - Run: `node test/manual/poll_job.js [JOB_ID]`

Notes
- These scripts are intended for interactive debugging. They assume local infrastructure (Redis, local API server) or containerized environments with `/proc` access where appropriate.
- If you want these run as part of a reproducible test harness, we can adapt them into Jest tests or add small wrapper scripts to start required services in test mode.

Note about automated tests
- Files in this folder are intentionally excluded from automated test runs. Jest is configured to ignore `test/manual` (see `jest.config.ts` `testPathIgnorePatterns`) so these interactive helpers are not executed by CI or `npm test`.
