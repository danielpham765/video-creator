#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="${1:-logs/worker/app.debug.log}"
TAIL_LINES="${2:-300}"

if [[ ! -f "$LOG_FILE" ]]; then
  echo "log file not found: $LOG_FILE" >&2
  exit 1
fi

tail -f -n "$TAIL_LINES" "$LOG_FILE" | awk '
NR==FNR { map[$1]=$2; next }
{
  sep = "  | ";
  p = index($0, sep);
  if (p > 0) {
    prefix = substr($0, 1, p - 1);
    if (prefix ~ /^worker-[[:alnum:]]+$/) {
      key = tolower(substr(prefix, 8));
      if (key in map) {
        $0 = "worker-" map[key] sep substr($0, p + length(sep));
      }
    }
  }
  print; fflush();
}
' <(
  docker compose ps -q worker \
  | xargs docker inspect --format '{{.Id}} {{.Name}}' \
  | awk '{
      id=tolower($1);
      name=$2; sub("^/", "", name);
      idx=name; sub(/^.*-worker-/, "", idx);
      print id, idx;
      print substr(id,1,12), idx;
    }'
) -
