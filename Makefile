.PHONY: logs worker api logs-worker-mapped

logs:
	@:

worker:
	bash scripts/tail-worker-log-mapped.sh

api:
	tail -f -n 300 logs/api/app.debug.log

logs-worker-mapped: worker
