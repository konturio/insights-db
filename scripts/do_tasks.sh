#!/bin/bash

# read task_queue and dispatch tasks.
# sleep if postgres is not responding

while true; do
    if ! pg_isready -q; then
        echo "postgres not ready, $0 is waiting..."
        sleep 120
        continue
    fi
    seq `psql -c 'select count(0) from task_queue' -t` | parallel -j ${MAX_PARALLEL_TASKS:=3} -n0 "psql -q -c 'set statement_timeout=\"${TASK_TIMEOUT:-1h}\"' -c 'call dispatch()'"
    vacuumdb -j 300 --analyze-only
    sleep 10
done
