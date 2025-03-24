#!/bin/bash

# read task_queue and dispatch tasks.
# sleep if postgres is not responding

while true; do
    if ! pg_isready -q; then
        echo "postgres not ready, $0 is waiting..."
        sleep 120
        continue
    fi
    copy_task_exists=$(psql -t -c "select 1 from task_queue where task_type='copy' limit 1")
    seq `psql -c 'select count(0) from task_queue' -t` | parallel -j ${MAX_PARALLEL_TASKS:=3} -n0 "psql -q -c 'set statement_timeout=\"${TASK_TIMEOUT:-1h}\"' -c 'call dispatch()'"
    if [ -n "$copy_task_exists" ]; then
        vacuumdb -j 300 --analyze-only
    fi
    sleep 10
done
