#!/bin/bash

# read task_queue and dispatch tasks.
# sleep if postgres is not responding

while true; do
    if ! pg_isready -q; then
        echo "postgres not ready, $0 is waiting..."
        sleep 120
        continue
    fi
    # if the task takes longer than 45 min, possibly the postgres stats is outdated for some indicator.
    # usually, if we interrupt the task, it will run much faster next time
    seq `psql -c 'select count(0) from task_queue' -t` | parallel -j ${MAX_PARALLEL_TASKS:=3} -n0 'psql -q -c "set statement_timeout='\''45 min'\''; call dispatch()"'
    sleep 10
done
