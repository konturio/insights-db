#!/bin/bash

# read task_queue and dispatch tasks.
# sleep if postgres is not responding

while true; do
    if ! pg_isready -q; then
        echo "postgres not ready, $0 is waiting..."
        sleep 120
        continue
    fi

    tasks_before=`psql -tAXc "select array_agg(x) from (select x_numerator_id x from task_queue where task_type='copy')"`

    seq `psql -c 'select count(0) from task_queue' -t` | parallel -j ${MAX_PARALLEL_TASKS:=3} -n0 "psql -q -c 'set statement_timeout=\"${TASK_TIMEOUT:-1h}\"' -c 'call dispatch()'"

    tasks_after=`psql -tAXc "select array_agg(x) from (select x_numerator_id x from task_queue where task_type='copy')"`

    completed=`psql -tAXc "with before(x) as (select unnest('${tasks_before:-{\}}'::uuid[])), after(x) as (select unnest('${tasks_after:-{\}}'::uuid[])), completed(x) as (select x from before except select x from after) select array_agg(x) from completed"`

    if [ -n "$completed" ]; then
        partitions=`psql -tAXc "select distinct 'stat_h3_transposed_p'||i from generate_series(0,255) i, unnest('${completed:-{\}}'::uuid[]) m where satisfies_hash_partition((select oid from pg_class where relname = 'stat_h3_transposed'), 256, i, m)"`
        command="vacuumdb -v -j 300 --analyze-only $(for p in $partitions; do echo -n ' -t ' $p; done)"
        echo running $command
        eval $command
    fi
    sleep 10
done
