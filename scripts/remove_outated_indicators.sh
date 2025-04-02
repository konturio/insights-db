#!/bin/bash

while true; do 
    to_remove=`psql -AXtc "select array_agg(x) from (select internal_id x from bivariate_indicators_metadata where state = 'OUTDATED')"`
    if [ -n "$to_remove" ]; then
        echo $to_remove
        partitions=`psql -tAXc "select distinct 'stat_h3_transposed_p'||i from generate_series(0,255) i, unnest('${to_remove:-{\}}'::uuid[]) m where satisfies_hash_partition((select oid from pg_class where relname = 'stat_h3_transposed'), 256, i, m)"`
        echo $to_remove | tr -d '{}' | tr ',' '\n' | parallel 'psql -1 -t -f scripts/remove_outated_indicator.sql -v indicator_id={}'
        command="vacuumdb -v -j 300 --analyze-only $(for p in $partitions; do echo -n ' -t ' $p; done)"
        echo running $command
        eval $command
    fi
    sleep 5m
done

