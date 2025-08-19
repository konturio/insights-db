#!/bin/bash

while true; do
    partitions=`psql -Atqf scripts/btree_bloat-superuser.sql`

    if [ -n "$partitions" ]; then
        # reindex partitions in 5 parallel jobs (more jobs will provoke OOM killer)
        command="reindexdb --concurrently -v -j 5 $(for p in $partitions; do echo -n ' -t ' $p; done)"
        echo reindexing $partitions
        eval $command
    else
        echo "nothing to reindex"
    fi
    sleep 1d
done
