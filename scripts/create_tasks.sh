#!/bin/bash

# 1. create bivariate axes
# 2. create tasks in task_queue table
# 3. update state of indicators in bivariate_indicators_metadata table

# sleep if postgres is not responding

while true; do
    if ! pg_isready -q; then
        echo "postgres not ready, $0 is waiting..."
        sleep 120
        continue
    fi

    psql -qf scripts/create_copy_tasks.sql
    psql -qf scripts/create_quality_stops_analytics_tasks.sql
    [ $RUN_CORRELATIONS = true ] && psql -qf scripts/create_correlation_tasks.sql
    psql -qt1f scripts/update_indicators_state.sql | tee >(grep -q 'status change' && bash scripts/clean_insights_cache.sh)

    sleep 20
done
