#!/bin/bash

# 1. create bivariate axes
# 2. create tasks in task_queue table
# 3. update state of indicators in bivariate_indicators_metadata table

# sleep if postgres is not responding

while true; do
    if ! pg_isready -q; then
        echo "postgres not ready, $0 is waiting..."
        sleep 120
    fi

	psql -qf scripts/create_quality_stops_analytics_tasks.sql

    # "repeatable read" is required in case update_indicators_state.sql starts when
    # indicator tasks are completed, but correlation tasks are not yet created - so that we're not mistakenly mark it as READY
    psql -1 -qc "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"   \
            -qf scripts/create_correlation_tasks.sql  \
            -qf scripts/update_indicators_state.sql
    sleep 20
done
