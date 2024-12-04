#!/bin/bash

# a wrapper for creating sql functions and procedures.
# insights_tasks_loop target depends on it and won't start until migration runs correctly

read -r -d '' migrate_cmd << EOF
    psql -qf procedures/dispatch.sql
         -qf procedures/direct_quality_estimation.sql
         -qf procedures/16269_bivariate_axis_analytics.sql
         -qf procedures/transformations.sql
         -qf procedures/bivariate_axis_correlation.sql
         -qf procedures/calculate_system_indicators.sql
         -qf procedures/find_max_resolution.sql
         -qf procedures/check_new_indicator.sql
         -qf scripts/add_system_indicators_metadata.sql
EOF

while ! pg_isready -q; do
    echo "postgres not ready, $0 is waiting..."
    sleep 120
    continue
done

eval $migrate_cmd
echo 'successfully created SQL functions'
