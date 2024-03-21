#!/bin/bash

# a wrapper for creating sql functions and procedures.
# insights_tasks_loop target depends on it and won't start until migration runs correctly

read -r -d '' migrate_cmd << EOF
    psql -qf procedures/dispatch.sql
         -qf procedures/direct_quality_estimation.sql
         -qf procedures/16269_bivariate_axis_analytics.sql
         -qf procedures/axis_stops_estimation.sql
         -qf procedures/bivariate_axis_correlation.sql
         -qf procedures/calculate_system_indicators.sql
         -qf scripts/add_system_indicators_metadata.sql
EOF

eval $migrate_cmd

while (( $? != 0 )); do
    echo 'WARN: failed to create SQL functions'
    sleep 10
    eval $migrate_cmd
done

echo 'successfully created SQL functions'
