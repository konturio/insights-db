SHELL := /bin/bash

.PHONY: all
all: task_scheduler calculate_geometry_loop remove_outated_indicators_loop insights_tasks_loop apply_overrides_loop


.PHONY: insights_tasks_loop
insights_tasks_loop:
	psql -f procedures/dispatch.sql
	psql -f procedures/direct_quality_estimation.sql
	psql -f procedures/16269_bivariate_axis_analytics.sql
	psql -f procedures/apply_bivariate_axis_overrides.sql
	psql -f procedures/axis_stops_estimation.sql
	psql -f procedures/bivariate_axis_correlation.sql

	while true; do seq `psql -c 'select count(0) from task_queue' -t` | parallel -n0 "psql -q -c 'call dispatch()'"; sleep 1; done

.PHONY: task_scheduler
task_scheduler:
	# 1. creates bivariate axes
	# 2. creates tasks in task_queue table
	# 3. updates state of indicators in bivariate_indicators_metadata table
	# "repeatable read" is required in case update_indicators_state.sql starts when
	# indicator tasks are completed, but correlation tasks are not yet created - so that we're not mistakenly mark it as READY
	while true; do psql -f scripts/create_quality_stops_analytics_tasks.sql; psql -1 -c "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" -f scripts/create_correlation_tasks.sql -f scripts/update_indicators_state.sql; sleep 1m; done

.PHONY: apply_overrides_loop
apply_overrides_loop:
	while true; do psql -f scripts/apply_all_axis_overrides.sql; sleep 5m; done

.PHONY: calculate_geometry_loop
calculate_geometry_loop:
	while true; do make calculate_geometry; sleep 5m; done

.PHONY: calculate_geometry
calculate_geometry:
	# TODO 17377: run stat_h3_geom update
	echo calculate_geometry

.PHONY: remove_outated_indicators_loop
remove_outated_indicators_loop:
	while true; do psql -1 -f scripts/remove_outated_indicators.sql; sleep 5m; done
