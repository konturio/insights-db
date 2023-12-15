SHELL := /bin/bash

.PHONY: all
all: calculate_geometry_loop remove_outated_indicators_loop insights_tasks_loop


.PHONY: insights_tasks_loop
insights_tasks_loop:
	psql -f procedures/dispatch.sql
	psql -f procedures/direct_quality_estimation.sql
	while true; do seq `psql -c 'select count(0) from task_queue' -t` | parallel -n0 "psql -q -c 'call dispatch()'"; sleep 1; done

.PHONY: calculate_geometry_loop
calculate_geometry_loop:
	while true; do make calculate_geometry; sleep 5m; done

.PHONY: calculate_geometry
calculate_geometry:
	# TODO 17377: run stat_h3_geom update
	echo calculate_geometry

.PHONY: remove_outated_indicators_loop
remove_outated_indicators_loop:
	while true; do make remove_outated_indicators; sleep 5m; done

.PHONY: remove_outated_indicators
remove_outated_indicators:
	psql -1 -f scripts/remove_outated_indicators.sql
