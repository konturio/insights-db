SHELL := /bin/bash

.PHONY: all
all: calculate_geometry_loop remove_outated_indicators_loop insights_tasks_loop


.PHONY: insights_tasks_loop
insights_tasks_loop:
	# TODO 17429: Implement core dispatcher to process calculation tasks
	echo insights_tasks_loop
	touch $@

.PHONY: calculate_geometry_loop
calculate_geometry_loop:
	while true; do make calculate_geometry; sleep 5m; done
	touch $@

.PHONY: calculate_geometry
calculate_geometry:
	# TODO 17377: run stat_h3_geom update
	echo calculate_geometry
	touch $@

.PHONY: remove_outated_indicators_loop
remove_outated_indicators_loop:
	while true; do make remove_outated_indicators; sleep 5m; done
	touch $@

.PHONY: remove_outated_indicators
remove_outated_indicators:
	psql -c "delete from bivariate_indicators_metadata where state = 'OUTDATED'"
	# due to FK constraint outdated indicators will also be removed from
	# bivariate_axis_correlation_v2, bivariate_axis_overrides, bivariate_axis_v2
	touch $@

.PHONY: clean
clean:
	rm -f insights_tasks_loop
	rm -f calculate_geometry_loop
	rm -f calculate_geometry
	rm -f remove_outated_indicators_loop
	rm -f remove_outated_indicators
