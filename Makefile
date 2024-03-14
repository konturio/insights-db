SHELL := /bin/bash

.PHONY: all
all: task_scheduler remove_outated_indicators_loop insights_tasks_loop apply_overrides_loop


.PHONY: insights_tasks_loop
insights_tasks_loop:
	$(SHELL) scripts/create_sql_functions.sh
	$(SHELL) scripts/do_tasks.sh

.PHONY: task_scheduler
task_scheduler:
	$(SHELL) scripts/create_tasks.sh

.PHONY: apply_overrides_loop
apply_overrides_loop:
	while true; do psql -qf scripts/apply_all_axis_overrides.sql; sleep 5m; done

.PHONY: remove_outated_indicators_loop
remove_outated_indicators_loop:
	while true; do psql -1 -f scripts/remove_outated_indicators.sql; sleep 5m; done
