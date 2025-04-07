SHELL := /bin/bash

.PHONY: all
all: task_scheduler remove_failed_upload_loop remove_outated_indicators_loop insights_tasks_loop apply_overrides_loop geometry reporting


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
	$(SHELL) scripts/remove_outated_indicators.sh

.PHONY: remove_failed_upload_loop
remove_failed_upload_loop:
	while true; do psql -tf scripts/remove_failed_upload.sql; sleep 4m; done

.PHONY: reporting
reporting:
	while true; do psql -qtf scripts/reporting.sql; sleep 45m; done

.PHONY: geometry
geometry:
	# once a day fill the gaps in stat_h3_geom
	while true; do echo 'start updating stat_h3_geom...'; psql -f scripts/update_stat_h3_geom.sql; echo 'end updating stat_h3_geom.'; sleep 1d; done
