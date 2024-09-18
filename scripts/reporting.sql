select now(), 'indicators count by state:';
select state, count(0) from bivariate_indicators_metadata group by state;

select now(), 'remaining tasks:';
select task_type, count(9) count from task_queue group by task_type order by count desc;

select now(), 'long-running queries:';
\x
select pid, application_name, client_addr, now()-query_start duration, wait_event_type, wait_event, state, pg_blocking_pids(pid) as blocked_by, replace(left(query,222), E'\n',' ')
from pg_stat_activity
where now()-query_start > interval '5 minutes' and state != 'idle';

select n_mod_since_analyze, n_ins_since_vacuum, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze, latest_indicator_upload
from pg_stat_all_tables, (select max(date) latest_indicator_upload from bivariate_indicators_metadata)
where relname = 'stat_h3_transposed';

table pg_stat_progress_vacuum;
