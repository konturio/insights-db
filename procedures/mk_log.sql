-- adds server timestamp and PID to the log message
-- example usage:
--   raise info using message = mk_log(format('task %s, ...', 'quality-task'));
--   select mk_log('new indicator:'), param_id from bivariate_indicators_metadata;
create or replace function mk_log(msg text default '') returns text as $$
begin
    return format('%s [%s] %s', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.US'), pg_backend_pid(), msg);
end;
$$ language plpgsql;
