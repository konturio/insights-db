create or replace procedure dispatch()
    language plpgsql
as
$$
declare
    task_id tid;
    task    text;
    x_num   uuid;
    x_den   uuid;
    y_num   uuid;
    y_den   uuid;
    declare t timestamptz := clock_timestamp();
begin

    perform pg_advisory_lock(42);

    select ctid, task_type, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id
      into task_id, task, x_num, x_den, y_num, y_den
    from task_queue
    order by created_at, priority
    for update skip locked
    limit 1;

    if task = 'system_indicators' then
        -- only 1 'system_indicators' task should be executed at a time to avoid unnecessary computations.
        -- lock all other tasks and release them in the end, without deleting them
        perform from task_queue
        where task_type = 'system_indicators'
        for update;
    end if;

    if task != 'correlations' then
        -- update_correlation() will lock some more rows in task_queue and then release the advisory lock
        perform pg_advisory_unlock(42);
    end if;

    if task_id is null then
        -- no tasks left, exit
        return;
    end if;

    raise notice '[%] start % task tid=% for %, %, %, %', pg_backend_pid(), task, task_id, x_num, x_den, y_num, y_den;

    case task
        when 'system_indicators' then
          call calculate_system_indicators(x_num);
        when 'quality' then
          call direct_quality_estimation(x_num, x_den);
        when 'stops' then
          call axis_stops_estimation(x_num, x_den);
        when 'analytics' then
          call bivariate_axis_analytics(x_num, x_den);
        when 'correlations' then
          call update_correlation(x_num, x_den, y_num, y_den);
        else
          raise notice 'unknown task type';
    end case;
    raise notice 'end % task tid=% time=%', task, task_id, date_trunc('second', clock_timestamp() - t);

    delete from task_queue where ctid = task_id;
end;
$$;
