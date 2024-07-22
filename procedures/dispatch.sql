create or replace procedure dispatch()
    language plpgsql
    set work_mem = '10GB'
as
$$
declare
    task_id tid;
    task    text;
    rc      text;
    x_num   uuid;
    x_den   uuid;
    y_num   uuid;
    y_den   uuid;
    declare t timestamptz := clock_timestamp();
begin

    -- custom setting indicating the status of the task
    set task.rc = 0;

    -- task selection is exclusively blocked, so that each process can block more than 1 task if there's dependency
    perform pg_advisory_lock(42);

    set application_name to 'insights-db selecting task...';
    select ctid, task_type, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id
      into task_id, task, x_num, x_den, y_num, y_den
    from task_queue
    order by
        case task_type
            -- better to remove obsolete tasks earlier to reduce queue
            when 'remove_outdated_tasks' then 0
            -- correlations should be calculated after any other tasks
            when 'correlations' then 2
            else 1
        end,
        -- sort by timestamp so that older indicators are calculated earlier
        created_at,
        -- tasks are created in batches with the same created_at for axis, need additional sorting by priority
        priority
    for update skip locked
    limit 1;

    if task_id is null then
        -- no tasks left, exit
        return;
    end if;

    execute 'set application_name to ' || quote_literal('insights-db ' || coalesce(task, ''));
    raise notice '[%] start % task tid=% for %, %, %, %', pg_backend_pid(), task, task_id, x_num, x_den, y_num, y_den;

    if task = 'system_indicators' then
        -- only 1 'system_indicators' task should be executed at a time to avoid unnecessary computations.
        -- lock all other tasks and release them in the end, without deleting them
        perform from task_queue
        where task_type = 'system_indicators'
        for update skip locked;
        -- also lock analytics + transformations tasks, they should wait until system indicators are calculated
        perform from task_queue
        where task_type in ('analytics', 'transformations') and x_numerator_id = x_num
        for update skip locked;

    elsif task = 'remove_outdated_tasks' then
        -- meta-task to cleanup queue.
        -- x_num is outdated indicator - remove all tasks with it so no other process locks them
        perform pg_advisory_unlock(42);
        delete from task_queue
        where ctid in (
            select ctid from task_queue
            where task_type != 'remove_outdated_tasks' and x_numerator_id = x_num or x_denominator_id = x_num or y_numerator_id = x_num or y_denominator_id = x_num
            for update skip locked
        );
        delete from task_queue where ctid = task_id;
        raise notice '[%] end % task tid=% time=%', pg_backend_pid(), task, task_id, date_trunc('second', clock_timestamp() - t);
        return;

    elsif task = 'analytics' then
        -- lock transformations tasks, they should wait until analytics is calculated for layers
        perform from task_queue
        where task_type = 'transformations' and x_numerator_id = x_num and x_denominator_id = x_den
        for update skip locked;

    elsif task = 'check_new_indicator' then
        -- lock all tasks with new indicator until we perform some checks on it.
        -- tasks will be deleted if the check fails
        perform from task_queue
        where x_numerator_id = x_num or x_denominator_id = x_num or y_numerator_id = x_num or y_denominator_id = x_num
        for update skip locked;
    end if;

    if task != 'correlations' then
        -- update_correlation() will lock some more rows in task_queue and then release the advisory lock
        perform pg_advisory_unlock(42);
    end if;

    if indicator_inactive(x_num) or indicator_inactive(x_den) or indicator_inactive(y_num) or indicator_inactive(y_den) then
        delete from task_queue where ctid = task_id;
        return;
    end if;

    case task
        -- simultaneous threads with check_new_indicator block each other terribly. task disabled until locks are fixed
        -- when 'check_new_indicator' then
        --   call check_new_indicator(x_num);
        when 'system_indicators' then
          call calculate_system_indicators(x_num);
        when 'quality' then
          call direct_quality_estimation(x_num, x_den);
        when 'analytics' then
          call bivariate_axis_analytics(x_num, x_den);
        when 'transformations' then
          call calculate_transformations(x_num, x_den);
        when 'correlations' then
          call update_correlation(x_num, x_den, y_num, y_den);
        else
          raise notice 'unknown task type %', task;
    end case;

    execute 'show task.rc' into rc;
    if rc = '0' then
        delete from task_queue where ctid = task_id;
        raise notice '[%] end % task tid=% time=%', pg_backend_pid(), task, task_id, date_trunc('second', clock_timestamp() - t);
    end if;
    -- if rc != 0, task stays in db to recalculate it later
end;
$$;


create or replace function indicator_inactive(indicator_uuid uuid)
returns bool as $$
declare
    indicator_count int;
begin
    if indicator_uuid is null then
        return false;
    end if;

    select count(0) into indicator_count
    from bivariate_indicators_metadata
    where internal_id = indicator_uuid and state != 'OUTDATED';

    if indicator_count = 0 then
        raise notice '[%] cancelled task because indicator % no longer active', pg_backend_pid(), indicator_uuid;
    end if;

    return indicator_count = 0;
end;
$$ language plpgsql parallel safe;
