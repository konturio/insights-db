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
    select ctid, task_type, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id
      into task_id, task, x_num, x_den, y_num, y_den
    from task_queue
    order by priority
    for update skip locked
    limit 1;

    if task_id is null then
        -- no tasks left, exit
        return;
    end if;

    raise notice 'start % task tid=% for %, %, %, %', task, task_id, x_num, x_den, y_num, y_den;
    case task
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
