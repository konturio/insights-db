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
begin
    select ctid, task_type, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id
      into task_id, task, x_num, x_den, y_num, y_den
    from task_queue
    order by priority
    for update skip locked
    limit 1;

    case
        when task = 'quality' then
          raise notice 'calculating quality';
          -- call direct_quality_estimation(x_num, x_den)
        when task = 'stops' then
          raise notice 'calculating stops';
          -- call axis_stops_estimation(x_num, x_den)
        -- TODO: add more types
        else
          raise notice 'unknown task type';
    end case;

    delete from task_queue where ctid = task_id;
end;
$$;
