do $$
declare
    rows_inserted integer;
begin

-- compose bivariate axis to store tasks results
with new_axis as (
    insert into bivariate_axis_v2
        (numerator, denominator, numerator_uuid, denominator_uuid)
    select
        a.param_id numerator,
        b.param_id denominator,
        a.internal_id numerator_uuid,
        b.internal_id denominator_uuid
    from bivariate_indicators_metadata a
    join bivariate_indicators_metadata b on
            b.state != 'OUTDATED' and b.state != 'TMP CREATED' and b.state != 'COPY IN PROGRESS'
        and a.state != 'OUTDATED' and a.state != 'TMP CREATED' and a.state != 'COPY IN PROGRESS'
        and a.external_id != b.external_id
        and b.is_base
    on conflict (numerator_uuid, denominator_uuid)
        do nothing
    returning numerator_uuid, denominator_uuid
),
-- select distinct NEW indicators included in axis
indicator_list as (
    select distinct numerator_uuid indicator_uuid
    from new_axis
    join bivariate_indicators_metadata on (numerator_uuid = internal_id)
    where state = 'NEW'
),
-- create set of tasks for each new axis
tasks as (
    select priority, task_type, numerator_uuid, denominator_uuid
    -- priority is important for task dependencies: task that should wait for other tasks should have lower priority.
    -- example: 'transformations' (priority 4) requires 'analytics' result (priority 2), so 'transformations' has lower priority
    from new_axis, (values
        (1., 'quality'),
        (2., 'analytics'),
        (4., 'transformations')
    ) tasks (priority, task_type)
),
-- create tasks for each new indicator
single_indicator_tasks as (
    select priority, task_type, indicator_uuid, null::uuid
    from indicator_list,
    (values
        --(0., 'check_new_indicator'), -- task is disabled currently
        (0., 'system_indicators'), -- should have higher priority than analytics and transformations
        (5., 'max_resolution')
    ) tasks (priority, task_type)
)

insert into task_queue
    (priority, task_type, x_numerator_id, x_denominator_id)
select * from tasks
union all
select * from single_indicator_tasks
on conflict do nothing;

get diagnostics rows_inserted = row_count;
if rows_inserted > 0 then
    raise info using message = mk_log(format('created %s tasks', rows_inserted));
end if;

end $$;
