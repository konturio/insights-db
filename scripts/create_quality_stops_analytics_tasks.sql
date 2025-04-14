create temp table new_tasks as
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
tasks(priority, task_type, x_numerator_id, x_denominator_id) as (
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
single_indicator_tasks(priority, task_type, x_numerator_id, x_denominator_id) as (
    select priority, task_type, indicator_uuid, null::uuid
    from indicator_list,
    (values
        --(0., 'check_new_indicator'), -- task is disabled currently
        (0., 'system_indicators'), -- should have higher priority than analytics and transformations
        (5., 'max_resolution')
    ) tasks (priority, task_type)
),
created_tasks as (
    insert into task_queue
        (priority, task_type, x_numerator_id, x_denominator_id)
    select * from tasks
    union all
    select * from single_indicator_tasks
    on conflict do nothing
    returning priority, task_type, x_numerator_id, x_denominator_id
)
select * from created_tasks;

select mk_log('new tasks for indicator '||x_numerator_id), count(0) from new_tasks group by x_numerator_id;

do $$
declare
    partition text;
begin

for partition in
        with m as (select distinct x_numerator_id from new_tasks
            where exists (
                select from bivariate_indicators_metadata
                where internal_id = x_numerator_id and state = 'NEW'
            ))
        select distinct 'stat_h3_transposed_p'||i
        from generate_series(0,255) i, m
        where satisfies_hash_partition((SELECT oid FROM pg_class WHERE relname = 'stat_h3_transposed'), 256, i, m.x_numerator_id)
    loop
        execute 'analyze (verbose, skip_locked) ' || partition;
    end loop;

end $$;
