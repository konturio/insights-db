do $$
declare
    rows_inserted integer;
    area_km2_uuid uuid;
    one_uuid uuid;
begin

create temp table new_tasks on commit drop as
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
        ((b.state = 'NEW' and a.state != 'OUTDATED')
           or (a.state = 'NEW' and b.state != 'OUTDATED'))
        and a.external_id != b.external_id
        and b.is_base
    on conflict (numerator_uuid, denominator_uuid)
        do nothing
    returning numerator_uuid, denominator_uuid
),
-- select distinct indicators included in axis
indicator_list as (
    select distinct numerator_uuid indicator_uuid from new_axis
    union
    select distinct denominator_uuid indicator_uuid from new_axis
),
-- create set of tasks for each new axis
tasks as (
    select priority, task_type, numerator_uuid, denominator_uuid
    from new_axis, (values
        (1., 'quality'),
        (3., 'analytics')
    ) tasks (priority, task_type)
)
select priority, task_type, numerator_uuid, denominator_uuid
from tasks
union all
-- calculate system_indicators for each h3 polygon of new indicators
select 0., 'system_indicators', indicator_uuid, null
from indicator_list
join bivariate_indicators_metadata on internal_id = indicator_uuid
where state = 'NEW';

-- retrieve system_indicators uuids
select internal_id into area_km2_uuid from bivariate_indicators_metadata
where owner = 'insights-db' and param_id = 'area_km2';

select internal_id into one_uuid from bivariate_indicators_metadata
where owner = 'insights-db' and param_id = 'one';

-- set lower priority for the tasks containing 'one' and 'area_km2',
-- so there's higher change to calculate these indicators in advance
update new_tasks
set priority = priority + .5
where task_type in ('quality', 'stops', 'analytics')
  and (numerator_uuid in (one_uuid, area_km2_uuid) or denominator_uuid in (one_uuid, area_km2_uuid));

insert into task_queue
    (priority, task_type, x_numerator_id, x_denominator_id)
select * from new_tasks
on conflict do nothing;

get diagnostics rows_inserted = row_count;
if rows_inserted > 0 then
    raise notice 'created % tasks', rows_inserted;
end if;

end $$;
