do $$
declare
    rows_inserted integer;
begin

-- create tasks for numerator/denominator pairs of indicators:
-- quality, stops, analytics
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
new_indicators as (
    select distinct numerator_uuid indicator_uuid from new_axis
    union
    select distinct denominator_uuid indicator_uuid from new_axis
)

insert into task_queue
    (priority, task_type, x_numerator_id, x_denominator_id)
select priority, task_type, numerator_uuid, denominator_uuid
from new_axis, (values
    (1, 'quality'),
    (2, 'stops'),
    (3, 'analytics')
) tasks (priority, task_type)
union all
select 0, 'system_indicators', indicator_uuid, null
from new_indicators
on conflict do nothing;

get diagnostics rows_inserted = row_count;
if rows_inserted > 0 then
    raise notice 'created % tasks', rows_inserted;
end if;

end $$;
