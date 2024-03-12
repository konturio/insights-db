do $$
declare
    rows_inserted integer;
begin

with new_corr_axis as (
    select
        a.numerator_uuid x_numerator_uuid,      -- not base
        a.denominator_uuid x_denominator_uuid,  -- base
        b.numerator_uuid y_numerator_uuid,      -- may be base or not base
        b.denominator_uuid y_denominator_uuid   -- base
    from bivariate_axis_v2 a
        join bivariate_indicators_metadata m
            on (a.numerator_uuid = m.internal_id and not m.is_base),
        bivariate_axis_v2 b
    where
            a.quality > .5
        and b.quality > .5
        and a.numerator_uuid != b.numerator_uuid
    except
    select x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id
    from bivariate_axis_correlation_v2
)

insert into task_queue
    (priority, task_type, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id)
select
    case
        when x_denominator_uuid = y_numerator_uuid or x_denominator_uuid = y_denominator_uuid
        then 10 -- set low priority for tasks with repeating UUIDs: we can catch them while calculating other tasks
        else 4
    end,
    'correlations',
    x_numerator_uuid, x_denominator_uuid, y_numerator_uuid, y_denominator_uuid
from new_corr_axis
on conflict do nothing;

get diagnostics rows_inserted = row_count;
if rows_inserted > 0 then
    raise notice 'created % correlation tasks', rows_inserted;
end if;

end $$;
