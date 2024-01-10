with new_corr_axis as (
    insert into bivariate_axis_correlation_v2
        (x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id)
    select
        a.numerator_uuid x_numerator_uuid,
        a.denominator_uuid x_denominator_uuid,
        b.numerator_uuid y_numerator_uuid,
        b.denominator_uuid y_denominator_uuid
    from bivariate_axis_v2 a
        join bivariate_indicators_metadata m
            on (a.numerator_uuid = m.internal_id and not m.is_base),
        bivariate_axis_v2 b
    where
            a.quality > .5
        and b.quality > .5
        and a.numerator_uuid != b.numerator_uuid
    on conflict (x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id)
        do nothing
    returning x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id
)

insert into task_queue
    (priority, task_type, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id)
select 4, 'correlations', x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id
from new_corr_axis;
-- TODO: on conflict do nothing - check unique constraint in task_queue (not exists currently)
