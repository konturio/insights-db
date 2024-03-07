with new_corr_axis as (
    select
        a.numerator_uuid x_numerator_uuid,      -- not base
        a.denominator_uuid x_denominator_uuid,  -- base
        b.numerator_uuid y_numerator_uuid,      -- may be base or not base
        b.denominator_uuid y_denominator_uuid   -- base
    from bivariate_axis_v2 a
        join bivariate_indicators_metadata ma
            on (a.numerator_uuid = ma.internal_id and not ma.is_base),
        bivariate_axis_v2 b
        join bivariate_indicators_metadata mb
            on (b.numerator_uuid = mb.internal_id)
    where
            a.quality > .5
        and b.quality > .5
        -- select unique 4-tuples of indicators.
        -- we'll calculate all permutations inside correlation task
        and (a.numerator_uuid < b.numerator_uuid or mb.is_base)
    except
    select x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id
    from bivariate_axis_correlation_v2
)

insert into task_queue
    (priority, task_type, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id)
select 4, 'correlations', x_numerator_uuid, x_denominator_uuid, y_numerator_uuid, y_denominator_uuid
from new_corr_axis;
-- TODO: on conflict do nothing - check unique constraint in task_queue (not exists currently)
