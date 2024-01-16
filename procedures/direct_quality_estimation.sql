drop procedure if exists direct_quality_estimation;

create or replace procedure direct_quality_estimation(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
    set work_mem = '10GB'
as
$$
begin
    -- 1. for hexagons of resolution 1..5 group values by common parent hexagon and calculate the average inside a parent
    with averages as (select h3_cell_to_parent(h3) as h3_parent,
                             indicator_uuid        as indicator_uuid,
                             avg(indicator_value)  as agg_value
                      from stat_h3_transposed
                      where (indicator_uuid = x_numerator_uuid or
                             indicator_uuid = x_denominator_uuid)
                        and h3_get_resolution(h3) between 1 and 5
                        and indicator_value != 0
                      group by h3_parent, indicator_uuid),

         -- 2. join actual indicator values and average values from prev. step
         stat as (select a.indicator_value as numerator_value,
                         c.indicator_value as denominator_value,
                         a.indicator_value / nullif(c.indicator_value, 0) as actual_norm_value,
                         b.agg_value / nullif(d.agg_value, 0) as agg_norm_value
                  from stat_h3_transposed a,
                       averages b,
                       stat_h3_transposed c,
                       averages d
                  where a.h3 = b.h3_parent
                    and a.h3 = c.h3
                    and a.h3 = d.h3_parent
                    and a.indicator_uuid = x_numerator_uuid
                    and b.indicator_uuid = x_numerator_uuid
                    and c.indicator_uuid = x_denominator_uuid
                    and d.indicator_uuid = x_denominator_uuid)

    -- 3. now compare aggregated values with the real values inside these hexagons.
    -- The greater the similarity among values, the higher the resulting quality.
    -- Also, less gaps in denominator = better quality.
    -- 0 means the worst quality, 1 -- the best.
    update bivariate_axis_v2
    set quality =
            coalesce((select (1.0::float - avg(
                -- if we zoom in one step, will current zoom values be the same as next zoom values?
                abs(actual_norm_value - agg_norm_value) / nullif(abs(actual_norm_value) + abs(agg_norm_value), 0)))
                -- does the denominator cover all of the cells where numerator is present?
                * (count(*) filter (where numerator_value != 0 and denominator_value != 0))::float
                / nullif((count(*) filter (where numerator_value != 0)), 0) as quality
             from stat), 0)
    where numerator_uuid = x_numerator_uuid
      and denominator_uuid = x_denominator_uuid;
end;
$$;
