drop procedure if exists direct_quality_estimation;

create or replace procedure direct_quality_estimation(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
as
$$
begin
    with indicators as (select x_numerator_uuid   as indicator1,
                               x_denominator_uuid as indicator2),

         averages as (select h3_cell_to_parent(a.h3) as h3_parent,
                             a.indicator_uuid        as indicator_uuid,
                             avg(a.indicator_value)  as agg_value
                      from indicators i,
                           stat_h3_transposed a
                               inner join stat_h3_geom b
                                          on a.h3 = b.h3
                      where ((a.indicator_uuid = i.indicator1) or
                             (a.indicator_uuid = i.indicator2))
                        and (b.resolution between 1 and 5)
                        and (a.indicator_value != 0)
                      group by h3_parent, indicator_uuid),

         stat as (select b.h3_parent       as h3,
                         a.indicator_value as numerator_value,
                         b.agg_value       as numerator_agg_value,
                         c.indicator_value as denominator_value,
                         d.agg_value       as denominator_agg_value
                  from indicators i,
                       stat_h3_transposed a,
                       averages b,
                       stat_h3_transposed c,
                       averages d
                  where a.h3 = b.h3_parent
                    and a.h3 = c.h3
                    and a.h3 = d.h3_parent
                    and a.indicator_uuid = i.indicator1
                    and b.indicator_uuid = i.indicator1
                    and c.indicator_uuid = i.indicator2
                    and d.indicator_uuid = i.indicator2)
    update bivariate_axis_v2
    set quality =
            coalesce((select (1.0::float - avg(
                -- if we zoom in one step, will current zoom values be the same as next zoom values?
                        abs((s.numerator_value / nullif(s.denominator_value, 0)) -
                            (s.numerator_agg_value / nullif(s.denominator_agg_value, 0))) / nullif(
                                    abs(s.numerator_value / nullif(s.denominator_value, 0)) +
                                    abs(s.numerator_agg_value / nullif(s.denominator_agg_value, 0)), 0)))
                        -- does the denominator cover all of the cells where numerator is present?
                        * ((count(*) filter (where s.numerator_value != 0 and s.denominator_value != 0))::float
                    / nullif((count(*) filter (where s.numerator_value != 0)),0)) as quality
             from stat s), 0)
    where numerator_uuid = x_numerator_uuid
      and denominator_uuid = x_denominator_uuid;
end;
$$;
