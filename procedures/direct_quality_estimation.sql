drop procedure if exists direct_quality_estimation;

create or replace procedure direct_quality_estimation(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
as
$$
declare
    area_km2_uuid uuid;
    one_uuid uuid;
    den_value text;
    agg_den_value text;
    cte_sql text;
begin
    select internal_id into area_km2_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'area_km2';

    select internal_id into one_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'one';

    if x_numerator_uuid in (area_km2_uuid, one_uuid) then
        -- we don't need such axis at all
        return;
    end if;

    if x_denominator_uuid in (area_km2_uuid, one_uuid) then
        case x_denominator_uuid
        when area_km2_uuid then
            den_value := 'h3_get_hexagon_area_avg(h3_get_resolution(h3))';
            -- divide average child hexagon value by avg area of child hexagon (it has resolution +1)
            agg_den_value := 'h3_get_hexagon_area_avg(h3_get_resolution(h3)+1)';
        when one_uuid then
            den_value := '1.';
            agg_den_value := den_value;
        end case;
        cte_sql := '
        with averages_num as (
            select h3_cell_to_parent(h3) as h3_parent,
                   sum(indicator_value)/7  as agg_value
            from stat_h3_transposed
            where indicator_uuid = '|| quote_literal(x_numerator_uuid) ||'
              and h3_get_resolution(h3) between 1 and 5
            group by h3_parent
            order by h3_parent),
        stat as (
            select a.indicator_value as numerator_value,
                   '||den_value||'  as denominator_value,
                   a.indicator_value / nullif('||den_value||', 0) as actual_norm_value,
                   b.agg_value / nullif('||agg_den_value||', 0) as agg_norm_value
            from stat_h3_transposed a
            join averages_num b on (a.indicator_uuid = '|| quote_literal(x_numerator_uuid) ||' and a.h3 = b.h3_parent))';

    else
        -- 1. for hexagons of resolution 1..5 group values by common parent hexagon and calculate the average inside a parent
        cte_sql := '
        with averages_num as (
            select h3_cell_to_parent(h3) as h3_parent,
            sum(indicator_value)/7  as agg_value
            from stat_h3_transposed
            where indicator_uuid = '|| quote_literal(x_numerator_uuid) ||'
              and h3_get_resolution(h3) between 1 and 5
            group by h3_parent),
        averages_den as (
            select h3_cell_to_parent(h3) as h3_parent,
                   sum(indicator_value)/7  as agg_value
            from stat_h3_transposed
            where indicator_uuid = '|| quote_literal(x_denominator_uuid) ||'
              and h3_get_resolution(h3) between 1 and 5
            group by h3_parent),
        -- 2. join actual indicator values and average values from prev. step
        stat as (
            select a.indicator_value as numerator_value,
                   c.indicator_value as denominator_value,
                   a.indicator_value / nullif(c.indicator_value, 0) as actual_norm_value,
                   b.agg_value / nullif(d.agg_value, 0) as agg_norm_value
            from stat_h3_transposed a,
                 averages_num b,
                 stat_h3_transposed c,
                 averages_den d
            where a.h3 = b.h3_parent
              and a.h3 = c.h3
              and a.h3 = d.h3_parent
              and a.indicator_uuid = '|| quote_literal(x_numerator_uuid) ||'
              and c.indicator_uuid = '|| quote_literal(x_denominator_uuid) ||')';
    end if;

    -- 3. now compare aggregated values with the real values inside these hexagons.
    -- The greater the similarity among values, the higher the resulting quality.
    -- Also, less gaps in denominator = better quality.
    -- 0 means the worst quality, 1 -- the best.
    execute cte_sql || '
    update bivariate_axis_v2
    set quality =
            coalesce((select (1.0::float - avg(
                -- if we zoom in one step, will current zoom values be the same as next zoom values?
                abs(actual_norm_value - agg_norm_value) / nullif(abs(actual_norm_value) + abs(agg_norm_value), 0)))
                -- does the denominator cover all of the cells where numerator is present?
                * (count(*) filter (where numerator_value != 0 and denominator_value != 0))::float
                / nullif((count(*) filter (where numerator_value != 0)), 0) as quality
             from stat), 0)
    where numerator_uuid = '|| quote_literal(x_numerator_uuid) ||'
      and denominator_uuid = '|| quote_literal(x_denominator_uuid);

    if x_denominator_uuid in (area_km2_uuid, one_uuid) then
        update bivariate_indicators_metadata m
        set downscale = (
            select case when a.quality>b.quality then 'proportional' else 'equal' end
            from bivariate_axis_v2 a
            join bivariate_axis_v2 b on (
                a.numerator_uuid = x_numerator_uuid and
                a.numerator_uuid = b.numerator_uuid and
                a.denominator_uuid = area_km2_uuid and
                b.denominator_uuid = one_uuid
            )
        )
        where internal_id = x_numerator_uuid;
    end if;

end;
$$;
