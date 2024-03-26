drop procedure if exists bivariate_axis_analytics;

create or replace procedure bivariate_axis_analytics(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
as
$$
declare
    skip_stops boolean := false;
    percentiles_keys text[] := '{p33, p50, p66}';
    percentiles float[] := '{.33, .5, .66}';
    area_km2_uuid uuid;
    one_uuid uuid;
    indicator_id uuid;
    num_value text;
    den_value text;
begin
    select internal_id into area_km2_uuid from bivariate_indicators_metadata
    where owner = 'insights-db' and param_id = 'area_km2';

    select internal_id into one_uuid from bivariate_indicators_metadata
    where owner = 'insights-db' and param_id = 'one';

    if x_numerator_uuid in (area_km2_uuid, one_uuid) and x_denominator_uuid in (area_km2_uuid, one_uuid) then
        -- shortcut if both indicators are system-
        -- TODO wht do I do
        return;
    end if;

    -- skip stops calculations if there's overrides for min, p25, p75, max
    if exists (select from bivariate_axis_overrides
               where numerator_id = x_numerator_uuid
                 and denominator_id = x_denominator_uuid
                 and min is not null
                 and p25 is not null
                 and p75 is not null
                 and max is not null) then
        skip_stops = true;
        percentiles_keys = '{p50}';
        percentiles = '{.5}';
    end if;

    if x_numerator_uuid in (area_km2_uuid, one_uuid) or x_denominator_uuid in (area_km2_uuid, one_uuid) then
        case x_numerator_uuid
        when area_km2_uuid then
            num_value := 'h3_cell_area(h3)';
        when one_uuid then
            num_value := '1.';
        else
            num_value := 'indicator_value';
            indicator_id := x_numerator_uuid;
        end case;
        case x_denominator_uuid
        when area_km2_uuid then
            den_value := 'h3_cell_area(h3)';
        when one_uuid then
            den_value := '1.';
        else
            den_value := 'indicator_value';
            indicator_id := x_denominator_uuid;
        end case;

        execute 'create temp view tmp_axis as
        select h3, '|| num_value ||' / nullif('|| den_value ||', 0) m
        from stat_h3_transposed
        where indicator_uuid = ' || quote_literal(indicator_id);
    else
        -- common case: select both indicators from db
        execute 'create temp view tmp_axis as
        select h3, z.m
        from (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = '|| quote_literal(x_numerator_uuid) ||'
            order by indicator_uuid, h3) numerator
        join (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = '|| quote_literal(x_denominator_uuid) ||'
            order by indicator_uuid, h3) denominator using(h3),
        lateral (
            select numerator.indicator_value / nullif(denominator.indicator_value, 0) as m
        ) z';
    end if;

    with statistics as (select h3_get_resolution(h3) as r,
                               jsonb_build_object(
                                       'sum', nullif(sum(m), 0),
                                       'min', min(m) filter (where m != 0),
                                       'max', max(m) filter (where m != 0),
                                       'mean', nullif(avg(m), 0),
                                       'stddev', nullif(stddev(m), 0)
                                   )
                                || 
                                jsonb_object(
                                    percentiles_keys,
                                    (percentile_cont(percentiles) within group (order by m))::text[]
                                ) as stats
                        from tmp_axis
                        group by r
                        order by r),
         quality as (select key,
                            avg(value) filter (where r = 8) as value,
                            case
                                when avg(value) filter (where r = 8) is null
                                    then null
                                when (nullif(max(value), 0) / nullif(min(value), 0)) > 0
                                    then log10(nullif(max(value), 0) / nullif(min(value), 0))
                                else log10((nullif(max(value), 0) - nullif(min(value), 0)) /
                                           least(abs(nullif(min(value), 0)), abs(nullif(max(value), 0))))
                                end                            quality
                     from statistics x,
                          jsonb_object_keys(stats) key,
                          lateral (values ((stats ->> key)::double precision)) as v(value)
                     group by key),
         upd as (select jsonb_object_agg(key, array [value, quality]) j
                 from quality)
    update bivariate_axis_v2 ba
    set sum_value = (j -> 'sum' ->> 0)::double precision,
        sum_quality = (j -> 'sum' ->> 1)::double precision,
        min_value      = (j -> 'min' ->> 0)::double precision,
        min_quality    = (j -> 'min' ->> 1)::double precision,
        max_value      = (j -> 'max' ->> 0)::double precision,
        max_quality    = (j -> 'max' ->> 1)::double precision,
        stddev_value   = (j -> 'stddev' ->> 0)::double precision,
        stddev_quality = (j -> 'stddev' ->> 1)::double precision,
        median_value   = (j -> 'p50' ->> 0)::double precision,
        median_quality = (j -> 'p50' ->> 1)::double precision,
        mean_value     = (j -> 'mean' ->> 0)::double precision,
        mean_quality   = (j -> 'mean' ->> 1)::double precision,
        -- stops:
        min = case when skip_stops then min else floor((j -> 'min' ->> 0)::double precision) end,
        p25 = case when skip_stops then p25 else (j -> 'p33' ->> 0)::double precision end,
        p75 = case when skip_stops then p75 else (j -> 'p66' ->> 0)::double precision end,
        max = case when skip_stops then max else ceil((j -> 'max' ->> 0)::double precision) end
    from upd
    where ba.numerator_uuid = x_numerator_uuid
      and ba.denominator_uuid = x_denominator_uuid;
end;
$$;
