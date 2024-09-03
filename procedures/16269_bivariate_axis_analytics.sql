drop procedure if exists bivariate_axis_analytics;

create or replace procedure bivariate_axis_analytics(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
as
$$
declare
    skip_stops boolean := false;
    percentiles_keys text := '{p33, p50, p66}';
    percentiles text := '{.33, .5, .66}';
    area_km2_uuid uuid;
    one_uuid uuid;
    population_uuid uuid;
    population_sql text := '';
    tmp_axis_sql text;
begin
    select internal_id into area_km2_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'area_km2';

    select internal_id into one_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'one';

    select internal_id into population_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'population' order by date desc limit 1;

    if x_numerator_uuid in (area_km2_uuid, one_uuid) then
        -- we don't need such axis
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

    if not skip_stops and population_uuid != x_numerator_uuid then
        population_sql = ' left join (
            select h3, indicator_value p
            from stat_h3_transposed
            where indicator_uuid = '|| quote_literal(population_uuid) ||'
            order by indicator_uuid, h3) w using(h3)';
    end if;

    if x_denominator_uuid = one_uuid then
        tmp_axis_sql := '(select h3, indicator_value m from stat_h3_transposed
        where indicator_uuid = ' || quote_literal(x_numerator_uuid) || ')' || population_sql;
    else
        -- common case: select both indicators from db
        tmp_axis_sql := '(
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = '|| quote_literal(x_numerator_uuid) ||'
            order by indicator_uuid, h3) numerator
        join (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = '|| quote_literal(x_denominator_uuid) ||'
            order by indicator_uuid, h3) denominator using(h3)
        ' || population_sql || ',
        lateral (
            select numerator.indicator_value / nullif(denominator.indicator_value, 0) as m
        ) z';
    end if;

    execute 'with statistics as (select h3_get_resolution(h3) as r,
                               jsonb_build_object(
                                       ''sum'', nullif(sum(m), 0),
                                       ''min'', min(m),
                                       ''max'', max(m),
                                       ''mean'', nullif(avg(m), 0),
                                       ''stddev'', nullif(stddev(m), 0)
                                   )
                                || 
                                jsonb_object(
                                    '|| quote_literal(percentiles_keys) ||',
                                    (percentile_cont('|| quote_literal(percentiles) ||'::float[]) within group (order by m)' ||
                                        case when skip_stops or population_uuid = x_numerator_uuid then '' else ' filter (where p is not null) ' end || ')::text[]
                                ) as stats
                        from '|| tmp_axis_sql ||'
                        group by rollup (r)
                        order by r),
         all_res_stats as (select stats s from statistics where r is null),
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
    set sum_value = (j -> ''sum'' ->> 0)::double precision,
        sum_quality = (j -> ''sum'' ->> 1)::double precision,
        min_value      = (j -> ''min'' ->> 0)::double precision,
        min_quality    = (j -> ''min'' ->> 1)::double precision,
        max_value      = (j -> ''max'' ->> 0)::double precision,
        max_quality    = (j -> ''max'' ->> 1)::double precision,
        stddev_value   = (j -> ''stddev'' ->> 0)::double precision,
        stddev_quality = (j -> ''stddev'' ->> 1)::double precision,
        median_value   = (j -> ''p50'' ->> 0)::double precision,
        median_quality = (j -> ''p50'' ->> 1)::double precision,
        mean_value     = (j -> ''mean'' ->> 0)::double precision,
        mean_quality   = (j -> ''mean'' ->> 1)::double precision,
        mean_all_res   = (all_res_stats.s ->> ''mean'')::double precision,
        stddev_all_res = (all_res_stats.s ->> ''stddev'')::double precision ' ||
            case when skip_stops then ''
            else ',
        min = floor((all_res_stats.s ->> ''min'')::double precision),
        p25 = (all_res_stats.s ->> ''p33'')::double precision,
        p75 = (all_res_stats.s ->> ''p66'')::double precision,
        max = ceil((all_res_stats.s ->> ''max'')::double precision) '
            end ||
    'from upd, all_res_stats
    where ba.numerator_uuid = '|| quote_literal(x_numerator_uuid) ||'
      and ba.denominator_uuid = '|| quote_literal(x_denominator_uuid);
end;
$$;
