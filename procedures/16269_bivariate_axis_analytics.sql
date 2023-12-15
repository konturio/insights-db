drop procedure if exists bivariate_axis_analytics;

create or replace procedure bivariate_axis_analytics(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
as
$$
begin
    with statistics as (select h3_get_resolution(numerator.h3) as r,
                               jsonb_build_object(
                                       'sum', nullif(sum(z.m), 0),
                                       'min', min(z.m) filter (where z.m != 0),
                                       'max', max(z.m) filter (where z.m != 0),
                                       'mean', nullif(avg(z.m), 0),
                                       'stddev', nullif(stddev(z.m), 0),
                                       'median', nullif(percentile_cont(0.5) within group (order by z.m), 0)
                                   )                           as stats
                        from stat_h3_transposed AS numerator
                                 join stat_h3_transposed as denominator
                                      on numerator.indicator_uuid = x_numerator_uuid
                                          and denominator.indicator_uuid = x_denominator_uuid
                                          and numerator.h3 = denominator.h3,
                             lateral (
                                 select numerator.indicator_value / nullif(denominator.indicator_value, 0) as m
                                 ) z
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
        median_value   = (j -> 'median' ->> 0)::double precision,
        median_quality = (j -> 'median' ->> 1)::double precision,
        mean_value     = (j -> 'mean' ->> 0)::double precision,
        mean_quality   = (j -> 'mean' ->> 1)::double precision
    from upd
    where ba.numerator_uuid = x_numerator_uuid
      and ba.denominator_uuid = x_denominator_uuid;
end;
$$;
