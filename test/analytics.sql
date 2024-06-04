\set x_numerator_uuid       '\'f6413d97-46ca-4925-a413-08824a6cbf33\'::uuid'
--\set x_numerator_uuid       '\'5a7736f6-1ff7-408b-b722-2a9d58ff733c\'::uuid'  -- seq scan on test db
\set x_denominator_uuid     '\'00000000-0000-0000-0000-000000000000\'::uuid'
--\set population             '\'f83a6214-4233-4b14-ae6a-3e26fc221edd\'::uuid' -- local
\set population             '\'f6413d97-46ca-4925-a413-08824a6cbf33\'::uuid'
\x

set work_mem='10GB';

--
-- common case: both indicators are from user
--

--explain verbose --(analyze, buffers, settings, verbose)
with statistics as (select h3_get_resolution(h3) as r,
                               jsonb_build_object(
                                       'sum', nullif(sum(m), 0),
                                       'min', min(m),
                                       'max', max(m),
                                       'mean', nullif(avg(m), 0),
                                       'stddev', nullif(stddev(m), 0)
                                   )
                                ||
                                jsonb_object(
                                    '{p33, p50, p66}',
                                    (percentile_cont('{.33, .5, .66}'::float[]) within group (order by m) filter (where p is not null) )::text[]
                                ) as stats
                        from (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = :x_numerator_uuid
            order by indicator_uuid, h3) numerator
        join (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = :x_denominator_uuid
            order by indicator_uuid, h3) denominator using(h3)
         left join (
            select h3, indicator_value p
            from stat_h3_transposed
            where indicator_uuid = :population
            order by indicator_uuid, h3) w using(h3),
        lateral (
            select numerator.indicator_value / nullif(denominator.indicator_value, 0) as m
        ) z
                        group by rollup (r)
                        order by r),
         stops as (select stats s from statistics where r is null),
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
select :x_numerator_uuid num, :x_denominator_uuid den, jsonb_pretty(j) stats, jsonb_pretty(s) stops
from upd, stops;
