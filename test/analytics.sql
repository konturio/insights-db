\set x_numerator_uuid       '\'19dfc858-69c9-40a9-b8e9-8f5dcccb087c\'::uuid'
\set x_denominator_uuid     '\'eeeedddd-dddd-dddd-dddd-ddddddddeeee\'::uuid'
\x

--
-- case when denominator is 'one'
--
create temp view tmp_axis as
select h3, indicator_value / 1. m
from stat_h3_transposed
where indicator_uuid = :x_numerator_uuid;

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
                                '{p33, p50, p66}',
                                (percentile_cont('{.33, .5, .66}'::float[]) within group (order by m))::text[]
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
select :x_numerator_uuid num, 'one' den, jsonb_pretty(j) stats
from upd;

drop view tmp_axis;

--
-- common case: both indicators are from user
--

create temp view tmp_axis as
select h3, z.m
from (
    select h3, indicator_value
    from stat_h3_transposed
    where indicator_uuid = :x_numerator_uuid
    order by indicator_uuid, h3) numerator
join (
    select h3, indicator_value
    from stat_h3_transposed
    where indicator_uuid = :x_denominator_uuid
    order by indicator_uuid, h3) denominator using(h3),
lateral (
    select numerator.indicator_value / nullif(denominator.indicator_value, 0) as m
) z;

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
                                '{p33, p50, p66}',
                                (percentile_cont('{.33, .5, .66}'::float[]) within group (order by m))::text[]
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
select :x_numerator_uuid num, :x_denominator_uuid den, jsonb_pretty(j) stats
from upd;

drop view tmp_axis;
