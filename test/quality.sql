\set x_numerator_uuid       '\'19dfc858-69c9-40a9-b8e9-8f5dcccb087c\'::uuid'
\set x_denominator_uuid     '\'eeeedddd-dddd-dddd-dddd-ddddddddeeee\'::uuid'

--
-- common case: both indicators are from user
--
create temp view tmp_stat as
with averages_num as (
    select h3_cell_to_parent(h3) as h3_parent,
    avg(indicator_value)  as agg_value
    from stat_h3_transposed
    where indicator_uuid = :x_numerator_uuid
      and h3_get_resolution(h3) between 1 and 5
      and indicator_value != 0
    group by h3_parent),
averages_den as (
    select h3_cell_to_parent(h3) as h3_parent,
           avg(indicator_value)  as agg_value
    from stat_h3_transposed
    where indicator_uuid = :x_denominator_uuid
      and h3_get_resolution(h3) between 1 and 5
      and indicator_value != 0
    group by h3_parent)

-- 2. join actual indicator values and average values from prev. step
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
  and a.indicator_uuid = :x_numerator_uuid
  and c.indicator_uuid = :x_denominator_uuid;

select
    :x_numerator_uuid num,
    :x_denominator_uuid den,
    coalesce((select (1.0::float - avg(
        -- if we zoom in one step, will current zoom values be the same as next zoom values?
        abs(actual_norm_value - agg_norm_value) / nullif(abs(actual_norm_value) + abs(agg_norm_value), 0)))
        -- does the denominator cover all of the cells where numerator is present?
        * (count(*) filter (where numerator_value != 0 and denominator_value != 0))::float
        / nullif((count(*) filter (where numerator_value != 0)), 0) as quality
     from tmp_stat), 0) as quality;

drop view tmp_stat;

--
-- case when denominator is a system indicator: area_km2
--
\set x_denominator_uuid     '\'00000000-0000-0000-0000-000000000000\'::uuid'

create temp view tmp_stat as
with averages_num as (
    select h3_cell_to_parent(h3) as h3_parent,
           avg(indicator_value)  as agg_value
    from stat_h3_transposed
    where indicator_uuid = :x_numerator_uuid
      and h3_get_resolution(h3) between 1 and 5
      and indicator_value != 0
    group by h3_parent
    order by h3_parent)
select a.indicator_value as numerator_value,
       h3_cell_area(h3)  as denominator_value,
       a.indicator_value / nullif(h3_cell_area(h3), 0) as actual_norm_value,
       b.agg_value / nullif(h3_cell_area(h3), 0) as agg_norm_value
from stat_h3_transposed a
join averages_num b on (a.indicator_uuid = :x_numerator_uuid and a.h3 = b.h3_parent);

select
    :x_numerator_uuid num,
    :x_denominator_uuid den,
    coalesce((select (1.0::float - avg(
        -- if we zoom in one step, will current zoom values be the same as next zoom values?
        abs(actual_norm_value - agg_norm_value) / nullif(abs(actual_norm_value) + abs(agg_norm_value), 0)))
        -- does the denominator cover all of the cells where numerator is present?
        * (count(*) filter (where numerator_value != 0 and denominator_value != 0))::float
        / nullif((count(*) filter (where numerator_value != 0)), 0) as quality
     from tmp_stat), 0) as quality;

drop view tmp_stat;
