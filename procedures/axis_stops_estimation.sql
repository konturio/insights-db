drop procedure if exists axis_stops_estimation;

create or replace procedure axis_stops_estimation(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
    -- increased work mem helps planner to prefer bitmap index scan over seq scan
    set work_mem = '10GB'
as
$$
begin
    -- skip percentile calculations if there's overrides for min, p25, p75, max
    if exists (select from bivariate_axis_overrides
               where numerator_id = x_numerator_uuid
                 and denominator_id = x_denominator_uuid
                 and min is not null
                 and p25 is not null
                 and p75 is not null
                 and max is not null) then
        return;
    end if;

    update bivariate_axis_v2 ba
    set min = floor(c.percentiles[1]),
        p25 = c.percentiles[2]::double precision,
        p75 = c.percentiles[3]::double precision,
        max = ceil(c.percentiles[4])
    from (
        select
            percentile_disc(array[0, .33, .66, 1])
                within group (order by a.indicator_value / b.indicator_value::double precision) as percentiles
        -- order 2 sets by h3 so that merge join is preferable for the planner
        from (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = x_numerator_uuid and indicator_value != 0
            order by indicator_uuid, h3) a
        join (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = x_denominator_uuid and indicator_value != 0
            order by indicator_uuid, h3) b on (a.h3 = b.h3)
    ) c
    where ba.numerator_uuid = x_numerator_uuid
        and ba.denominator_uuid = x_denominator_uuid;
end;
$$;
