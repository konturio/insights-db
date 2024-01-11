drop procedure if exists axis_stops_estimation;

create or replace procedure axis_stops_estimation(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
as
$$
begin
    -- TODO 17318: it's possible to skip some calculations if we check what's in bivariate_axis_overrides
    update bivariate_axis_v2 ba
    set min = c.min,
        p25 = c.p25,
        p75 = c.p75,
        max = c.max
    from (
        select
            floor(min(a.indicator_value / b.indicator_value::double precision)) as min,
            percentile_disc(0.33) within group (order by a.indicator_value / b.indicator_value::double precision)::double precision as p25,
            percentile_disc(0.66) within group (order by a.indicator_value / b.indicator_value::double precision)::double precision as p75,
            ceil(max(a.indicator_value / b.indicator_value::double precision)) as max
        from stat_h3_transposed a, stat_h3_transposed b
        where a.indicator_uuid = x_numerator_uuid
            and b.indicator_uuid = x_denominator_uuid
            and a.h3 = b.h3
            and a.indicator_value != 0
            and b.indicator_value != 0
    ) c
    where ba.numerator_uuid = x_numerator_uuid
        and ba.denominator_uuid = x_denominator_uuid;
end;
$$;
