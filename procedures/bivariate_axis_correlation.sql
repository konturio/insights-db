drop function if exists correlate_bivariate_axes;
create or replace function correlate_bivariate_axes
(
    x_numerator uuid, x_denominator uuid, y_numerator uuid, y_denominator uuid
)
    returns float
as
$$
declare
    result float;
begin
    select corr(x_num.indicator_value / x_den.indicator_value, y_num.indicator_value / y_den.indicator_value)
    from stat_h3_transposed x_num
    join stat_h3_transposed x_den using(h3)
    join stat_h3_transposed y_num using(h3)
    join stat_h3_transposed y_den using(h3)
    where
        x_num.indicator_uuid = x_numerator and
        x_den.indicator_uuid = x_denominator and
        y_num.indicator_uuid = y_numerator and
        y_den.indicator_uuid = y_denominator and
        x_den.indicator_value != 0 and
        y_den.indicator_value != 0 and
        (x_num.indicator_value != 0 or
         y_num.indicator_value !=0)
    into result;
    return result;
end;
$$
language plpgsql stable parallel safe;


drop procedure if exists update_correlation;

create or replace procedure update_correlation(
    x_num uuid, x_den uuid, y_num uuid, y_den uuid)
    language plpgsql
as
$$
begin
    insert into bivariate_axis_correlation_v2 (
            correlation, quality, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id)
        select
            correlate_bivariate_axes(x_num, x_den, y_num, y_den) as correlation,
            1 - ((1 - x.quality) * (1 - y.quality)) as quality,
            x_num,
            x_den,
            y_num,
            y_den
        from
            bivariate_axis_v2 x, bivariate_axis_v2 y
        where
              x.numerator_uuid = x_num and
              x.denominator_uuid = x_den and
              y.numerator_uuid = y_num and
              y.denominator_uuid = y_den
    on conflict (x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id) do update
    set
        correlation = excluded.correlation,
        quality = excluded.quality;
end;
$$;
