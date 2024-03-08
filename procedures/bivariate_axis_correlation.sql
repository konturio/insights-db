drop procedure if exists update_correlation;

create or replace procedure update_correlation(
    A uuid, B uuid, C uuid, D uuid)
    language plpgsql
    set work_mem = '10GB'
as
$$
declare
    corr_sql text;
begin
    drop table if exists to_correlate;
    drop table if exists corr_results;
    drop table if exists result_col;

    create temp table to_correlate as
    with letters as (
        select unnest(ARRAY[A, B, C, D]) l
    ),
    permutations as (
        select distinct a.l x_num, b.l x_den, c.l y_num, d.l y_den
        from letters a, letters b, letters c, letters d
    )
    select ctid task_id, x_numerator_id x_num, x_denominator_id x_den, y_numerator_id y_num, y_denominator_id y_den
    from task_queue
    where
        task_type = 'correlations' and
        exists (
            select from permutations
            where x_numerator_id = x_num and x_denominator_id = x_den and y_numerator_id = y_num and y_denominator_id = y_den
        )
    for update skip locked;

    if not exists (select from to_correlate) then
        -- correlation for all tuples is calculated, nothing to do
        return;
    end if;

    -- compose corr() expressions for all to_correlate rows
    select into corr_sql string_agg(
        replace(replace(replace(replace(
            format('corr(%s/%s, %s/%s)', x_num, x_den, y_num, y_den),
            A::text, 'x_num.indicator_value'),
            B::text, 'x_den.indicator_value'),
            C::text, 'y_num.indicator_value'),
            D::text, 'y_den.indicator_value'),
        ',') from to_correlate;

    -- long part: select 4 indicators and run the actual correlation
    execute 'create temp table result_col as select unnest(array[' || corr_sql || ']) correlation'
    '   from stat_h3_transposed x_num
        join stat_h3_transposed x_den using(h3)
        join stat_h3_transposed y_num using(h3)
        join stat_h3_transposed y_den using(h3)
        where
            x_num.indicator_uuid = $1 and
            x_den.indicator_uuid = $2 and
            y_num.indicator_uuid = $3 and
            y_den.indicator_uuid = $4 and
            x_den.indicator_value != 0 and
            y_den.indicator_value != 0 and
            (x_num.indicator_value != 0 or
             y_num.indicator_value !=0)
    ' using A, B, C, D;

    -- join correlation results with the indicator uuids for which it was calculated
    create temp table corr_results as
    select x_num, x_den, y_num, y_den, correlation
    from (select *, row_number() over () as row_num from to_correlate)
    join (select *, row_number() over () as row_num from result_col)
--    using(row_num)
--    -- add symmetric values: corr(x,y) = corr(y,x)
--    union
--    select y_num, y_den, x_num, x_den, correlation
--    from (select *, row_number() over () as row_num from to_correlate)
--    join (select *, row_number() over () as row_num from result_col)
    using(row_num);

    -- apply results to the bivariate_axis_correlation_v2 table
    insert into bivariate_axis_correlation_v2 (
            correlation, quality, x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id)
    select
        correlation,
        1 - ((1 - x.quality) * (1 - y.quality)) as quality,
        x_num, x_den, y_num, y_den
    from corr_results
    join bivariate_axis_v2 x on (x.numerator_uuid = x_num and x.denominator_uuid = x_den)
    join bivariate_axis_v2 y on (y.numerator_uuid = y_num and y.denominator_uuid = y_den)
    on conflict (x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id) do update
    set
        correlation = excluded.correlation,
        quality = excluded.quality;
    
    delete from task_queue
    where ctid in (select task_id from to_correlate);
end;
$$;
