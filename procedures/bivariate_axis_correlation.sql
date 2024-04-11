drop function if exists get_correlation_sql;

create or replace function get_correlation_sql(A uuid, B uuid, C uuid, D uuid) returns text
as
$$
declare
    corr_sql text;
    sql text;
    area_km2_uuid uuid;
    one_uuid uuid;
begin
    select internal_id into area_km2_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'area_km2';

    select internal_id into one_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'one';

    -- compose corr() expressions for all to_correlate rows
    select into corr_sql string_agg(
        replace(replace(replace(replace(replace(replace(
            format('corr(%s/nullif(%s, 0), %s/nullif(%s, 0))', x_num, x_den, y_num, y_den),
            area_km2_uuid::text, 'h3_get_hexagon_area_avg(h3_get_resolution(h3))'),
            one_uuid::text, '1.'),
            A::text, 'x_num.indicator_value'),
            B::text, 'x_den.indicator_value'),
            C::text, 'y_num.indicator_value'),
            D::text, 'y_den.indicator_value'),
        ',') from to_correlate;

    sql := 'create temp table result_col on commit drop as select unnest(array[' || corr_sql || ']) correlation
        from (select h3, indicator_value from stat_h3_transposed where indicator_uuid = $1 order by h3) x_num';
    if B not in (one_uuid, area_km2_uuid) then
        sql := sql || ' join (select h3, indicator_value from stat_h3_transposed where indicator_uuid = $2 order by h3) x_den using(h3) ';
    end if;
    if C not in (one_uuid, area_km2_uuid) then
        sql := sql || ' join (select h3, indicator_value from stat_h3_transposed where indicator_uuid = $3 order by h3) y_num using(h3) ';
    end if;
    if D not in (one_uuid, area_km2_uuid) then
        sql := sql || ' join (select h3, indicator_value from stat_h3_transposed where indicator_uuid = $4 order by h3) y_den using(h3) ';
    end if;

    return sql;
end;
$$
    language plpgsql;


drop procedure if exists update_correlation;

create or replace procedure update_correlation(
    A uuid, B uuid, C uuid, D uuid)
    language plpgsql
    set work_mem = '10GB'
as
$$
declare
    task_count integer;
begin
    create temp table tasks on commit drop as
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

    -- unlock task_queue for other processes
    perform pg_advisory_unlock(42);

    create temp table to_correlate on commit drop as
    select x_num, x_den, y_num, y_den
    from tasks
    -- remove symmetric equations from calculation:
    except
    select y_num, y_den, x_num, x_den
    from tasks
    where y_num < x_num;

    if not exists (select from to_correlate) then
        -- correlation for all tuples is calculated, nothing to do
        return;
    end if;

    select count(0) from to_correlate into task_count;
    raise notice 'run correlation for % tuples', task_count;

    execute get_correlation_sql(A, B, C, D) using A, B, C, D;

    -- join correlation results with the indicator uuids for which it was calculated
    create temp table corr_results on commit drop as
    select x_num, x_den, y_num, y_den, correlation
    from (select *, row_number() over () as row_num from to_correlate)
    join (select *, row_number() over () as row_num from result_col)
    using(row_num)
    -- add symmetric values: corr(x,y) = corr(y,x)
    union
    select y_num, y_den, x_num, x_den, correlation
    from (select *, row_number() over () as row_num from to_correlate)
    join (select *, row_number() over () as row_num from result_col)
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
    where ctid in (select task_id from tasks);
end;
$$;
