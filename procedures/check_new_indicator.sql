drop procedure if exists check_new_indicator;

create or replace procedure check_new_indicator(x_numerator_uuid uuid)
    language plpgsql
    set work_mem = '10GB'
as
$$
declare
    external_uuid uuid;
    upload_date timestamptz;
    prev_version uuid;
    slope double precision;
    intercept double precision;
    fill_ratio double precision;
begin

    select external_id, date into external_uuid, upload_date
    from bivariate_indicators_metadata
    where internal_id = x_numerator_uuid;

    -- find the oldest version of current indicator
    select internal_id into prev_version
    from bivariate_indicators_metadata
    where external_id = external_uuid and internal_id != x_numerator_uuid and date < upload_date
    order by date
    limit 1;

    if prev_version is null then
        raise notice 'no prev version found for %', x_numerator_uuid;
        return;
    end if;

    select
        regr_slope(a.indicator_value, nullif(b.indicator_value, 0)),
        regr_intercept(a.indicator_value, nullif(b.indicator_value, 0)),
        count(a.indicator_value)::double precision / count(b.indicator_value)
    into slope, intercept, fill_ratio
    from (select h3, indicator_value from stat_h3_transposed where indicator_uuid = x_numerator_uuid order by h3) a
    full join (select h3, indicator_value from stat_h3_transposed where indicator_uuid = prev_version order by h3) b using(h3);

    raise notice '% has prev version %: regr slope %, intercept %, fill_ratio %', x_numerator_uuid, prev_version, slope, intercept, fill_ratio;

    if slope between 0.999 and 1.001 and intercept between -0.01 and 0.01 and fill_ratio between 0.99 and 1.01 then
        raise notice 'discarding % indicator and all related tasks', prev_version;

        delete from task_queue
        where x_numerator_id = x_numerator_uuid or x_denominator_id = x_numerator_uuid or y_numerator_id = x_numerator_uuid or y_denominator_id = x_numerator_uuid;

        update bivariate_indicators_metadata
        set state = 'OUTDATED'
        where internal_id = x_numerator_uuid;
        raise notice 'discarding % indicator and all related tasks', prev_version;
    else
        raise notice 'indicator % stays in db', prev_version;
    end if;

end;
$$;
