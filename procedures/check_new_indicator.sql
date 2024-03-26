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
    correlation double precision;
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

    -- long part: select 4 indicators and run the actual correlation
    select corr(a.indicator_value, nullif(b.indicator_value, 0)) into correlation
    from (select h3, indicator_value from stat_h3_transposed where indicator_uuid = x_numerator_uuid order by h3) a
    join (select h3, indicator_value from stat_h3_transposed where indicator_uuid = prev_version order by h3) b using(h3);

    raise notice '% has prev version %, with correlation %', x_numerator_uuid, prev_version, correlation;

    if correlation > .999 then
        raise notice 'discarding % indicator and all related tasks', prev_version;

        delete from task_queue
        where x_numerator_id = x_numerator_uuid or x_denominator_id = x_numerator_uuid or y_numerator_id = x_numerator_uuid or y_denominator_id = x_numerator_uuid;

        update bivariate_indicators_metadata
        set state = 'OUTDATED'
        where internal_id = x_numerator_uuid;
    end if;

end;
$$;
