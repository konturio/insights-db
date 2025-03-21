drop procedure if exists copy_indicator;

create or replace procedure copy_indicator(x_numerator_uuid uuid)
    language plpgsql
as
$$
declare
    rows_inserted bigint;
    tmp_table text;
    indicator_name text;
begin

    select 'tmp_stat_h3_' || upload_id, param_id into tmp_table, indicator_name
    from bivariate_indicators_metadata
    where internal_id = x_numerator_uuid;

    execute 'insert into stat_h3_transposed select distinct on (h3) * from "' || tmp_table || '" order by h3';

    get diagnostics rows_inserted = row_count;
    if rows_inserted > 0 then
        raise info using message = mk_log(format('inserted new indicator %s: %s rows', indicator_name, rows_inserted));
    else
        raise warning using message = mk_log(format('new indicator %s is empty!', indicator_name));
    end if;

    update bivariate_indicators_metadata
    set state = 'NEW'
    where internal_id = x_numerator_uuid;

    execute 'drop table "' || tmp_table || '"';

end;
$$;
