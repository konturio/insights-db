drop procedure if exists copy_indicator;

create or replace procedure copy_indicator(x_numerator_uuid uuid)
    language plpgsql
as
$$
declare
    rows_inserted integer;
    tmp_table text;
begin

    select 'tmp_stat_h3_' || upload_id into tmp_table
    from bivariate_indicators_metadata
    where internal_id = x_numerator_uuid;

    execute 'insert into stat_h3_transposed select distinct on (h3) * from "' || tmp_table || '" order by h3';

    get diagnostics rows_inserted = row_count;
    if rows_inserted > 0 then
        raise notice 'inserted % rows', rows_inserted;
    end if;

    update bivariate_indicators_metadata
    set state = 'NEW'
    where internal_id = x_numerator_uuid;

    execute 'drop table "' || tmp_table || '"';

end;
$$;
