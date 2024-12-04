drop procedure if exists find_max_resolution;

create or replace procedure find_max_resolution(x_numerator_uuid uuid)
    language plpgsql
as
$$
declare
    area_km2_uuid uuid;
    one_uuid uuid;
begin

    select internal_id into area_km2_uuid
    from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'area_km2';

    select internal_id into one_uuid
    from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'one';

    if x_numerator_uuid = one_uuid or x_numerator_uuid = area_km2_uuid then
        -- skip system indicators
        return;
    end if;

    update bivariate_indicators_metadata
    set max_res = (
        select max(h3_get_resolution(h3))
        from stat_h3_transposed
        where indicator_uuid = x_numerator_uuid
    )
    where internal_id = x_numerator_uuid;

end;
$$;
