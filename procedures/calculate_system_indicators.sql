drop procedure if exists calculate_system_indicators;

create or replace procedure calculate_system_indicators(x_numerator_uuid uuid)
    language plpgsql
    set work_mem = '10GB'
as
$$
declare
    area_km2_uuid uuid;
    one_uuid uuid;
begin

    select internal_id into area_km2_uuid
    from bivariate_indicators_metadata
    where owner = 'insights-db' and param_id = 'area_km2';

    select internal_id into one_uuid
    from bivariate_indicators_metadata
    where owner = 'insights-db' and param_id = 'one';

    if x_numerator_uuid = one_uuid or x_numerator_uuid = area_km2_uuid then
        -- skip system indicators
        return;
    end if;

    with missing_polygons as (
        select h3
        from stat_h3_transposed a
        where a.indicator_uuid = x_numerator_uuid
          and not exists (
            select from stat_h3_transposed b
            where b.indicator_uuid = one_uuid and b.h3 = a.h3
          )
    )
    insert into stat_h3_transposed (h3, indicator_uuid, indicator_value)
    select
        h3,
        one_uuid,
        1.
    from missing_polygons
    union all
    select
        h3,
        area_km2_uuid,
        ST_Area(h3_cell_to_boundary_geography(h3)) / 1000000.0
    from missing_polygons;

end;
$$;
