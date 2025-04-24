drop procedure if exists find_max_resolution;

create or replace procedure find_max_resolution(x_numerator_uuid uuid)
    language plpgsql
as
$$
declare
    area_km2_uuid uuid;
    one_uuid uuid;
    global_max_res integer;
    coverage_max_res integer;
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

    with resolution_counts as (
        select
            h3_get_resolution(h3) as res,
            count(9) as cnt
        from stat_h3_transposed
        where indicator_uuid = x_numerator_uuid
        group by 1
    ),
    global as (
        select max(res) as r from resolution_counts
    ),
    coverage as (
        select max(res) as r from resolution_counts where cnt <= 1000
    )
    select global.r, coverage.r
    into global_max_res, coverage_max_res
    from global, coverage;

    update bivariate_indicators_metadata
    set
        max_res = global_max_res,
        coverage_polygon = (
            select ST_Transform(ST_Union(h3_cell_to_boundary_geometry(h3)), 4326)
            from stat_h3_transposed
            where indicator_uuid = x_numerator_uuid
              and h3_get_resolution(h3) = coverage_max_res
    )
    where internal_id = x_numerator_uuid;

end;
$$;
