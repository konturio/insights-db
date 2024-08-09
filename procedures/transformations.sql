drop procedure if exists calculate_transformations;

create or replace procedure calculate_transformations(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
as
$$
declare
    area_km2_uuid uuid;
    one_uuid uuid;
    layer_min double precision;
    layer_mean double precision;
    layer_stddev double precision;
    rc text;
begin
    select internal_id into area_km2_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'area_km2';

    select internal_id into one_uuid from bivariate_indicators_metadata
    where owner = 'disaster.ninja' and param_id = 'one';

    if x_numerator_uuid in (area_km2_uuid, one_uuid) then
        -- we don't need such axis
        return;
    end if;

    -- bivariate_axis_v2.min is floor() of minimal value of all hexagons for all resolutions.
    -- we take minimum at all resolutions so log(x-min+epsilon) doesn't break for any x for any resolution on front.
    -- bivariate_axis_v2.mean_value and stddev_value are from 8 resolution - as further calculations are performed on 8 res only
    select min, mean_value, stddev_value into layer_min, layer_mean, layer_stddev from bivariate_axis_v2
    where numerator_uuid = x_numerator_uuid and denominator_uuid = x_denominator_uuid;

    with hist_bounds(p, new_stddev, new_mean) as (
        select
            percentile_disc((select array_agg(x) from generate_series(0, 1, .01) x))
                within group (order by m),
            stddev(m) filter (where m between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
            avg(m) filter (where m between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev)
        from (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = x_numerator_uuid
              and h3_get_resolution(h3) = 8
            order by indicator_uuid, h3) numerator
        join (
            select h3, indicator_value
            from stat_h3_transposed
            where indicator_uuid = x_denominator_uuid
              and h3_get_resolution(h3) = 8
            order by indicator_uuid, h3) denominator using(h3),
        lateral (
            select numerator.indicator_value / nullif(denominator.indicator_value, 0) as m
        ) z
    ),
    range as (
        select
            x,
            sign(x) * sqrt(abs(x)) sqrt_x,
            sign(x) * pow(abs(x), 1/3.) cube_root_x,
            log10(x - layer_min + 1) log_x,
            log10(x - layer_min + 2.220446049250313e-16::double precision) log_epsilon_x,
            low,
            high
        from hist_bounds,
             lateral unnest(p) x,
             lateral (select new_mean - 3*new_stddev low, new_mean + 3*new_stddev high) z
    ),
    transformations(transformation, points, n, min_of_transformed, max_of_transformed, stddev_of_transformed, mean_of_transformed) as (
        select 
                'no',
                array_agg(x),
                count(x) filter (where x between low and high),
                min(x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                max(x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                stddev(x) filter (where x between low and high),
                avg(x) filter (where x between low and high)
        from range
        union all
        select
                'square_root',
                array_agg(sqrt_x),
                count(x) filter (where x between low and high),
                min(sqrt_x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                max(sqrt_x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                stddev(sqrt_x) filter (where x between low and high),
                avg(sqrt_x) filter (where x between low and high)
        from range
        union all
        select
                'cube_root',
                array_agg(cube_root_x),
                count(x) filter (where x between low and high),
                min(cube_root_x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                max(cube_root_x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                stddev(cube_root_x) filter (where x between low and high),
                avg(cube_root_x) filter (where x between low and high)
        from range
        union all
        select
                'log',
                array_agg(log_x),
                count(x) filter (where x between low and high),
                min(log_x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                max(log_x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                stddev(log_x) filter (where x between low and high),
                avg(log_x) filter (where x between low and high)
        from range
        union all
        select
                'log_epsilon',
                array_agg(log_epsilon_x),
                count(x) filter (where x between low and high),
                min(log_epsilon_x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                max(log_epsilon_x) filter (where x between layer_mean - 3.*layer_stddev and layer_mean + 3.*layer_stddev),
                stddev(log_epsilon_x) filter (where x between low and high),
                avg(log_epsilon_x) filter (where x between low and high)
        from range
    ),
    stats(transformation, points, stddev, mean, debug_max, lower_bound, upper_bound, skew) as (
        select
            transformation,
            array_agg(x),
            stddev_of_transformed,
            mean_of_transformed,
            max_of_transformed,
            greatest(min_of_transformed, mean_of_transformed-3*stddev_of_transformed),
            least(max_of_transformed, mean_of_transformed+3*stddev_of_transformed),
            n::float/(n-1)/(n-2) * sum(pow((x-mean_of_transformed)/(stddev_of_transformed+2.220446049250313e-16::double precision), 3)) filter (where x between min_of_transformed and max_of_transformed)
        from transformations, lateral unnest(transformations.points) x
        group by transformation, n, stddev_of_transformed, mean_of_transformed, min_of_transformed, max_of_transformed
    ),
    upd(j) as (select jsonb_agg(to_jsonb(t)) from stats t)
    update bivariate_axis_v2 ba
    set
        transformations = u.j,
        default_transform = t.default_transform - 'points'
    from
        upd u,
        (select to_jsonb(r) default_transform from stats r order by abs(skew) limit 1) t
    where ba.numerator_uuid = x_numerator_uuid
      and ba.denominator_uuid = x_denominator_uuid;

    exception
        when data_exception then
            if sqlerrm like '%logarithm of a negative number%' then
                raise notice 'cannot take logarithm of a negative number, need to recalculate layer_min';
                -- sometimes min_value in bivariate_axis_v2 gets outdated, so log(x-min(x)<0) may occur.
                -- try to repair it by recalculating analytics
                insert into task_queue (task_type, x_numerator_id, x_denominator_id, priority, created_at)
                values ('analytics', x_numerator_uuid, x_denominator_uuid, 2.0, now()-interval '1 day')
                on conflict do nothing;
                set task.rc = 1;
            else
                raise;
            end if;
end;
$$;
