do
$$
declare
    -- uuids from test db
    x_numerator_uuid uuid := 'f6413d97-46ca-4925-a413-08824a6cbf33'; -- population
    x_denominator_uuid uuid := '11111111-1111-1111-1111-111111111111'; -- one
    --x_numerator_uuid uuid := 'f83a6214-4233-4b14-ae6a-3e26fc221edd';
    --x_denominator_uuid uuid := '1f5678ea-ae40-40e4-af2d-a263a1d673c7';
    layer_min double precision;
    layer_mean double precision;
    layer_stddev double precision;
begin
    select min_value, mean_value, stddev_value into layer_min, layer_mean, layer_stddev from bivariate_axis_v2
    where numerator_uuid = x_numerator_uuid and denominator_uuid = x_denominator_uuid;

    create temp table aaa as
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
    transformations(transformation, points, n, low, high, final_stddev, final_mean) as (
        select 
                'no',
                array_agg(x),
                count(x) filter (where x between low and high),
                min(x) filter (where x between low and high),
                max(x) filter (where x between low and high),
                stddev(x) filter (where x between low and high),
                avg(x) filter (where x between low and high)
        from range
        union all
        select
                'sqrt',
                array_agg(sqrt_x),
                count(x) filter (where x between low and high),
                min(sqrt_x) filter (where x between low and high),
                max(sqrt_x) filter (where x between low and high),
                stddev(sqrt_x) filter (where x between low and high),
                avg(sqrt_x) filter (where x between low and high)
        from range
        union all
        select
                'cube_root',
                array_agg(cube_root_x),
                count(x) filter (where x between low and high),
                min(cube_root_x) filter (where x between low and high),
                max(cube_root_x) filter (where x between low and high),
                stddev(cube_root_x) filter (where x between low and high),
                avg(cube_root_x) filter (where x between low and high)
        from range
        union all
        select
                'log',
                array_agg(log_x),
                count(x) filter (where x between low and high),
                min(log_x) filter (where x between low and high),
                max(log_x) filter (where x between low and high),
                stddev(log_x) filter (where x between low and high),
                avg(log_x) filter (where x between low and high)
        from range
        union all
        select
                'log_epsilon',
                array_agg(log_epsilon_x),
                count(x) filter (where x between low and high),
                min(log_epsilon_x) filter (where x between low and high),
                max(log_epsilon_x) filter (where x between low and high),
                stddev(log_epsilon_x) filter (where x between low and high),
                avg(log_epsilon_x) filter (where x between low and high)
        from range
    ),
    stats(transformation, points, min, stddev, mean, lower_bound, upper_bound, skew) as (
        select
            transformation,
            array_agg(round(x::numeric, 2)),
            layer_min, -- value to subtract under log()
            final_stddev,
            final_mean,
            greatest(low, final_mean-3*final_stddev),
            least(high, final_mean+3*final_stddev),
            n::float/(n-1)/(n-2) * sum(pow((x-final_mean)/(final_stddev+2.220446049250313e-16::double precision), 3)) filter (where x between low and high)
        from transformations, lateral unnest(transformations.points) x
        group by transformation, n, final_stddev, final_mean, low, high
    ),
    upd(j) as (select jsonb_agg(to_jsonb(t)) from stats t)
    select
        jsonb_pretty(u.j) transformations, 
        jsonb_pretty(t.default_transform - 'points') default_transform 
    from
        upd u,
        (select to_jsonb(r) default_transform from stats r order by abs(skew) limit 1) t;
end;
$$;
select * from aaa;
