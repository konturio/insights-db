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
    transformations(transformation, points, n, final_stddev, final_mean) as (
        select 
                'no',
                array_agg(x),
                count(x) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                stddev(x) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                avg(x) filter (where x-new_mean between -3*new_stddev and 3*new_stddev)
        from hist_bounds, lateral unnest(p) x
        union all
        select
                'sqrt',
                array_agg(sign(x) * sqrt(abs(x))),
                count(x) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                stddev(sign(x) * sqrt(abs(x))) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                avg(sign(x) * sqrt(abs(x))) filter (where x-new_mean between -3*new_stddev and 3*new_stddev)
        from hist_bounds, lateral unnest(p) x
        union all
        select
                'cube_root',
                array_agg(pow(x, 1/3.)),
                count(x) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                stddev(pow(x, 1/3.)) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                avg(pow(x, 1/3.)) filter (where x-new_mean between -3*new_stddev and 3*new_stddev)
        from hist_bounds, lateral unnest(p) x
        union all
        select
                'log',
                array_agg(log10(x - layer_min + 1)),
                count(x) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                stddev(log10(x - layer_min + 1)) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                avg(log10(x - layer_min + 1)) filter (where x-new_mean between -3*new_stddev and 3*new_stddev)
        from hist_bounds, lateral unnest(p) x
        union all
        select
                'log_epsilon',
                array_agg(log10(x - layer_min + 2.220446049250313e-16::double precision)),
                count(x) filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                stddev(log10(x - layer_min + 2.220446049250313e-16::double precision))
                    filter (where x-new_mean between -3*new_stddev and 3*new_stddev),
                avg(log10(x - layer_min + 2.220446049250313e-16::double precision))
                    filter (where x-new_mean between -3*new_stddev and 3*new_stddev)
        from hist_bounds, lateral unnest(p) x
    ),
    stats(transformation, points, min, stddev, mean, lower_bound, upper_bound, skew) as (
        select
            transformation,
            array_agg(x),
            layer_min, -- value to subtract under log()
            final_stddev,
            final_mean,
            greatest(min(x), final_mean-3*final_stddev),
            least(max(x), final_mean+3*final_stddev),
            sum(pow((x-final_mean)/final_stddev, 3))*n::float/(n-1)/(n-2)
        from transformations, lateral unnest(transformations.points) x
        group by transformation, n, final_stddev, final_mean
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
