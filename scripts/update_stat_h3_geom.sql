INSERT INTO stat_h3_geom (h3, resolution, geom)
SELECT t.h3,
       h3_get_resolution(t.h3),
       ST_Transform(h3_cell_to_boundary_geometry(t.h3), 3857) as geom
FROM (
    SELECT DISTINCT h3 FROM stat_h3_transposed
    where indicator_uuid in (select internal_id from bivariate_indicators_metadata where date > now() - interval '2 days')
      and h3_get_resolution(h3) <= 8
    order by h3
) t
WHERE NOT exists (SELECT FROM stat_h3_geom g WHERE g.h3 = t.h3)
