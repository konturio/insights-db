\set area_km2_uuid  '\'00000000-0000-0000-0000-000000000000\'::uuid'
\set one_uuid       '\'11111111-1111-1111-1111-111111111111\'::uuid'

insert into bivariate_indicators_metadata
(param_id, internal_id, external_id, param_label, owner, state, is_base,
 copyrights, direction, description, coverage, update_frequency, unit_id, is_public)
values
('one', :one_uuid, :one_uuid, '1', 'insights-db', 'NEW', true,
 '["Numbers © Muḥammad ibn Mūsā al-Khwārizmī"]'::json, '[["neutral"], ["neutral"]]'::jsonb,
 '', 'World', 'static', null, false),
('area_km2', :area_km2_uuid, :area_km2_uuid, 'Area', 'insights-db', 'NEW', true,
 '["Concept of areas © Brahmagupta, René Descartes"]'::json, '[["neutral"], ["neutral"]]'::jsonb,
 '', 'World', 'static', 'km2', false)
 on conflict do nothing
