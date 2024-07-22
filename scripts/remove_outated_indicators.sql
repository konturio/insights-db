set synchronous_commit to off;

delete from stat_h3_transposed t
using bivariate_indicators_metadata b
where t.indicator_uuid = b.internal_id and state = 'OUTDATED';

delete from bivariate_indicators_metadata
where state = 'OUTDATED'
returning 'delete', param_id, state, internal_id, 'ext.id=', external_id;
-- due to FK constraint outdated indicators will also be removed from
-- bivariate_axis_correlation_v2 & bivariate_axis_v2

-- clean up obsolete overrides
delete from bivariate_axis_overrides o
where not exists (select from bivariate_indicators_metadata m where m.external_id = o.numerator_id)
   or not exists (select from bivariate_indicators_metadata m where m.external_id = o.denominator_id);

-- clean up obsolete overlays
delete from bivariate_overlays_v2 o
where not exists (select from bivariate_indicators_metadata m where m.external_id = o.x_numerator_id)
   or not exists (select from bivariate_indicators_metadata m where m.external_id = o.x_denominator_id)
   or not exists (select from bivariate_indicators_metadata m where m.external_id = o.y_numerator_id)
   or not exists (select from bivariate_indicators_metadata m where m.external_id = o.y_denominator_id);
