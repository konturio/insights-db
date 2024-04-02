delete from stat_h3_transposed t
using bivariate_indicators_metadata b
where t.indicator_uuid = b.internal_id and state = 'OUTDATED';

delete from bivariate_overlays_v2 t
using bivariate_indicators_metadata b
where b.external_id in (x_numerator_id, x_denominator_id, y_numerator_id, y_denominator_id) and state = 'OUTDATED';

delete from bivariate_axis_overrides t
using bivariate_indicators_metadata b
where b.external_id in (numerator_id, denominator_id) and state = 'OUTDATED';

delete from bivariate_indicators_metadata
where state = 'OUTDATED'
returning 'delete', param_id, state, internal_id, 'ext.id=', external_id;
-- due to FK constraint outdated indicators will also be removed from
-- bivariate_axis_correlation_v2 & bivariate_axis_v2
