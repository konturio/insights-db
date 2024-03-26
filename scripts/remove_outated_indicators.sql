delete from stat_h3_transposed t
using bivariate_indicators_metadata b
where t.indicator_uuid = b.internal_id and state = 'OUTDATED';

delete from bivariate_indicators_metadata
where state = 'OUTDATED'
returning 'delete', state, internal_id;
-- due to FK constraint outdated indicators will also be removed from
-- bivariate_axis_correlation_v2 & bivariate_axis_v2
