select mk_log('removing outdated indicator from stat_h3_transposed: ' || :'indicator_id');

delete from stat_h3_transposed t
where t.indicator_uuid = :'indicator_id';

delete from bivariate_indicators_metadata
where internal_id = :'indicator_id'
returning mk_log('delete indicator'), param_id, state, internal_id, 'ext.id=', external_id;
-- due to FK constraint outdated indicators will also be removed from
-- bivariate_axis_correlation_v2 & bivariate_axis_v2

-- clean up obsolete overrides
delete from bivariate_axis_overrides o
where not exists (select from bivariate_indicators_metadata m where m.external_id = o.numerator_id)
   or not exists (select from bivariate_indicators_metadata m where m.external_id = o.denominator_id)
returning mk_log('delete indicator override'), numerator_id, denominator_id;
