-- if there are rows with COPY IN PROGRESS that are not locked, these are failed upload attempts
delete from bivariate_indicators_metadata
where internal_id in (
    select internal_id from bivariate_indicators_metadata
    where state = 'COPY IN PROGRESS' and date < now() - interval '1m'
    for no key update skip locked)
returning 'delete', param_id, state, internal_id, 'ext.id=', external_id;
