-- #18782: we can mark indicator as ready even if correlation task still not done
with indicators_with_tasks(indicator_uuid) as (
              select x_numerator_id     from task_queue where task_type != 'correlations'
    union all select x_denominator_id   from task_queue where task_type != 'correlations'
    union all select y_numerator_id     from task_queue where task_type != 'correlations'
    union all select y_denominator_id   from task_queue where task_type != 'correlations'
),
-- select all versions of indicators where at least 1 version is NEW and without pending tasks
indicators_to_update as (
        select internal_id
        from bivariate_indicators_metadata
        where
                state != 'OUTDATED'
            and external_id in (
                select external_id
                from bivariate_indicators_metadata
                where
                        state = 'NEW'
                    and internal_id not in (select distinct indicator_uuid from indicators_with_tasks where indicator_uuid is not null)
            )
)

-- for selected indicator versions change state READY -> OUTDATED, NEW -> READY
update bivariate_indicators_metadata
set
    state = case state when 'READY' then 'OUTDATED' else 'READY' end
where
    internal_id in (select internal_id from indicators_to_update)
returning 'status change', param_id, state, internal_id;

-- now there might be duplicated indicators in state READY, need to outdate oldest of them
update bivariate_indicators_metadata
set
    state = 'OUTDATED'
where
    state = 'READY' and
    internal_id not in (
        select distinct on(external_id) internal_id
        from bivariate_indicators_metadata
        where state = 'READY'
        order by external_id, date desc
    );
