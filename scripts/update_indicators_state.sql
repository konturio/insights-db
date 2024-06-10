with indicators_with_tasks as (
              select x_numerator_id from task_queue
    union all select x_denominator_id from task_queue
    union all select y_numerator_id from task_queue
    union all select y_denominator_id from task_queue
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
                    and internal_id not in (select * from indicators_with_tasks)
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
