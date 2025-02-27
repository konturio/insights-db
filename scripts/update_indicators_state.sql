create temp table indicators_with_tasks as
with uuids(indicator_uuid) as (
              select x_numerator_id   from task_queue where task_type != 'remove_outdated_tasks'
    union all select x_denominator_id from task_queue where task_type != 'remove_outdated_tasks'
    union all select y_numerator_id   from task_queue where task_type != 'remove_outdated_tasks'
    union all select y_denominator_id from task_queue where task_type != 'remove_outdated_tasks'
)
select distinct indicator_uuid from uuids where indicator_uuid is not null;

-- #18782: we can mark indicator as ready even if correlation task still not done.
-- also can set READY if there are tasks with this indicator as denominator
create temp table indicators_with_important_tasks as
select distinct x_numerator_id indicator_uuid from task_queue where task_type != 'correlations' and x_numerator_id is not null;

-- select all versions of indicators where at least 1 version is NEW and without pending tasks
with indicators_to_update as (
        select internal_id
        from bivariate_indicators_metadata
        where
                state != 'OUTDATED' and state != 'TMP CREATED' and state != 'COPY IN PROGRESS'
            and external_id in (
                select external_id
                from bivariate_indicators_metadata
                where
                        state = 'NEW'
                    and internal_id not in (select indicator_uuid from indicators_with_important_tasks)
            )
)

-- for selected indicator versions change state READY -> OUTDATED, NEW -> READY
update bivariate_indicators_metadata
set
    last_updated = current_timestamp at time zone 'utc',
    state = case state when 'READY' then 'OUTDATED' else 'READY' end
where
    internal_id in (select internal_id from indicators_to_update)
returning mk_log('status change'), param_id, state, internal_id, 'remaining_tasks='||(select count(0) from task_queue where x_numerator_id = internal_id) remaining_tasks;

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
    )
returning mk_log('status change'), param_id, state, internal_id, 'remaining_tasks='||(select count(0) from task_queue where x_numerator_id = internal_id) remaining_tasks;

-- tell dispatcher to remove tasks related to outdated indicators
insert into task_queue
    (priority, task_type, x_numerator_id)
select -1, 'remove_outdated_tasks', internal_id
from bivariate_indicators_metadata
join indicators_with_tasks on indicator_uuid = internal_id
where state = 'OUTDATED'
on conflict do nothing;
