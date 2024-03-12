do $$
declare
    rows_updated integer;
begin

with indicators_with_tasks as (
              select x_numerator_id from task_queue
    union all select x_denominator_id from task_queue
    union all select y_numerator_id from task_queue
    union all select y_denominator_id from task_queue
),
-- select all versions of indicators where at least 1 version is NEW and without pending tasks
-- and then select only 2 oldest versions of such indicators
indicators_to_update as (
    select
        internal_id
    from (
        select
            internal_id,
            row_number() over (partition by external_id order by date) num
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
    ) t
    where num <= 2
)

-- for selected indicator versions change state READY -> OUTDATED, NEW -> READY
update bivariate_indicators_metadata
set
    state = case state when 'READY' then 'OUTDATED' else 'READY' end
where
    internal_id in (select internal_id from indicators_to_update);

get diagnostics rows_updated = row_count;
if rows_updated > 0 then
    raise notice 'status changed for % indicators', rows_updated;
end if;

end $$;
