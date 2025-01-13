do $$
declare
    rows_inserted integer;
begin

insert into task_queue
    (priority, task_type, x_numerator_id)
select -3, 'copy', internal_id
from pg_tables
join bivariate_indicators_metadata
  on regexp_replace(tablename, '^tmp_stat_h3_', '')::uuid = upload_id
where schemaname = 'public' and tablename like 'tmp_stat_h3_%'
on conflict do nothing;

get diagnostics rows_inserted = row_count;
if rows_inserted > 0 then
    raise notice 'created % copy tasks', rows_inserted;
end if;

end $$;
