do $$
declare
    rows_updated integer;
begin

update bivariate_axis_v2 a
set
    min         = coalesce(b.min, a.min),
    max         = coalesce(b.max, a.max),
    p25         = coalesce(b.p25, a.p25),
    p75         = coalesce(b.p75, a.p75),
    label       = coalesce(b.label, a.label),
    min_label   = coalesce(b.min_label, a.min_label),
    max_label   = coalesce(b.max_label, a.max_label),
    p25_label   = coalesce(b.p25_label, a.p25_label),
    p75_label   = coalesce(b.p75_label, a.p75_label)
from bivariate_axis_overrides b
where
    a.numerator_uuid = b.numerator_id and
    a.denominator_uuid = b.denominator_id;

get diagnostics rows_updated = row_count;
if rows_updated > 0 then
    raise notice 'status changed for % indicators', rows_updated;
end if;

end $$;
