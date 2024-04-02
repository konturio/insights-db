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
join bivariate_indicators_metadata num on b.numerator_id = num.external_id
join bivariate_indicators_metadata den on b.denominator_id = den.external_id
where
    a.numerator_uuid = num.internal_id and
    a.denominator_uuid = den.internal_id;

get diagnostics rows_updated = row_count;
if rows_updated > 0 then
    raise notice 'overrides applied for % indicators', rows_updated;
end if;

end $$;
