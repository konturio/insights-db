drop procedure if exists apply_bivariate_axis_overrides;

create or replace procedure apply_bivariate_axis_overrides(x_numerator_uuid uuid, x_denominator_uuid uuid)
    language plpgsql
as
$$
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
        a.numerator_uuid = x_numerator_uuid and
        a.denominator_uuid = x_denominator_uuid and
        b.numerator_id = x_numerator_uuid and
        b.denominator_id = x_denominator_uuid;
end;
$$;
