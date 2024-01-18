with dept_counts as (
     select
        department.dept_key,
        MAX(
            case when
                (
                    LOWER(fdrs.intended_use_dept_grp_abbr) in ('inpatient', 'overflow')
                -- MARIA TO ADD THESE DEPTS TO FACT_DEPARTMENT_ROLLUP_SUMMARY TABLE
                    or department.dept_nm in ( '3 CENTER', '7 EAST', '7 NORTH', '7 WEST', 'NICU EAST', 'NICU WEST', 'KOPH PACU', 'KOPH EMERGENCY DEP')
                )
                and department.dept_nm not in ( '4W-PACU', '7 WEST A PICU' )
                then 1
                else 0
            end
        ) as ip_unit_ind,
        COUNT(*)
    from
        {{source('cdw', 'visit_event')}} as visit_event
        inner join {{source('cdw', 'department')}} as department on department.dept_key = visit_event.dept_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_adt_event on dict_adt_event.dict_key = visit_event.dict_adt_event_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_event_subtype on dict_event_subtype.dict_key = visit_event.dict_event_subtype_key
        left join {{ref('fact_department_rollup_summary')}} as fdrs on fdrs.dept_key = department.dept_key
    where
        dict_adt_event.src_id in (1, 3) --'Admission'/'Transfer In'
        and dict_event_subtype.dict_nm != 'Canceled'
    group by
        department.dept_key
)
select
    d.dept_key,
    d.dept_id,
    d.dept_nm,
    COALESCE(dc.ip_unit_ind, 0) as ip_unit_ind
from
    {{source('cdw', 'department')}} as d
    left join dept_counts as dc on dc.dept_key = d.dept_key
