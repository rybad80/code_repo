select
    department.dept_key,
    mstr_dept_grp_nhsn_key,
    mstr_dept_grp_chop_key,
    mstr_dept_grp_nursing_key,
    mstr_dept_grp_bed_care_key,
    mstr_dept_grp_nrcp_key,
    mstr_dept_grp_intended_use_key
from
    {{ref('department_group_supplemental')}} as department_group_supplemental
    inner join {{source('cdw','department')}} as department
        on department.dept_id = department_group_supplemental.dept_id
    left join {{ref('master_department_group_supplemental')}} as chop
        on chop.mstr_dept_grp_key = department_group_supplemental.mstr_dept_grp_chop_key
        and chop.dept_grp_type = 'CHOP'
    left join {{ref('master_department_group_supplemental')}} as bed_care
        on bed_care.mstr_dept_grp_key = department_group_supplemental.mstr_dept_grp_bed_care_key
        and bed_care.dept_grp_type = 'BED_CARE_LVL'
    left join {{ref('master_department_group_supplemental')}} as intended_use
        on intended_use.mstr_dept_grp_key = department_group_supplemental.mstr_dept_grp_intended_use_key
        and intended_use.dept_grp_type = 'INTENDED_USE'
where
    department.dept_key not in (select dept_key from {{ref('fact_department_rollup_summary')}})

union all

select
    dept_key,
    0 as mstr_dept_grp_nhsn_key,
    mstr_dept_grp_chop_key,
    0 as mstr_dept_grp_nursing_key,
    mstr_dept_grp_bed_care_key,
    0 as mstr_dept_grp_nrcp_key,
    mstr_dept_grp_intended_use_key
from
    {{ref('fact_department_rollup_summary')}}
where
    max_dept_align_dt = current_date
