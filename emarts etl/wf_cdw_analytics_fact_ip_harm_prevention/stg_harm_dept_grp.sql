select
    dept_key,
    rollup_nm,
    chop_dept_grp_nm,
    chop_dept_grp_abbr,
    intended_use_dept_grp_abbr,
    bed_care_dept_grp_abbr,
    unit_dept_grp_abbr,
    dept_align_dt,
    mstr_dept_grp_unit_key,
    always_count_for_census_ind,
    mstr_dept_grp_chop_key,
    row_number() over (partition by dept_key order by dept_align_dt) as depts_seq_num
from
    {{ref('fact_department_rollup')}}
