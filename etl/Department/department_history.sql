select
    dept_key,
    dept_id as department_id,
    dept_nm as department_name,
    dept_abbr as department_abbr,
    min_dept_align_dt as valid_from_date,
    max_dept_align_dt as valid_to_date,
    loc_dept_grp_abbr as floor_location_abbr,
    loc_dept_grp_nm as floor_location_name,
    unit_dept_grp_abbr as care_area_abbr,
    unit_dept_grp_nm as care_area_name,
    bed_care_dept_grp_abbr as level_of_care_abbr,
    bed_care_dept_grp_nm as level_of_care_name,
    intended_use_dept_grp_abbr as intended_use_abbr,
    intended_use_dept_grp_nm as intended_use_name,
    chop_dept_grp_abbr as department_group_abbr,
    chop_dept_grp_nm as department_group_name,
    department_center_id,
    department_center_abbr,
    department_center_nm as department_center_name,
    hosp_count_for_census_ind as periop_or_census_department_ind,
    always_count_for_census_ind as count_for_census_ind
from
    {{source('cdw_analytics','fact_department_rollup_summary')}}
