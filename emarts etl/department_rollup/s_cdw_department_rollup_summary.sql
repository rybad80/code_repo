select
    s_cdw_snapshot_trange_join.dept_id,
    department.dept_key,
    s_cdw_snapshot_trange_join.dept_nm,
    s_cdw_snapshot_trange_join.dept_abbr,
    s_cdw_snapshot_trange_join.intended_use_dept_grp_nm,
    s_cdw_snapshot_trange_join.intended_use_dept_grp_abbr,
    s_cdw_snapshot_trange_join.loc_dept_grp_nm,
    s_cdw_snapshot_trange_join.loc_dept_grp_abbr,
    case -- remove abbr in parentheses from full care area name
        when
            care_area_4_dept_grp_nm is not null
        then
            care_area_1_dept_grp_nm || '/'
            || care_area_2_dept_grp_nm || '/'
            || care_area_3_dept_grp_nm || '/'
            || care_area_4_dept_grp_nm
        when
            care_area_3_dept_grp_nm is not null
        then
            care_area_1_dept_grp_nm || '/'
            || care_area_2_dept_grp_nm || '/'
            || care_area_3_dept_grp_nm
        when
            care_area_2_dept_grp_nm is not null
        then
            care_area_1_dept_grp_nm || '/'
            || care_area_2_dept_grp_nm
        else
            care_area_1_dept_grp_nm
    end as unit_dept_grp_nm,
    case
        when
            care_area_4_dept_grp_abbr is not null
        then
            care_area_1_dept_grp_abbr || '/'
            || care_area_2_dept_grp_abbr || '/'
            || care_area_3_dept_grp_abbr || '/'
            || care_area_4_dept_grp_abbr
        when
            care_area_3_dept_grp_abbr is not null
        then
            care_area_1_dept_grp_abbr || '/'
            || care_area_2_dept_grp_abbr || '/'
            || care_area_3_dept_grp_abbr
        when
            care_area_2_dept_grp_abbr is not null
        then
            care_area_1_dept_grp_abbr || '/'
            || care_area_2_dept_grp_abbr
        else
            care_area_1_dept_grp_abbr
    end as unit_dept_grp_abbr,
    s_cdw_snapshot_trange_join.chop_dept_grp_nm,
    s_cdw_snapshot_trange_join.chop_dept_grp_abbr,
    s_cdw_snapshot_trange_join.bed_care_dept_grp_id,
    s_cdw_snapshot_trange_join.bed_care_dept_grp_nm,
    s_cdw_snapshot_trange_join.bed_care_dept_grp_abbr,
    zc_center.internal_id as department_center_id,
    zc_center.name as department_center_nm,
    zc_center.abbr as department_center_abbr,
    case
        when -- 'inpatient', 'observation unit', 'overflow', 'perioperative'
            intended_use_dept_grp_id in (1004, 1005, 1007, 1008)
        then
            1
        else
            0
    end as hosp_count_for_census_ind, -- more clear name for future would be evaluate_for_census_ind
    case
        when -- 'inpatient', 'observation unit', 'overflow'
            intended_use_dept_grp_id in (1004, 1005, 1007)
        then
            1
        else
            0
    end as always_count_for_census_ind,
    case
        when
            hosp_count_for_census_ind = 1
            and bed_care_dept_grp_id not in (103, 113) -- REHAB, NOT APPLICABLE
        then
            1
        else
            0
    end as acute_inpatient_ind,
    case
        when
            hosp_count_for_census_ind = 1
            and bed_care_dept_grp_id not in (106, 113) -- SDU, NOT APPLICABLE
        then
            1
        else
            0
    end as pediatric_ind,
    s_cdw_snapshot_trange_join.dbt_scd_id,
    cast(s_cdw_snapshot_trange_join.min_dept_align_dt as date) as min_dept_align_dt,
    case
        when
            s_cdw_snapshot_trange_join.max_dept_align_dt = {{ var('open_end_date') }}
        then
            current_date
        else
            s_cdw_snapshot_trange_join.max_dept_align_dt - 1
    end as max_dept_align_dt
from
    {{ref('s_cdw_snapshot_trange_join')}} as s_cdw_snapshot_trange_join
    inner join {{source('cdw','department')}} as department
        on department.dept_id = s_cdw_snapshot_trange_join.dept_id
    left join {{source('clarity_ods','zc_center')}} as zc_center
        on zc_center.internal_id = s_cdw_snapshot_trange_join.center_c
