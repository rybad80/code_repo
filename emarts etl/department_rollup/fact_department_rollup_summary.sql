with merge_new_and_active as (
    select
        s_cdw_department_rollup_summary.dept_id,
        cast(
            coalesce(
            s_cdw_department_rollup_summary_supplemental.min_dept_align_dt,
            s_cdw_department_rollup_summary.min_dept_align_dt
            )
        as date) as  min_dept_align_dt,
        cast(s_cdw_department_rollup_summary.max_dept_align_dt as date) as max_dept_align_dt
    from    
        {{ref('s_cdw_department_rollup_summary')}} as s_cdw_department_rollup_summary
        left join {{ref('s_cdw_department_rollup_summary_supplemental')}} as  s_cdw_department_rollup_summary_supplemental
            on s_cdw_department_rollup_summary_supplemental.dept_id = s_cdw_department_rollup_summary.dept_id
            and s_cdw_department_rollup_summary_supplemental.max_dept_align_dt = s_cdw_department_rollup_summary.min_dept_align_dt
    where
        s_cdw_department_rollup_summary.intended_use_dept_grp_nm is not null
),

union_set as(
    select
        s_cdw_department_rollup_summary.dept_id,
        dept_nm,
        merge_new_and_active.min_dept_align_dt,
        merge_new_and_active.max_dept_align_dt,
        cast(dept_abbr as varchar(50)) as dept_abbr,
        loc_dept_grp_abbr,
        loc_dept_grp_nm,
        unit_dept_grp_abbr,
        unit_dept_grp_nm,
        cast(0 as int) as unit_temp_ind,
        cast(acute_inpatient_ind as byteint) as loc_acute_inpatient_ind, --should be acute_inpatient_ind loc is hold over
        cast(pediatric_ind as byteint) as loc_pediatric_ind, -- should be pediatric_ind loc is hold over
        cast(hosp_count_for_census_ind as byteint) as hosp_count_for_census_ind, 
        cast(always_count_for_census_ind as byteint) as always_count_for_census_ind, 
        case
            when 
                lower(intended_use_dept_grp_nm) = 'perioperative'
            then
                'PERIOP'
            when
                lower(intended_use_dept_grp_nm) = 'cardiac observation unit'
            then
                'PROCEDURAL'
            else  
                bed_care_dept_grp_abbr
        end as bed_care_dept_grp_abbr,
        case
            when 
                lower(intended_use_dept_grp_nm) = 'perioperative'
            then
                'PERIOP'
            when
                lower(intended_use_dept_grp_nm) = 'cardiac observation unit'
            then
                'PROCEDURAL'
            else  
                bed_care_dept_grp_nm
        end as bed_care_dept_grp_nm,
        cast(intended_use_dept_grp_abbr as varchar(100)) as intended_use_dept_grp_abbr,
        intended_use_dept_grp_nm,
        case
            when 
                lower(intended_use_dept_grp_nm) = 'perioperative'
            then
                'PERIOP COMPLEX'
            when
                lower(intended_use_dept_grp_nm) = 'cardiac observation unit'
            then
                'CPRU'
            else  
                chop_dept_grp_abbr
        end as chop_dept_grp_abbr, --should be service_grouper_abbr chop is hold over
        case
            when 
                lower(intended_use_dept_grp_nm) = 'perioperative'
            then
                'PERIOP COMPLEX'
            when
                lower(intended_use_dept_grp_nm) = 'cardiac observation unit'
            then
                'CPRU - 6NE Cardiac Post Recovery Unit'
            else  
                chop_dept_grp_nm
        end as chop_dept_grp_nm, --should be service_grouper_nm chop is hold over
        department_center_id,
        department_center_nm,
        department_center_abbr,
        'UNIT' as unit,
        'CHOP' as chop,
        'LOC' as loc,
        'INTENDED_USE' as intended_use,
        'BED_CARE_LVL' as bed_care_lvl
    from
        merge_new_and_active
        inner join {{ref('s_cdw_department_rollup_summary')}} as s_cdw_department_rollup_summary
            on s_cdw_department_rollup_summary.dept_id = merge_new_and_active.dept_id
            and s_cdw_department_rollup_summary.max_dept_align_dt = merge_new_and_active.max_dept_align_dt
    
    union all

    select
        dept_id,
        dept_nm,
        cast(min_dept_align_dt as date) as min_dept_align_dt,
        case
            when
                rollup_type = 'Supplemental'
            then
                current_date
            else
                cast(max_dept_align_dt as date)
            end as max_dept_align_dt,
        dept_abbr,
        loc_dept_grp_abbr,
        loc_dept_grp_nm,
        unit_dept_grp_abbr,
        unit_dept_grp_nm,
        unit_temp_ind,
        loc_acute_inpatient_ind,
        loc_pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind,
        bed_care_dept_grp_abbr,
        bed_care_dept_grp_nm,
        intended_use_dept_grp_abbr,
        intended_use_dept_grp_nm,
        chop_dept_grp_abbr,
        chop_dept_grp_nm,
        cast(department_center_id as nvarchar(256)) as department_center_id,
        department_center_nm,
        department_center_abbr,
        'UNIT' as unit,
        'CHOP' as chop,
        'LOC' as loc,
        'INTENDED_USE' as intended_use,
        'BED_CARE_LVL' as bed_care_lvl
    from
        {{ref('s_cdw_department_rollup_summary_supplemental')}}
    where
        max_dept_align_dt < '2022-01-01' --inactive at automation go live
        or rollup_type = 'Supplemental'
),

generate_keys as (
    select
        dept_id,
        min_dept_align_dt,
        {{
            dbt_utils.surrogate_key([
                'lower(loc_dept_grp_nm)',
                'lower(loc_dept_grp_abbr)',
                'lower(loc)'
            ])
        }} as mstr_dept_grp_loc_key,
        {{
            dbt_utils.surrogate_key([
                'lower(unit_dept_grp_nm)',
                'lower(unit_dept_grp_abbr)',
                'lower(unit)'
            ])
        }} as mstr_dept_grp_unit_key,
        {{
            dbt_utils.surrogate_key([
                'lower(bed_care_dept_grp_nm)',
                'lower(bed_care_dept_grp_abbr)',
                'lower(bed_care_lvl)'
            ])
        }} as mstr_dept_grp_bed_care_key,
        {{
            dbt_utils.surrogate_key([
                'lower(intended_use_dept_grp_nm)',
                'lower(intended_use_dept_grp_abbr)',
                'lower(intended_use)'
            ])
        }} as mstr_dept_grp_intended_use_key,
        {{
            dbt_utils.surrogate_key([
                'lower(chop_dept_grp_nm)',
                'lower(chop_dept_grp_abbr)',
                'lower(chop)'
            ])
        }} as mstr_dept_grp_chop_key
    from
        union_set
)

select
    department.dept_key,
    union_set.dept_id,
    union_set.dept_nm,
    union_set.min_dept_align_dt,
    max_dept_align_dt,
    cast(
        {{
        dbt_chop_utils.datetime_diff(
            'union_set.min_dept_align_dt',
            'max_dept_align_dt',
            'day')
        }} 
    as int) as days_cnt,
    union_set.dept_abbr,
    coalesce(
            chop_dept_grp_abbr,
            loc_dept_grp_abbr || ' [' || unit_dept_grp_nm || ']'
    )  as rollup_nm,
    mstr_dept_grp_loc_key,
    coalesce(loc_dept_grp_abbr, 'MISSING') as loc_dept_grp_abbr,
    coalesce(loc_dept_grp_nm, 'MISSING') as loc_dept_grp_nm,
    mstr_dept_grp_unit_key,
    coalesce(unit_dept_grp_abbr, 'MISSING') as unit_dept_grp_abbr,
    coalesce(unit_dept_grp_nm, 'MISSING') as unit_dept_grp_nm,
    unit_temp_ind,
    loc_acute_inpatient_ind,
    loc_pediatric_ind,
    hosp_count_for_census_ind,
    always_count_for_census_ind,
    mstr_dept_grp_bed_care_key,
    bed_care_dept_grp_abbr,
    bed_care_dept_grp_nm,
    mstr_dept_grp_intended_use_key, 
    intended_use_dept_grp_abbr,
    intended_use_dept_grp_nm, 
    mstr_dept_grp_chop_key,
    chop_dept_grp_abbr,
    chop_dept_grp_nm,
    department_center_id,
    department_center_abbr,
    department_center_nm,
    cast(now() as timestamp) as create_dt,
    cast('FACTROLLUP' as varchar(20)) as create_by,
    cast(now() as timestamp) as upd_dt,
    cast('FACTROLLUP' as varchar(20)) as upd_by
from
    union_set
    inner join {{source('cdw','department')}} as department
        on department.dept_id = union_set.dept_id
    inner join generate_keys
        on generate_keys.dept_id = union_set.dept_id
        and generate_keys.min_dept_align_dt = union_set.min_dept_align_dt
