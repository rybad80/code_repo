with stage as (
    select
        dept_key,
        mstr_dept_grp_loc_key,
        mstr_dept_grp_unit_key,
        rollup_nm,
        dept_id,
        dept_abbr,
        dept_nm,
        chop_dept_grp_abbr,
        chop_dept_grp_nm,
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
        department_center_id,
        department_center_abbr,
        department_center_nm,
        mstr_dept_grp_bed_care_key,
        mstr_dept_grp_intended_use_key,
        mstr_dept_grp_chop_key,
        min_dept_align_dt,
        coalesce(max_dept_align_dt, current_date) as max_dept_align_dt
    from
        {{ref('fact_department_rollup_summary')}}
    where
        -- Inpatient Campus departments only
        -- Includes Periop, Cardiac Procudural, and ED
        bed_care_dept_grp_nm is not null
),

sort_num_stage as (
    -- This creates the sort num columns from MDG
    -- Only used in QlikViews but I would rather recreate the columns
    -- than remove them and have something break.
    select
        mstr_dept_grp_chop_key as mstr_dept_grp_key,
        chop_dept_grp_abbr as dept_grp_abbr,
        'CHOP' as dept_grp_type,
        5 as dept_grp_type_sort_num
    from
        stage
    where
        chop_dept_grp_abbr is not null
    group by
        mstr_dept_grp_chop_key,
        chop_dept_grp_abbr

    union all

    select
        mstr_dept_grp_loc_key as mstr_dept_grp_key,
        loc_dept_grp_abbr as dept_grp_abbr,
        'LOC' as dept_grp_type,
        6 as dept_grp_type_sort_num
    from
        stage
    where
        loc_dept_grp_abbr is not null
    group by
        mstr_dept_grp_loc_key,
        loc_dept_grp_abbr

    union all

    select
        mstr_dept_grp_intended_use_key as mstr_dept_grp_key,
        intended_use_dept_grp_abbr as dept_grp_abbr,
        'INTENDED_USE' as dept_grp_type,
        20 as dept_grp_type_sort_num
    from
        stage
    where
        intended_use_dept_grp_abbr is not null
    group by
        mstr_dept_grp_intended_use_key,
        intended_use_dept_grp_abbr

    union all

    select
        mstr_dept_grp_bed_care_key as mstr_dept_grp_key,
        bed_care_dept_grp_abbr as dept_grp_abbr,
        'BED_CARE_LVL' as dept_grp_type,
        25 as dept_grp_type_sort_num
    from
        stage
    where
        bed_care_dept_grp_abbr is not null
    group by
        mstr_dept_grp_bed_care_key,
        bed_care_dept_grp_abbr

    union all

    select
        mstr_dept_grp_unit_key as mstr_dept_grp_key,
        unit_dept_grp_abbr as dept_grp_abbr,
        'UNIT' as dept_grp_type,
        8 as dept_grp_type_sort_num
    from
        stage
    where
        unit_dept_grp_abbr is not null
    group by
        mstr_dept_grp_unit_key,
        unit_dept_grp_abbr
),

sort_num as (
    select
        cast(mstr_dept_grp_key as bigint) as mstr_dept_grp_key,
        cast(dept_grp_abbr as varchar(100)) as dept_grp_abbr,
        cast(dept_grp_type as varchar(25)) as dept_grp_type,
        cast(dept_grp_type_sort_num as int) as dept_grp_type_sort_num,
        cast(dense_rank() over(partition by dept_grp_type order by dept_grp_abbr) as int) as dept_grp_sort_num
    from
        sort_num_stage
)

select
    stage.dept_key,
    cast(master_date.dt_key as bigint) as dept_align_dt_key,
    cast(master_date.full_dt as timestamp) as dept_align_dt,
    stage.mstr_dept_grp_loc_key,
    stage.mstr_dept_grp_unit_key,
    stage.rollup_nm,
    stage.dept_id,
    stage.dept_abbr,
    stage.dept_nm,
    stage.loc_dept_grp_abbr,
    stage.loc_dept_grp_nm,
    loc.dept_grp_sort_num as loc_sort_num, -- hold over
    'LOC' as loc_dept_grp_type, -- hold over
    stage.unit_dept_grp_abbr,
    stage.unit_dept_grp_nm,
    unit.dept_grp_sort_num as unit_sort_num, -- hold over
    'UNIT' as unit_dept_grp_type, -- hold over
    stage.loc_acute_inpatient_ind, -- hold over should be acute_inpatient_ind
    stage.loc_pediatric_ind, -- hold over should be pediatric_ind
    stage.unit_temp_ind,
    stage.min_dept_align_dt as loc_eff_dt, -- hold over
    stage.max_dept_align_dt as loc_end_dt, -- hold over
    stage.min_dept_align_dt as unit_eff_dt, -- hold over
    stage.max_dept_align_dt as unit_end_dt, -- hold over
    stage.unit_temp_ind as loc_temp_ind, -- hold over
    stage.hosp_count_for_census_ind, --new
    stage.always_count_for_census_ind, --new
    bed_care.dept_grp_sort_num as bed_care_sort_num, --hold over
    stage.bed_care_dept_grp_abbr, --hold over name, level of care
    stage.bed_care_dept_grp_nm, --hold over name, level of care
    stage.intended_use_dept_grp_abbr,
    stage.intended_use_dept_grp_nm,
    intended_use.dept_grp_sort_num as intended_use_sort_num, -- hold over
    stage.chop_dept_grp_abbr, --new field hold over name should be service_grouper_abbr
    stage.chop_dept_grp_nm, --new field hold over name should be service_grouper_nm
    stage.mstr_dept_grp_bed_care_key,
    stage.mstr_dept_grp_intended_use_key,
    stage.mstr_dept_grp_chop_key,
    stage.department_center_abbr,
    stage.department_center_nm,
    cast(now() as timestamp) as create_dt,
    cast('FACTROLLUP' as varchar(20)) as create_by,
    cast(now() as timestamp) as upd_dt,
    cast('FACTROLLUP' as varchar(20)) as upd_by
from
    stage
    inner join {{source('cdw','master_date')}} as master_date
        on master_date.full_dt
        between stage.min_dept_align_dt and stage.max_dept_align_dt
    left join sort_num as loc
        on loc.mstr_dept_grp_key = stage.mstr_dept_grp_loc_key
        and loc.dept_grp_type = 'LOC'
    left join sort_num as unit
        on unit.mstr_dept_grp_key = stage.mstr_dept_grp_unit_key
        and unit.dept_grp_type = 'UNIT'
    left join sort_num as chop
        on chop.mstr_dept_grp_key = stage.mstr_dept_grp_chop_key
        and chop.dept_grp_type = 'CHOP'
    left join sort_num as bed_care
        on bed_care.mstr_dept_grp_key = stage.mstr_dept_grp_bed_care_key
        and bed_care.dept_grp_type = 'BED_CARE_LVL'
    left join sort_num as intended_use
        on intended_use.mstr_dept_grp_key = stage.mstr_dept_grp_intended_use_key
        and intended_use.dept_grp_type = 'INTENDED_USE'
