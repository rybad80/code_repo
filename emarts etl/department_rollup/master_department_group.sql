with stg_department_rollup as (
    select
        mstr_dept_grp_loc_key,
        loc_dept_grp_abbr,
        loc_dept_grp_nm,
        mstr_dept_grp_unit_key,
        unit_dept_grp_abbr,
        unit_dept_grp_nm,
        mstr_dept_grp_bed_care_key,
        bed_care_dept_grp_abbr,
        bed_care_dept_grp_nm,
        mstr_dept_grp_intended_use_key,
        intended_use_dept_grp_abbr,
        intended_use_dept_grp_nm,
        mstr_dept_grp_chop_key,
        chop_dept_grp_abbr,
        chop_dept_grp_nm,
        loc_acute_inpatient_ind as acute_inpatient_ind,
        loc_pediatric_ind as pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind
    from
        {{ref('fact_department_rollup_summary')}}

),

unionset as (
    select
        mstr_dept_grp_chop_key as mstr_dept_grp_key,
        chop_dept_grp_abbr as dept_grp_abbr,
        chop_dept_grp_nm as dept_grp_nm,
        'CHOP' as dept_grp_type,
        5 as dept_grp_type_sort_num,
        max(acute_inpatient_ind) as acute_inpatient_ind,
        max(pediatric_ind) as pediatric_ind,
        max(hosp_count_for_census_ind) as hosp_count_for_census_ind,
        max(always_count_for_census_ind) as always_count_for_census_ind
    from
        stg_department_rollup
    where
        chop_dept_grp_nm is not null
    group by
        mstr_dept_grp_chop_key,
        chop_dept_grp_abbr,
        chop_dept_grp_nm

    union all

    select
        mstr_dept_grp_loc_key as mstr_dept_grp_key,
        loc_dept_grp_abbr as dept_grp_abbr,
        loc_dept_grp_nm as dept_grp_nm,
        'LOC' as dept_grp_type,
        6 as dept_grp_type_sort_num,
        acute_inpatient_ind,
        pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind
    from
        stg_department_rollup
    where
        loc_dept_grp_nm is not null
    group by
        mstr_dept_grp_loc_key,
        loc_dept_grp_abbr,
        loc_dept_grp_nm,
        acute_inpatient_ind,
        pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind

    union all

    select
        mstr_dept_grp_intended_use_key as mstr_dept_grp_key,
        intended_use_dept_grp_abbr as dept_grp_abbr,
        intended_use_dept_grp_nm as dept_grp_nm,
        'INTENDED_USE' as dept_grp_type,
        20 as dept_grp_type_sort_num,
        acute_inpatient_ind,
        pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind
    from
        stg_department_rollup
    where
        intended_use_dept_grp_nm is not null
    group by
        mstr_dept_grp_intended_use_key,
        intended_use_dept_grp_abbr,
        intended_use_dept_grp_nm,
        acute_inpatient_ind,
        pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind

    union all

    select
        mstr_dept_grp_bed_care_key as mstr_dept_grp_key,
        bed_care_dept_grp_abbr as dept_grp_abbr,
        bed_care_dept_grp_nm as dept_grp_nm,
        'BED_CARE_LVL' as dept_grp_type,
        25 as dept_grp_type_sort_num,
        acute_inpatient_ind,
        pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind
    from
        stg_department_rollup
    where
        bed_care_dept_grp_nm is not null
    group by
        mstr_dept_grp_bed_care_key,
        bed_care_dept_grp_abbr,
        bed_care_dept_grp_nm,
        acute_inpatient_ind,
        pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind

    union all

    select
        mstr_dept_grp_unit_key as mstr_dept_grp_key,
        unit_dept_grp_abbr as dept_grp_abbr,
        unit_dept_grp_nm as dept_grp_nm,
        'UNIT' as dept_grp_type,
        8 as dept_grp_type_sort_num,
        max(acute_inpatient_ind) as acute_inpatient_ind,
        max(pediatric_ind) as acute_inpatient_ind,
        max(hosp_count_for_census_ind) as hosp_count_for_census_ind,
        max(always_count_for_census_ind) as always_count_for_census_ind
    from
        stg_department_rollup
    where
        unit_dept_grp_nm is not null
    group by
        mstr_dept_grp_unit_key,
        unit_dept_grp_abbr,
        unit_dept_grp_nm
),

new_dept_grp as (
    select distinct
        mstr_dept_grp_key,
        dept_grp_abbr,
        dept_grp_nm,
        dept_grp_type,
        dept_grp_type_sort_num,
        case
            when
                lower(dept_grp_type) in ('unit', 'chop')
                and always_count_for_census_ind = 1
            then
                acute_inpatient_ind
            else
                -2
        end as acute_inpatient_ind, --not rehab
        case
            when
                lower(dept_grp_type) in ('unit', 'chop')
                and always_count_for_census_ind = 1
            then
                pediatric_ind
            else
                -2
        end as pediatric_ind, -- not sdu
        case
            when
                lower(dept_grp_type) = 'intended_use'
            then
                hosp_count_for_census_ind
            else
                -2
        end as hosp_count_for_census_ind,
        case
            when
                lower(dept_grp_type) = 'intended_use'
            then
                always_count_for_census_ind
            else
                -2
        end as always_count_for_census_ind,
        now() as create_dt,
        'ADMIN' as create_by,
        now() as upd_dt,
        'ADMIN' as upd_by
    from
        unionset
),

old_dept_grp as (
    select
        mstr_dept_grp_key,
        dept_grp_abbr,
        dept_grp_nm,
        dept_grp_type,
        dept_grp_type_sort_num,
        acute_inpatient_ind,
        pediatric_ind,
        hosp_count_for_census_ind,
        always_count_for_census_ind,
        create_dt,
        create_by,
        upd_dt,
        upd_by
    from
        {{ref('master_department_group_supplemental')}}
    where
        --mstr_dept_grp_key not in (select mstr_dept_grp_key from new_dept_grp)
        --only OP and Specialty Care (no numbers in names)
        (
            dept_grp_type = 'CHOP'
            and regexp_extract(dept_grp_nm, '\d') is null
            and dept_grp_nm not in ('PICU', 'NICU', 'ED', 'EDECU', 'PERIOP COMPLEX', '3W CSH CHOP')
        )
        or dept_grp_type in ('NURSING', 'OTHER_OCC', 'SEED', 'NHSN', 'NRCP')
),

combine_new_old as (

    select
        *,
        'MASTER_DEPARMENT_GROUP_DEPRECATED' as source
    from
        old_dept_grp

    union all

    select
        *,
        'FACT_DEPARTMENT_ROLLUP_SUMMARY' as source
    from
        new_dept_grp
)

select
    cast(mstr_dept_grp_key as bigint) as mstr_dept_grp_key,
    cast(dept_grp_abbr as varchar(100)) as dept_grp_abbr,
    cast(dept_grp_nm as varchar(100)) as dept_grp_nm,
    cast(dept_grp_type as varchar(25)) as dept_grp_type,
    cast(dept_grp_type_sort_num as int) as dept_grp_type_sort_num,
    cast(dense_rank() over(partition by dept_grp_type order by dept_grp_abbr) as int) as dept_grp_sort_num,
    cast(acute_inpatient_ind as byteint) as acute_inpatient_ind,
    cast(pediatric_ind as byteint) as pediatric_ind,
    cast(hosp_count_for_census_ind as byteint) as hosp_count_for_census_ind,
    cast(always_count_for_census_ind as byteint) as always_count_for_census_ind,
    cast(create_dt as timestamp) as create_dt,
    cast(create_by as varchar(20)) as create_by,
    cast(upd_dt as timestamp) as upd_dt,
    cast(upd_by as varchar(20)) as upd_by,
    source
from
    combine_new_old
