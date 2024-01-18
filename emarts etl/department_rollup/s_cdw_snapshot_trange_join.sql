with department_group_history as (
    select
        department_id,
        department_name,
        dept_abbreviation,
        dbt_scd_id,
        service_grouper_c,
        level_of_care_grouper_c,
        rpt_grp_thirtyone_c,
        rpt_grp_thirtytwo_c,
        rpt_grp_trtythree_c,
        rpt_grp_trtyfour_c,
        rpt_grp_trtyfive_c,
        rpt_grp_thirtysix_c,
        center_c,
        cast(dbt_valid_from as date) as dbt_valid_from,
        coalesce(
            cast(dbt_valid_to as date),
            {{ var('open_end_date') }}) as dbt_valid_to
    from
        {{ref('department_group_history')}}
),

service_grouper as (
    select
        internal_id,
        name,
        abbr,
        dbt_scd_id,
        cast(dbt_valid_from as date) as dbt_valid_from,
        coalesce(
            cast(dbt_valid_to as date),
            {{ var('open_end_date') }}) as dbt_valid_to
    from
      {{ref('service_grouper_snapshot')}}

),

level_of_care as (
    select
        internal_id,
        name,
        abbr,
        dbt_scd_id,
        cast(dbt_valid_from as date) as dbt_valid_from,
        coalesce(
            cast(dbt_valid_to as date),
            {{ var('open_end_date') }}) as dbt_valid_to
    from
      {{ref('level_of_care_snapshot')}}
),

intended_use as (
    select
        internal_id,
        name,
        abbr,
        dbt_scd_id,
        cast(dbt_valid_from as date) as dbt_valid_from,
        coalesce(
            cast(dbt_valid_to as date),
            {{ var('open_end_date') }}) as dbt_valid_to
    from
      {{ref('intended_use_snapshot')}}
),

department_location as (
    select
        internal_id,
        name,
        abbr,
        dbt_scd_id,
        cast(dbt_valid_from as date) as dbt_valid_from,
        coalesce(
            cast(dbt_valid_to as date),
            {{ var('open_end_date') }}) as dbt_valid_to
    from
      {{ref('department_location_snapshot')}}
),

care_area as (
    select
        internal_id,
        name,
        abbr,
        dbt_scd_id,
        cast(dbt_valid_from as date) as dbt_valid_from,
        coalesce(
            cast(dbt_valid_to as date),
            {{ var('open_end_date') }}) as dbt_valid_to
    from
      {{ref('care_area_snapshot')}}
)

select
    department_group_history.dbt_scd_id,
    max( -- set NULLS to way in past so they can't be the Max value
        coalesce(department_group_history.dbt_valid_from, cast('2000-01-01' as date)),
        coalesce(service_grouper.dbt_valid_from, cast('2000-01-01' as date)),
        coalesce(level_of_care.dbt_valid_from, cast('2000-01-01' as date)),
        coalesce(intended_use.dbt_valid_from, cast('2000-01-01' as date)),
        coalesce(department_location.dbt_valid_from, cast('2000-01-01' as date)),
        coalesce(care_area_1.dbt_valid_from, cast('2000-01-01' as date)),
        coalesce(care_area_2.dbt_valid_from, cast('2000-01-01' as date)),
        coalesce(care_area_3.dbt_valid_from, cast('2000-01-01' as date)),
        coalesce(care_area_4.dbt_valid_from, cast('2000-01-01' as date))
    ) over() as min_dept_align_dt,
    min( -- set NULLS to way in the future so they can't be the Min value
        coalesce(department_group_history.dbt_valid_to, {{ var('open_end_date') }}),
        coalesce(service_grouper.dbt_valid_to, {{ var('open_end_date') }}),
        coalesce(level_of_care.dbt_valid_to, {{ var('open_end_date') }}),
        coalesce(intended_use.dbt_valid_to, {{ var('open_end_date') }}),
        coalesce(department_location.dbt_valid_to, {{ var('open_end_date') }}),
        coalesce(care_area_1.dbt_valid_to, {{ var('open_end_date') }}),
        coalesce(care_area_2.dbt_valid_to, {{ var('open_end_date') }}),
        coalesce(care_area_3.dbt_valid_to, {{ var('open_end_date') }}),
        coalesce(care_area_4.dbt_valid_to, {{ var('open_end_date') }})
    ) over () as max_dept_align_dt,
    department_group_history.department_id as dept_id,
    department_group_history.department_name as dept_nm,
    department_group_history.dept_abbreviation as dept_abbr,
    department_group_history.center_c,
    service_grouper.internal_id as chop_dept_grp_id,
    service_grouper.name as chop_dept_grp_nm,
    service_grouper.abbr as chop_dept_grp_abbr,
    level_of_care.internal_id as bed_care_dept_grp_id,
    level_of_care.name as bed_care_dept_grp_nm,
    level_of_care.abbr as bed_care_dept_grp_abbr,
    intended_use.internal_id as intended_use_dept_grp_id,
    intended_use.name as intended_use_dept_grp_nm,
    intended_use.abbr as intended_use_dept_grp_abbr,
    department_location.internal_id as loc_dept_grp_id,
    department_location.name as loc_dept_grp_nm,
    department_location.abbr as loc_dept_grp_abbr,
    care_area_1.internal_id as care_area_1_dept_grp_id,
    care_area_1.name as care_area_1_dept_grp_nm,
    care_area_1.abbr as care_area_1_dept_grp_abbr,
    care_area_2.internal_id as care_area_2_dept_grp_id,
    care_area_2.name as care_area_2_dept_grp_nm,
    care_area_2.abbr as care_area_2_dept_grp_abbr,
    care_area_3.internal_id as care_area_3_dept_grp_id,
    care_area_3.name as care_area_3_dept_grp_nm,
    care_area_3.abbr as care_area_3_dept_grp_abbr,
    care_area_4.internal_id as care_area_4_dept_grp_id,
    care_area_4.name as care_area_4_dept_grp_nm,
    care_area_4.abbr as care_area_4_dept_grp_abbr
from
    department_group_history
    left join service_grouper
        on service_grouper.internal_id = department_group_history.service_grouper_c
        and service_grouper.dbt_valid_from < department_group_history.dbt_valid_to
        and service_grouper.dbt_valid_to >= department_group_history.dbt_valid_from
    left join level_of_care
        on level_of_care.internal_id = department_group_history.level_of_care_grouper_c
        and level_of_care.dbt_valid_from < department_group_history.dbt_valid_to
        and level_of_care.dbt_valid_to >= department_group_history.dbt_valid_from
    left join intended_use
        on intended_use.internal_id = department_group_history.rpt_grp_thirtyone_c
        and intended_use.dbt_valid_from < department_group_history.dbt_valid_to
        and intended_use.dbt_valid_to >= department_group_history.dbt_valid_from
    left join department_location
        on department_location.internal_id = department_group_history.rpt_grp_thirtytwo_c
        and department_location.dbt_valid_from < department_group_history.dbt_valid_to
        and department_location.dbt_valid_to >= department_group_history.dbt_valid_from
    left join care_area as care_area_1
        on care_area_1.internal_id = department_group_history.rpt_grp_trtythree_c
        and care_area_1.dbt_valid_from < department_group_history.dbt_valid_to
        and care_area_1.dbt_valid_to >= department_group_history.dbt_valid_from
    left join care_area as care_area_2
        on care_area_2.internal_id = department_group_history.rpt_grp_trtyfour_c
        and care_area_2.dbt_valid_from < department_group_history.dbt_valid_to
        and care_area_2.dbt_valid_to >= department_group_history.dbt_valid_from
    left join care_area as care_area_3
        on care_area_3.internal_id = department_group_history.rpt_grp_trtyfive_c
        and care_area_3.dbt_valid_from < department_group_history.dbt_valid_to
        and care_area_3.dbt_valid_to >= department_group_history.dbt_valid_from
    left join care_area as care_area_4
        on care_area_4.internal_id = department_group_history.rpt_grp_thirtysix_c
        and care_area_4.dbt_valid_from < department_group_history.dbt_valid_to
        and care_area_4.dbt_valid_to >= department_group_history.dbt_valid_from
where
    min_dept_align_dt < max_dept_align_dt
