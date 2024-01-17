/* stg_department_staffing
Prep for department_staffing for filter lists for Epic Departments and Department Groups,
current alignment.
See data over time in department_cost_center_pp_history
*/
with
dept_check_cost_center as (
    select
        round(department_id) as department_id,
        cost_center_id
    from
         {{ ref('stg_department_cost_center_history') }}
    group by
        department_id,
        cost_center_id
),

dept_one_cost_center as (
    select
        department_id
    from
        dept_check_cost_center
    group by
        department_id having count(*) = 1
),

dept_get_one_cost_center as (
    select
        has_one.department_id,
        get_the_one.cost_center_id
    from
        dept_one_cost_center as has_one
        inner join dept_check_cost_center as get_the_one
            on has_one.department_id = get_the_one.department_id
),

distinct_dept as (
    select
        round(department_id) as department_id
    from
        {{ ref('stg_department_cost_center_history') }}
    group by
        department_id
)

select
    nccs_dept.department_id,
    case d.specialty_name
        when 'UNKNOWN' then ''
        else d.specialty_name
    end as specialty,
	d.department_abbr,
	d.department_name,
    d.care_area_abbr as current_care_area_abbr,
    d.intended_use_abbr as current_intended_use_abbr,

	-- clone this logic also to department_cost_center_pp_history SQL
    case
        when d.intended_use_name in (
            'Primary Care',
            'Urgent Care',
            'Emergency')
        then d.intended_use_name
        when d.intended_use_name
            = 'Outpatient Specialty Care'
        then d.care_area_name /* full care area name for speciality */
        else d.care_area_abbr /* else use VCC abbreviation */
    end as current_department_group,
    coalesce(get_use_grouper.set_dept_use_grouper, 'unknown') as department_use_grouper,
    case current_department_group
        when 'NA' then department_use_grouper
        when 'Unknown' then department_use_grouper
        else case
            when d.intended_use_name = 'Overflow' then 'IP Overflow'
            else current_department_group
        end
    end as care_area_or_use,
    coalesce(get_use_grouper.hospital_unit_ind, 0) as hospital_unit_ind,
    case department_use_grouper
        when 'OP Specialty Care' then 1 else 0
    end as specialty_care_ind,
	-- end clone this logic also to department_cost_center_pp_history SQL

	d.location_name,
    d.department_center_id,
    d.department_center_abbr,
    d.department_center,
    d.scc_abbreviation,
    d.scc_ind,
    d.care_network_ind,
    d.record_status_active_ind,
    coalesce(cc.cost_center_display, 'multiples') as ref_cost_center,
    cc.cost_center_group as ref_cc_group,
    cc.cost_center_type as ref_cc_type,
    cc.whuos_rollup as ref_whuos_rollup,
    d.care_area_id as current_care_area_id,
    d.care_area_name as current_care_area_name,
    d.intended_use_id as current_intended_use_id,
    d.intended_use_name as current_intended_use_name,
    get_single_cc.cost_center_id as ref_single_cost_center_id,
    d.specialty_name as epic_specialty_name
from
    distinct_dept as nccs_dept
    inner join  {{ ref('stg_department_all') }} as d
        on nccs_dept.department_id = d.department_id
    left join  {{ ref('lookup_department_use_grouper') }} as get_use_grouper
        on d.intended_use_name = get_use_grouper.intended_use_name
    left join dept_get_one_cost_center as get_single_cc
        on nccs_dept.department_id = get_single_cc.department_id
    left join  {{ ref('nursing_cost_center_attributes') }} as cc
        on get_single_cc.cost_center_id = cc.cost_center_id
