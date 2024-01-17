{{ config(meta = {
    'critical': false
}) }}
/* stg_department_cost_center_history
a BETA dataset under review in fall 2023
to relate Epic departments to cost centers
over time (pay period) to facilitate trend by CHOP area data that is at
Epic department granularity with its corresponding cost center's data.
The inpatient units' data has changed over time for some departments
and is already in department_cost_center_xref historically so union this
with:
The Primary Care alginments maintained by the Care Network team in
lookup_care_network_department_cost_center_sites.
Methods to be finalized after Nursing client review for the remainder (hard-coded
here for now) but essentially mapping the Epic specialty to the cost center name,
or a slightly adjusted name, handles most of the Specialty Care departments,
and urgent Care is hard-code hered for now
to make:
one table of all Epic departments applying to staffing over time related to cost center
*/
with
epic_cc_dept as (
    select
        pp.pp_end_dt_key,
        pp.pp_end_dt,
        department_id,
        workday_cost_center_id as cost_center_id,
        clarity_cost_center_id,
        cost_center_site_id
    from
        {{ ref('department_cost_center_xref') }} as cc_dept_hist
        inner join {{ ref('nursing_pay_period') }} as pp
            on cc_dept_hist.align_dt_key = pp.pp_end_dt_key
    where
        workday_cost_center_id != '97777' /* ignore cc EPIC Suspense */
),

min_dt_key as (
	select
        min(pp_end_dt_key) as min_cc_dept_dt_key
	from
        epic_cc_dept
),

dt_key as (
	select
        pp_end_dt_key,
        pp_end_dt
	from
        {{ ref('nursing_pay_period') }} as pp
        inner join min_dt_key
            on pp.prior_pay_period_ind = 1
            and pp.pp_end_dt_key >= min_dt_key.min_cc_dept_dt_key
),

/* the non Inpatient, non Primary Care departments are special cases for now */
non_ip_non_pc as (
    select
        department_id,
        cost_center_id
    from
        {{ ref('stg_nursing_dept_cc_p1_other') }}
),

all_ccs as (
    /* Epic data driven matches for cost center to Epic department over time */
	select
        epic_cc_dept.pp_end_dt_key,
        epic_cc_dept.pp_end_dt,
        epic_cc_dept.department_id,
        epic_cc_dept.cost_center_id,
        epic_cc_dept.clarity_cost_center_id,
        epic_cc_dept.cost_center_site_id
	from
        epic_cc_dept

	union all
    /* Primary Care lookup-driven matches for cost center to Epic department rolled out to pay periods */
	select
        dt_key.pp_end_dt_key,
        dt_key.pp_end_dt,
        amb_cc.department_id,
        amb_cc.cost_center_id,
        null::integer as clarity_cost_center_id,
        amb_cc.cost_center_site_id
	from
        dt_key
        inner join {{ ref('lookup_care_network_department_cost_center_sites') }} as amb_cc
            on amb_cc.department_id > 0

	union all
    /* other matches for cost center to Epic department rolled out to pay periods */
	select
        dt_key.pp_end_dt_key,
        dt_key.pp_end_dt,
        oth_dept_cc.department_id,
        oth_dept_cc.cost_center_id,
        null::integer as clarity_cost_center_id,
        null as cost_center_site_id
	from
        dt_key
        inner join non_ip_non_pc as oth_dept_cc
            on oth_dept_cc.department_id > 0
            and oth_dept_cc.cost_center_id is not null
),

assemble_attributes as (
    select
        cc.cost_center_display,
        cost_center_group,
        cost_center_type,
        case
            when dept.specialty_name = 'UNKNOWN'
            then '' else dept.specialty_name
        end as epic_department_specialty_name,
        all_ccs.pp_end_dt_key,
        all_ccs.pp_end_dt,
        round(all_ccs.department_id, 0) as department_id,
        all_ccs.cost_center_id,
        all_ccs.clarity_cost_center_id,
        all_ccs.cost_center_site_id,
        dept.department_abbr,
        dept.department_name,
        coalesce(dept_hist.care_area_name, dept.care_area_name) as historical_care_area_name,
        coalesce(dept_hist.care_area_abbr,
            dept.care_area_abbr) as historical_care_area_abbr, /* for Dept Grp use historically */
        coalesce(dept_hist.intended_use_name, dept.intended_use_name) as historical_intended_use_name
    from
        all_ccs
        left join {{ ref('department_history') }} as dept_hist
            on all_ccs.department_id = dept_hist.department_id
            and all_ccs.pp_end_dt between
                dept_hist.valid_from_date and dept_hist.valid_to_date

        left join {{ ref('stg_department_all') }} as dept
            on all_ccs.department_id = dept.department_id

        left join {{ ref('nursing_cost_center_attributes') }} as cc
            on all_ccs.cost_center_id = cc.cost_center_id
)

select
    cost_center_display,
    cost_center_group,
    cost_center_type,
    epic_department_specialty_name,
    pp_end_dt_key,
    pp_end_dt,
    cost_center_id,
    cost_center_site_id,
    department_abbr,
    department_name,
    historical_care_area_abbr,
    case when historical_care_area_name = 'Unknown'
        then 'Care Area Unk: ' || department_id || ' [' || department_name || ']'
        else historical_care_area_name
    end as historical_care_area_name,
    coalesce(historical_intended_use_name,
        'check this NULL intended use!') as historical_intended_use_name,
    department_id,
    clarity_cost_center_id
from
    assemble_attributes
