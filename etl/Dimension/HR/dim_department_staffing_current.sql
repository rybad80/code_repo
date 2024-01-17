{{
  config(
    materialized = 'incremental',
    unique_key = 'department_id',
    incremental_strategy = 'merge',
    merge_update_columns = ['department_id', 'specialty', 'department_abbr', 'department_name', 'current_care_area_abbr', 'current_intended_use_abbr', 'current_department_group', 'department_use_grouper', 'care_area_or_use', 'hospital_unit_ind', 'specialty_care_ind', 'location_name', 'department_center_abbr', 'scc_abbreviation', 'scc_ind', 'care_network_ind', 'record_status_active_ind', 'curr_pp_end_dt_key', 'max_past_pp_end_dt_key', 'over_time_only_in_past_ind', 'current_latest_cost_center_display', 'current_latest_cost_center_type', 'current_latest_cost_center_group', 'current_latest_whuos_rollup', 'whuos_short_rollup', 'current_care_area_id', 'current_care_area_name', 'current_intended_use_id', 'current_intended_use_name', 'department_center_id', 'department_center', 'ref_single_cost_center_id', 'current_latest_cost_center_id', 'epic_specialty_name', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': false
    }
  )
}}
/* dim_department_staffing_current
capture attributes that apply now for Epic departments with people staffed to them
and how they relate/related to cost center and rollup(s) for Nursing and other
purposes and with latest date keys where applicable.
Please note that update_date will not change for cases when only the 
curr_pp_end_dt_key is modified in incremental processing
*/
with
curr_dept_pp as (
    select
        dept_cc_pp.department_id as curr_dept_id,
        dept_cc_pp.pp_end_dt_key as curr_pp_end_dt_key,
        dept_cc_pp.cost_center_id as curr_cc_id,
        dept_cc_pp.cost_center_type as curr_cc_type,
        dept_cc_pp.cost_center_group as curr_cc_grp,
        dept_cc_pp.cost_center_display as curr_cc_display
    from
        {{ ref('department_cost_center_pp_history') }} as dept_cc_pp
        inner join {{ ref('nursing_pay_period') }} as curr_pp
            on dept_cc_pp.pp_end_dt_key = curr_pp.pp_end_dt_key
            and curr_pp.latest_pay_period_ind = 1
),
max_dept_pp as (
    select
        dept_cc_pp.department_id as max_dept_id,
        dept_cc_pp.pp_end_dt_key as max_past_pp_end_dt_key,
        dept_cc_pp.cost_center_id as max_cc_id,
        dept_cc_pp.cost_center_type as max_cc_type,
        dept_cc_pp.cost_center_group as max_cc_grp,
        dept_cc_pp.cost_center_display as max_cc_display
    from
        {{ ref('department_cost_center_pp_history') }} as dept_cc_pp
        inner join {{ ref('stg_nursing_dept_cc_p3_add_start') }} as d_cc_when
            on dept_cc_pp.pp_end_dt_key = d_cc_when.take_to_end_dt_key
            and dept_cc_pp.department_id = d_cc_when.department_id
),
resolve_a_cc as (
    /* address the Epic departments that had different cost centers over time or are
    now not alignmd at all or inactive
    by getting the applicable current or latet alignments at that applicable time */
    select
        dept_curr.department_id as dept_id,
        curr_pp.curr_pp_end_dt_key,
        case
            when max_pp.max_past_pp_end_dt_key
                < coalesce(curr_pp.curr_pp_end_dt_key, 99991231)
            then max_pp.max_past_pp_end_dt_key
        end as max_past_pp_end_dt_key,
        case
            when max_pp.max_past_pp_end_dt_key
                < coalesce(curr_pp.curr_pp_end_dt_key, 99991231)
            then 1 else 0
        end as over_time_only_in_past_ind,
        coalesce(dept_curr.ref_single_cost_center_id,
            curr_pp.curr_cc_id,
            max_pp.max_cc_id) as current_latest_cost_center_id,
        coalesce(dept_curr.ref_cc_type,
            curr_pp.curr_cc_type,
            max_pp.max_cc_type) as current_latest_cost_center_type,
        coalesce(dept_curr.ref_cc_group,
            curr_pp.curr_cc_grp,
            max_pp.max_cc_grp) as current_latest_cost_center_group,
        coalesce(
          case dept_curr.ref_cost_center
                when 'multiples' then null
                else dept_curr.ref_cost_center
            end, /* force a fall-thru for the multiples scenario */
            curr_pp.curr_cc_display,
            max_pp.max_cc_display) as current_latest_cost_center_display
    from
        {{ ref('stg_department_staffing') }} as dept_curr
        left join curr_dept_pp as curr_pp
            on dept_curr.department_id = curr_pp.curr_dept_id
        left join max_dept_pp as max_pp
            on dept_curr.department_id = max_pp.max_dept_id
),

department_staffing_data_row as (
select
    {{
        dbt_utils.surrogate_key([
            'dept.department_id'
        ])
    }} as department_staffing_key,
    dept.department_id,
    dept.specialty,
    dept.department_abbr,
    dept.department_name,
    dept.current_care_area_abbr,
    dept.current_intended_use_abbr,

    dept.current_department_group,
    dept.department_use_grouper,
    dept.care_area_or_use,
    dept.hospital_unit_ind,
    dept.specialty_care_ind,
    dept.location_name,
    dept.department_center_abbr,
    dept.scc_abbreviation,
    dept.scc_ind,
    dept.care_network_ind,
    dept.record_status_active_ind,

    cc.curr_pp_end_dt_key,
    cc.max_past_pp_end_dt_key,
    cc.over_time_only_in_past_ind,

    cc.current_latest_cost_center_display,
    cc.current_latest_cost_center_type,
    cc.current_latest_cost_center_group,
    get_cc_rollup.whuos_rollup as current_latest_whuos_rollup,
    coalesce(get_cc_rollup.hppd_rollup_short_name,
        get_cc_rollup.whuos_rollup) as whuos_short_rollup,
    dept.current_care_area_id,
    dept.current_care_area_name,
    dept.current_intended_use_id,
    dept.current_intended_use_name,
    dept.department_center_id,
    dept.department_center,
    dept.ref_single_cost_center_id,
    cc.current_latest_cost_center_id,
    dept.epic_specialty_name,

    {{
        dbt_utils.surrogate_key(['department_id', 'specialty', 'department_abbr', 'department_name', 'current_care_area_abbr', 'current_intended_use_abbr', 'current_department_group', 'department_use_grouper', 'care_area_or_use', 'hospital_unit_ind', 'specialty_care_ind', 'location_name', 'department_center_abbr', 'scc_abbreviation', 'scc_ind', 'care_network_ind', 'record_status_active_ind', 'curr_pp_end_dt_key', 'max_past_pp_end_dt_key', 'over_time_only_in_past_ind', 'current_latest_cost_center_display', 'current_latest_cost_center_type', 'current_latest_cost_center_group', 'current_latest_whuos_rollup', 'whuos_short_rollup', 'current_care_area_id', 'current_care_area_name', 'current_intended_use_id', 'current_intended_use_name', 'department_center_id', 'department_center', 'ref_single_cost_center_id', 'current_latest_cost_center_id', 'epic_specialty_name'])
    }} as hash_value,
    dept.department_id as integration_id,
    current_timestamp as create_date,
    current_timestamp as new_update_date,
    {{
        dbt_utils.surrogate_key(['department_id', 'specialty', 'department_abbr', 'department_name', 'current_care_area_abbr', 'current_intended_use_abbr', 'current_department_group', 'department_use_grouper', 'care_area_or_use', 'hospital_unit_ind', 'specialty_care_ind', 'location_name', 'department_center_abbr', 'scc_abbreviation', 'scc_ind', 'care_network_ind', 'record_status_active_ind', 'max_past_pp_end_dt_key', 'over_time_only_in_past_ind', 'current_latest_cost_center_display', 'current_latest_cost_center_type', 'current_latest_cost_center_group', 'current_latest_whuos_rollup', 'whuos_short_rollup', 'current_care_area_id', 'current_care_area_name', 'current_intended_use_id', 'current_intended_use_name', 'department_center_id', 'department_center', 'ref_single_cost_center_id', 'current_latest_cost_center_id', 'epic_specialty_name'])
    }} as today_hash_value_without_latest_dt

from
    {{ ref('stg_department_staffing') }} as dept
    left join resolve_a_cc as cc
        on dept.department_id = cc.dept_id
    left join {{ ref('nursing_cost_center_attributes') }} as get_cc_rollup
        on cc.current_latest_cost_center_id = get_cc_rollup.cost_center_id
)
{%- if is_incremental() %}
,

check_on_latest_date_chg as (
    select
        department_id as chk_department_id,
        update_date as latest_real_update_date,
    {{
        dbt_utils.surrogate_key(['department_id', 'specialty', 'department_abbr', 'department_name', 'current_care_area_abbr', 'current_intended_use_abbr', 'current_department_group', 'department_use_grouper', 'care_area_or_use', 'hospital_unit_ind', 'specialty_care_ind', 'location_name', 'department_center_abbr', 'scc_abbreviation', 'scc_ind', 'care_network_ind', 'record_status_active_ind', 'max_past_pp_end_dt_key', 'over_time_only_in_past_ind', 'current_latest_cost_center_display', 'current_latest_cost_center_type', 'current_latest_cost_center_group', 'current_latest_whuos_rollup', 'whuos_short_rollup', 'current_care_area_id', 'current_care_area_name', 'current_intended_use_id', 'current_intended_use_name', 'department_center_id', 'department_center', 'ref_single_cost_center_id', 'current_latest_cost_center_id', 'epic_specialty_name'])
    }}
        as pre_run_hash_value_without_latest_dt
    from
        {{ this }}
)
{%- endif %}

select
    department_staffing_data_row.department_staffing_key,
    department_staffing_data_row.department_id,
    department_staffing_data_row.specialty,
    department_staffing_data_row.department_abbr,
    department_staffing_data_row.department_name,
    department_staffing_data_row.current_care_area_abbr,
    department_staffing_data_row.current_intended_use_abbr,
    department_staffing_data_row.current_department_group,
    department_staffing_data_row.department_use_grouper,
    department_staffing_data_row.care_area_or_use,
    department_staffing_data_row.hospital_unit_ind,
    department_staffing_data_row.specialty_care_ind,
    department_staffing_data_row.location_name,
    department_staffing_data_row.department_center_abbr,
    department_staffing_data_row.scc_abbreviation,
    department_staffing_data_row.scc_ind,
    department_staffing_data_row.care_network_ind,
    department_staffing_data_row.record_status_active_ind,
    department_staffing_data_row.curr_pp_end_dt_key,
    department_staffing_data_row.max_past_pp_end_dt_key,
    department_staffing_data_row.over_time_only_in_past_ind,
    department_staffing_data_row.current_latest_cost_center_display,
    department_staffing_data_row.current_latest_cost_center_type,
    department_staffing_data_row.current_latest_cost_center_group,
    department_staffing_data_row.current_latest_whuos_rollup,
    department_staffing_data_row.whuos_short_rollup,
    department_staffing_data_row.current_care_area_id,
    department_staffing_data_row.current_care_area_name,
    department_staffing_data_row.current_intended_use_id,
    department_staffing_data_row.current_intended_use_name,
    department_staffing_data_row.department_center_id,
    department_staffing_data_row.department_center,
    department_staffing_data_row.ref_single_cost_center_id,
    department_staffing_data_row.current_latest_cost_center_id,
    department_staffing_data_row.epic_specialty_name,
    department_staffing_data_row.hash_value,
    department_staffing_data_row.integration_id,
    department_staffing_data_row.create_date,
{%- if is_incremental() %}    
    case
        when check_on_latest_date_chg.chk_department_id is null
        then department_staffing_data_row.new_update_date /* since is an add */

        /* do not reset when only the curr_pp_end_dt_key is updated */
        when department_staffing_data_row.today_hash_value_without_latest_dt
            != check_on_latest_date_chg.pre_run_hash_value_without_latest_dt
        /* set new update_date when other field(s) are changed for the dept */
        then department_staffing_data_row.new_update_date

        else check_on_latest_date_chg.latest_real_update_date
    end
{%- else %}
    department_staffing_data_row.new_update_date
{%- endif %}
     as update_date

from
    department_staffing_data_row
{%- if is_incremental() %}    
    left join check_on_latest_date_chg
        on department_staffing_data_row.department_id = check_on_latest_date_chg.chk_department_id
{%- endif %}

where 1 = 1     
{%- if is_incremental() %}
    and department_staffing_data_row.hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where department_id = department_staffing_data_row.department_id)
{%- endif %}
