{{ config(meta = {
    'critical': true
}) }}

/* nursing_cost_center_attributes
from STGs and lookups apply budget and nursing indicators
and set nursing_business_report_select_ind and INDs for
Flex target processing and inclusion
*/
with operations_budget_cost_center_current_year as (
    select
        nursing_cc.cost_center_id,
        1 as nursing_current_cost_center_ind
    from {{ ref('lookup_nursing_cost_center') }} as nursing_cc
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on nursing_cc.fiscal_year = nursing_pay_period.fiscal_year
        and nursing_pay_period.latest_pay_period_ind = 1
    group by nursing_cc.cost_center_id
),
cost_center_with_nursing_budget as (
    select
        cost_center_id,
        1 as has_nursing_current_year_budget_ind
    from {{ ref('lookup_nursing_staffing_annual_budget') }} as nursing_cc
    inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
        on nursing_cc.fiscal_year = nursing_pay_period.fiscal_year
        and nursing_pay_period.latest_pay_period_ind = 1
    group by nursing_cc.cost_center_id
),
latest_year_unit_group as (

    select
        lookup_nursing_unit_group_cost_center.cost_center_id,
        lookup_nursing_unit_group.unit_group_id,
        lookup_nursing_unit_group.year_survey_added,
        lookup_nursing_unit_group.cost_center_site_id as unit_group_cost_center_site_id,

	case when
            row_number() over(
                partition by
                     lookup_nursing_unit_group_cost_center.cost_center_id
                order by
                    year_survey_added desc,
                    coalesce(year_survey_ended, 9999) desc
            ) = 1 then 1
            else 0
        end as take_this_unit_group_id_ind

    from {{ ref('lookup_nursing_unit_group_cost_center') }} as lookup_nursing_unit_group_cost_center
    inner join {{ ref('lookup_nursing_unit_group') }} as lookup_nursing_unit_group
        on lookup_nursing_unit_group_cost_center.unit_group_id = lookup_nursing_unit_group.unit_group_id
)

select
    cc.cc_active_worker_cnt,
    cc.cc_active_magnet_worker_cnt,
    cc.cc_active_rn_cnt,
    cc.cost_center_id,
    cc.cost_center_type,
    cc.cost_center_group,
    cc.cost_center_name,
    cc.parent_level,
    cc.cost_center_parent,
    cc.cost_center_display,
    cc.cc_active_ind,
    cc.lvl_1_cc_hier_nm,
    cc.lvl_2_cc_hier_nm,
    cc.lvl_3_cc_hier_nm,
    cc.lvl_4_cc_hier_nm,
    cc.lvl_5_cc_hier_nm,
    cc.lvl_6_cc_hier_nm,
    cc.full_hierarchy_level_path,
    cc.drill_cc_l1,
    cc.drill_cc_l2,
    cc.drill_cc_l3,
    cc.drill_cc_l4,
    cc.drill_cc_l5,
    cc.drill_cc_l6,
    cc.drill_cc_l2_path,
    cc.drill_cc_l3_path,
    cc.drill_cc_l4_path,
    cc.drill_cc_l5_path,
    cc.drill_cc_l6_path,
	cc.full_drill_cc_path,
    cc.surgery_center_cc_ind,
    cc.surgery_center_cc_level_name,
    cc.hospital_cc_ind,
    cc.hospital_cc_level_1_name,
    cc.hospital_cc_level_2_name,
    cc.care_network_cc_ind,
    cc.care_network_cc_level_name,
    cc.room_and_board_rollup,
    cc.hppd_rollup,
    cc.hppd_rollup_short_name,
    cc.room_and_board_ind,
    cc.hppd_ind,
    cc.whuos_rollup,
    cc.cc_has_active_workers_ind,
    cc.cc_has_active_rns_ind,
    operations_budget_cost_center_current_year.nursing_current_cost_center_ind,
    cost_center_with_nursing_budget.has_nursing_current_year_budget_ind,
    case
        when
        coalesce(cc.cc_has_active_rns_ind, 0)
        + coalesce(operations_budget_cost_center_current_year.nursing_current_cost_center_ind, 0)
        + coalesce(cost_center_with_nursing_budget.has_nursing_current_year_budget_ind, 0)
        > 0
        then 1
        else 0
    end as nursing_business_report_select_ind,
    latest_year_unit_group.unit_group_id,
    latest_year_unit_group.unit_group_cost_center_site_id,
    latest_year_unit_group.year_survey_added as unit_group_survey_start_year,
    case
        when cost_center_with_nursing_budget.has_nursing_current_year_budget_ind = 1
        then coalesce(cc_flex_inds.nursing_flex_ind, 1)
        else 0 /* cannot do flex target if we do not  have budget */
    end as nursing_flex_ind,
     /* for cost center's variable state, assume all fixed if not in lookup */
    coalesce(cc_flex_inds.flex_variable_ind, 0) as flex_variable_ind,
    case nursing_flex_ind
        when 1
        then case flex_variable_ind
            when 1
            then coalesce(cc_flex_inds.volume_denominator, 'need volume data point!')
            when 0
            then coalesce('will be ' || cc_flex_inds.volume_denominator, ' n/a')
            else 'unk flex_variable_ind but nursing flex on'
            end
        when 0 /* we do not have a budget or lookup has flex off*/
        then case flex_variable_ind
            when 1
            then coalesce(cc_flex_inds.volume_denominator, 'TBD')
                || ' - but Flex turned off'
            when 0
            then coalesce('would be ' || cc_flex_inds.volume_denominator, ' n/a')
                || ' - but Flex turned off & not variable'
            else null
            end
        end as flex_volume_denominator
from
    {{ ref('stg_nursing_cost_center_drill_and_attributes') }} as cc
    left join operations_budget_cost_center_current_year as operations_budget_cost_center_current_year
        on cc.cost_center_id = operations_budget_cost_center_current_year.cost_center_id
    left join cost_center_with_nursing_budget as cost_center_with_nursing_budget
        on cc.cost_center_id = cost_center_with_nursing_budget.cost_center_id
    left join latest_year_unit_group
        on cc.cost_center_id = latest_year_unit_group.cost_center_id
        and take_this_unit_group_id_ind = 1
    left join {{ ref('lookup_nursing_variable_cost_center') }} as cc_flex_inds
        on cc.cost_center_id = cc_flex_inds.cost_center_id
