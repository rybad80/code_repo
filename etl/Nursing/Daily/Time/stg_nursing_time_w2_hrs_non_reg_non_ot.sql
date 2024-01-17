{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_time_w2_hrs_non_reg_non_ot
Collect the non-regular and non-overtime total hours by job group,
and for the Ambulatory RN indirect total for the cost center
which will be used for the FlexFTEjr granularity adjustment in flex_p6
Additionally breakout the grand totals of counted non-Direct staff nurse time.
*/
with
nondirect_metric as (
    select
        addend_metric as nondirect_metric_abbreviation,
        1 as nondirect_metric_ind,
        case
            when summed_metric = 'IndirectProductiveHrs'
            then 1 else 0
        end as indirect_ind
    from
        {{ ref('lookup_nursing_metric_sum_mapping') }}
	where
        summed_metric in (
            'TimeNonProdHrs',
            'IndirectProductiveHrs'
            )
        and operation = 'add'
),

indirect_ambulatory_rn_component as (
    /* only actuals for completed pay periods for Flex ambulatory RN granularity */
    select
        'IndirectHrsAmbRN' as metric_abbreviation,
        nursing_pay_period.pp_end_dt_key as metric_dt_key,
        non_reg_or_ot_hrs.cost_center_id,
        'AmbulatoryRN' as job_group_id,
        sum(non_reg_or_ot_hrs.non_direct_hours) as indirect_amb_rn_hrs
    from
        {{ ref('stg_nursing_non_direct_p1_pp_hours') }} as non_reg_or_ot_hrs
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on non_reg_or_ot_hrs.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
        inner join {{ ref('lookup_paycode_attribute') }} as paycode_attribute
            on lower(non_reg_or_ot_hrs.timereport_paycode) = lower(paycode_attribute.wf_kronos_code)
            and paycode_attribute.attribute_type like 'Selection%'
        inner join nondirect_metric
            on paycode_attribute.attribute_value || 'Hrs'  = nondirect_metric_abbreviation
            and nondirect_metric.indirect_ind = 1
    where
        non_reg_or_ot_hrs.ambulatory_rn_job_ind = 1
        and nursing_pay_period.prior_pay_period_ind = 1
    group by
        nursing_pay_period.pp_end_dt_key,
        non_reg_or_ot_hrs.cost_center_id
),

nondirect_component as (
    select
        case nursing_pay_period.future_pay_period_ind when 1 then 'Upcoming'
            else '' end || paycode_attribute.attribute_value
        || 'Hrs' as metric_abbreviation,
        nursing_pay_period.pp_end_dt_key as metric_dt_key,
        nursing_pay_period.future_pay_period_ind,
        non_reg_or_ot_hrs.cost_center_id,
        non_reg_or_ot_hrs.job_group_id,
        non_reg_or_ot_hrs.staff_nurse_ind,
        case
            when nondirect_metric.nondirect_metric_ind = 1
            then sum(non_reg_or_ot_hrs.non_direct_hours)
        end as nondirect_sum_period_hrs,
        sum(non_reg_or_ot_hrs.non_direct_hours) as sum_period_hrs
    from
        {{ ref('stg_nursing_non_direct_p1_pp_hours') }} as non_reg_or_ot_hrs
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on non_reg_or_ot_hrs.pp_end_dt_key = nursing_pay_period.pp_end_dt_key
        inner join {{ ref('lookup_paycode_attribute') }} as paycode_attribute
            on lower(non_reg_or_ot_hrs.timereport_paycode) = lower(paycode_attribute.wf_kronos_code)
            and (paycode_attribute.attribute_type like 'Selection%'
            or paycode_attribute.attribute_type = 'PPLbreakout')
        left join nondirect_metric
            on paycode_attribute.attribute_value || 'Hrs'  = nondirect_metric_abbreviation

    group by
        nursing_pay_period.pp_end_dt_key,
        paycode_attribute.attribute_value,
        non_reg_or_ot_hrs.cost_center_id,
        nondirect_metric_ind,
		non_reg_or_ot_hrs.job_group_id,
        non_reg_or_ot_hrs.staff_nurse_ind,
        nursing_pay_period.future_pay_period_ind,
        paycode_attribute.attribute_type
),

union_set as (

    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        job_group_id as metric_grouper,
        sum_period_hrs
    from
        nondirect_component

    union all

    select
        metric_abbreviation,
        metric_dt_key,
        cost_center_id,
        job_group_id,
        job_group_id as metric_grouper,
        indirect_amb_rn_hrs
    from
        indirect_ambulatory_rn_component

    union all

    select
        metric_abbreviation, /* sNurseNonDirectHrs */
        pp_end_dt_key as metric_dt_key,
        cost_center_id,
        job_group_id,
        job_group_id as metric_grouper,
        numerator
    from
        {{ ref('stg_nursing_non_direct_p2_pp_subset') }}
    where
        metric_abbreviation = 'sNurseNonDirectHrs'
)

select
    metric_abbreviation,
    metric_dt_key,
    null as worker_id,
    cost_center_id,
    null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    sum_period_hrs as numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from
    union_set
