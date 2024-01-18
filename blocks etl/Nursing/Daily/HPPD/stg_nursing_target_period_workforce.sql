{{ config(meta = {
    'critical': true
}) }}

with lookup_nursing_target_hppd_skill_mix as (
    select *
    from {{ref('lookup_nursing_target_hppd_skill_mix')}}
),

workforce_targets as (
select
    'HPPDtrgt' as metric_abbreviation,
    null as job_group_id,
    null as metric_grouper,
    cost_center_id,
	fiscal_yr as fiscal_year,
	hppd_cc_target as numerator
from lookup_nursing_target_hppd_skill_mix

union

select
    'HPPDtrgtRN' as metric_abbreviation,
    'StaffNurse' as job_group_id,
    'RN' as metric_grouper,
    cost_center_id,
	fiscal_yr as fiscal_year,
	rn_hppd_cc_target as numerator
from lookup_nursing_target_hppd_skill_mix

union

select
    'HPPDtrgtUAP' as metric_abbreviation,
    'UAP' as job_group_id,
    'UAP' as metric_grouper,
    cost_center_id,
	fiscal_yr as fiscal_year,
	uap_hppd_cc_target as numerator
from lookup_nursing_target_hppd_skill_mix

union

select
    'SKILLMIXtrgtRN' as metric_abbreviation,
    'StaffNurse' as job_group_id,
    'RN' as metric_grouper,
    cost_center_id,
	fiscal_yr,
	rn_skill_mix_target as numerator
from lookup_nursing_target_hppd_skill_mix

union

select
    'SKILLMIXtrgtUAP' as metric_abbreviation,
    'UAP' as job_group_id,
    'UAP' as metric_grouper,
    cost_center_id,
	fiscal_yr as fiscal_year,
	uap_skill_mix_target as numerator
from lookup_nursing_target_hppd_skill_mix
)

select
    metric_abbreviation,
    master_pay_periods.end_dt_key as metric_dt_key,
    null as worker_id,
    cost_center_id,
	null as cost_center_site_id,
    null as job_code,
    job_group_id,
    metric_grouper,
    numerator,
    null::numeric as denominator,
    numerator as row_metric_calculation
from workforce_targets
inner join {{source('cdw', 'master_pay_periods')}} as master_pay_periods
    on workforce_targets.fiscal_year = master_pay_periods.fiscal_year
