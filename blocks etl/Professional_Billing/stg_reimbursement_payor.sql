select
    arpb_transactions.tx_id,
    coverage.subscr_num as member_id,
    fc_original.financial_class_name as original_fin_class_name,
    COALESCE(clarity_epm.payor_name, 'SELFPAY') as cur_payor_name,
    COALESCE(clarity_fc.financial_class_name, 'Self-Pay') as cur_fin_class,
    COALESCE(CAST(clarity_fc.internal_id as int), 4) as cur_fin_class_id,
    COALESCE(
        clarity_epp.benefit_plan_name, 'SELFPAY'
    ) as cur_benefit_plan_name,
    COALESCE(clarity_epp.benefit_plan_id, -2) as cur_benefit_plan_id,
    COALESCE(
        epp_original.benefit_plan_name, 'SELFPAY'
    ) as original_plan_name,
    COALESCE(epp_original.benefit_plan_id, -2) as original_plan_id,
    COALESCE(epm_original.payor_name, 'SELFPAY') as original_payor_name
from
    {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions
left join {{ source('clarity_ods', 'coverage') }} as coverage on
    coverage.coverage_id = arpb_transactions.coverage_id
left join {{ source('clarity_ods', 'clarity_epm') }} as clarity_epm on
    clarity_epm.payor_id = arpb_transactions.payor_id
left join {{ source('clarity_ods', 'clarity_fc') }} as clarity_fc on
    clarity_fc.internal_id = clarity_epm.financial_class
left join {{ source('clarity_ods', 'clarity_epp') }} as clarity_epp on
    clarity_epp.benefit_plan_id = coverage.plan_id
left join {{ source('clarity_ods', 'coverage') }} as coverage_original
    on coverage_original.coverage_id = arpb_transactions.original_cvg_id
left join {{ source('clarity_ods', 'clarity_epp') }} as epp_original
    on epp_original.benefit_plan_id = coverage_original.plan_id
left join {{ source('clarity_ods', 'clarity_epm') }} as epm_original
    on epm_original.payor_id = arpb_transactions.original_epm_id
left join {{ source('clarity_ods', 'clarity_fc') }} as fc_original
    on fc_original.internal_id = arpb_transactions.original_fc_c
left join {{ source('clarity_ods', 'referral') }} as referral
    on referral.referral_id = arpb_transactions.referral_id
where
    arpb_transactions.tx_type_c = 1
    and arpb_transactions.void_date is null
