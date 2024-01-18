{{
    config(
        materialized='incremental',
        unique_key = 'tdl_id'
    )
}}

-- CTE's

with fin_class_dt2 as (
    select
        tx_id,
        original_fin_class as action_fin_class2
    from {{ source('clarity_ods', 'clarity_tdl_tran') }} where detail_type in (2)
),

fin_class_dt4 as (
    select
        tx_id,
        original_fin_class as action_fin_class4
    from {{ source('clarity_ods', 'clarity_tdl_tran') }} where detail_type in (4)
)

-- CHARGE TRANSACTIONS
select
    clarity_tdl_tran.tdl_id,
    clarity_tdl_tran.tx_id,
    clarity_tdl_tran.tx_id as chrg_tx_id,
    clarity_tdl_tran.tx_num,
    clarity_tdl_tran.detail_type,
    clarity_tdl_tran.post_date,
    clarity_tdl_tran.orig_post_date,
    clarity_tdl_tran.orig_post_date as chrg_post_date,
    clarity_tdl_tran.orig_service_date,
    clarity_tdl_tran.orig_service_date as chrg_service_date,
    clarity_tdl_tran.tran_type, -- 1=Charge; 2=Payment; 3=Adjustment
    clarity_tdl_tran.match_trx_id,
    clarity_tdl_tran.match_tx_type,
    clarity_tdl_tran.match_proc_id,
    clarity_tdl_tran.match_loc_id,
    clarity_tdl_tran.match_prov_id,
    clarity_tdl_tran.charge_slip_number,
    clarity_tdl_tran.account_id,
    clarity_tdl_tran.int_pat_id as pat_id,
    clarity_tdl_tran.amount,
    clarity_tdl_tran.patient_amount,
    clarity_tdl_tran.insurance_amount,
    clarity_tdl_tran.user_id,
    REPLACE(clarity_tdl_tran.debit_gl_num, '|', '') as debit_gl_num,
    REPLACE(clarity_tdl_tran.credit_gl_num, '|', '') as credit_gl_num,
    clarity_tdl_tran.billing_provider_id as billing_prov_id,
    clarity_tdl_tran.performing_prov_id as servicing_prov_id,
    clarity_tdl_tran.original_cvg_id as original_cvg_id,
    case
        when clarity_tdl_tran.original_fin_class = 4 then -2
        else clarity_tdl_tran.original_plan_id
    end as original_plan_id,
    COALESCE(clarity_tdl_tran.original_payor_id, -2) as original_payor_id,
    clarity_tdl_tran.original_fin_class as original_fin_class,
    case
        when clarity_tdl_tran.detail_type in (1, 10) then null
        else COALESCE(clarity_tdl_tran.match_payor_id, -2)
    end as action_payor_id,
    case
        when clarity_tdl_tran.detail_type in (1, 10) then null
        when
            clarity_tdl_tran.detail_type = 20 then CAST(fin_class_dt2.action_fin_class2 as int)
        when
            clarity_tdl_tran.detail_type = 21 then CAST(fin_class_dt4.action_fin_class4 as int)
    end as action_fin_class,
    clarity_tdl_tran.proc_id as proc_id,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then clarity_tdl_tran.proc_id
        else clarity_tdl_tran.match_proc_id
    end as derived_proc_id,
    clarity_tdl_tran.procedure_quantity,
    clarity_tdl_tran.cpt_code,
    clarity_tdl_tran.modifier_one as modifier_one,
    clarity_tdl_tran.modifier_two as modifier_two,
    clarity_tdl_tran.modifier_three as modifier_three,
    clarity_tdl_tran.modifier_four as modifier_four,
    clarity_tdl_tran.dx_one_id,
    clarity_tdl_tran.dx_two_id,
    clarity_tdl_tran.dx_three_id,
    clarity_tdl_tran.dx_four_id,
    clarity_tdl_tran.dx_five_id,
    clarity_tdl_tran.dx_six_id,
    clarity_tdl_tran.loc_id,
    clarity_tdl_tran.dept_id,
    clarity_tdl_tran.pos_id,
    clarity_tdl_tran.copay_indicator,
    clarity_tdl_tran.referral_source_id as referral_source_id,
    clarity_tdl_tran.referral_id as referral_id,
    clarity_tdl_tran.posting_batch_num,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then clarity_tdl_tran.base_units
        else 0
    end as base_units,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then clarity_tdl_tran.timed_units
        else 0
    end as timed_units,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then clarity_tdl_tran.phy_status_units
        else 0
    end as phy_status_units,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then clarity_tdl_tran.emergency_units
        else 0
    end as emergency_units,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then clarity_tdl_tran.age_units
        else 0
    end as age_units,
    case
        when SUBSTR(clarity_tdl_tran.cpt_code, 6, 1) = 'A'
            and clarity_tdl_tran.billing_provider_id = clarity_tdl_tran.performing_prov_id
            and clarity_tdl_tran.detail_type = 1 then 1
        when SUBSTR(clarity_tdl_tran.cpt_code, 6, 1) = 'A'
            and clarity_tdl_tran.billing_provider_id = clarity_tdl_tran.performing_prov_id
            and clarity_tdl_tran.detail_type = 10 then -1
    end as caa_anes_case_count,
    case
        when SUBSTR(clarity_tdl_tran.cpt_code, 6, 1) = 'A'
            and clarity_tdl_tran.billing_provider_id = clarity_tdl_tran.performing_prov_id
            and clarity_tdl_tran.detail_type = 1
            then clarity_tdl_tran.base_units + clarity_tdl_tran.phy_status_units + clarity_tdl_tran.timed_units
        when SUBSTR(clarity_tdl_tran.cpt_code, 6, 1) = 'A'
            and clarity_tdl_tran.billing_provider_id = clarity_tdl_tran.performing_prov_id
            and clarity_tdl_tran.detail_type = 10
            then (
                clarity_tdl_tran.base_units + clarity_tdl_tran.phy_status_units + clarity_tdl_tran.timed_units
            ) * -1
        when clarity_tdl_tran.cpt_code in ('99100', '99140', '99116', '99135')
            and clarity_tdl_tran.billing_provider_id = clarity_tdl_tran.performing_prov_id
            and clarity_tdl_tran.detail_type in (
                1, 10
            ) then clarity_tdl_tran.procedure_quantity
    end as caa_anes_units,
    clarity_tdl_tran.visit_number,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) and clarity_tdl_tran.allowed_amount is null then clarity_tdl_tran.amount
        when
            clarity_tdl_tran.detail_type = 1 and clarity_tdl_tran.amount < clarity_tdl_tran.allowed_amount then clarity_tdl_tran.amount
        when
            clarity_tdl_tran.detail_type = 1 and clarity_tdl_tran.amount >= clarity_tdl_tran.allowed_amount then clarity_tdl_tran.allowed_amount
        when
            clarity_tdl_tran.detail_type = 10 and -1 * clarity_tdl_tran.amount < clarity_tdl_tran.allowed_amount then clarity_tdl_tran.amount
        when
            clarity_tdl_tran.detail_type = 10 and -1 * clarity_tdl_tran.amount >= clarity_tdl_tran.allowed_amount then -1 * clarity_tdl_tran.allowed_amount
        else 0
    end as exp_vs_act_pmt_variance,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then clarity_tdl_tran.allowed_amount
        else 0
    end as allowed_amount,
    clarity_tdl_tran.customer_item_one as program_id,
    clarity_tdl_tran.tx_comment,
    case
        when
            clarity_tdl_tran.detail_type in (1, 10) then clarity_tdl_tran.amount
        else 0
    end as charge_amt,
    case
        when clarity_tdl_tran.detail_type in (20) then clarity_tdl_tran.amount
        else 0
    end as pmt_pract,
    0 as pmt_gl,
    case
        when
            clarity_tdl_tran.detail_type in (
                21
            ) and clarity_epg_match.proc_group_name in (
                'BD EXP ADJ', 'BD REC ADJ', 'BD SYS ADJ'
            ) then clarity_tdl_tran.amount
        else 0
    end as agg_bad_debt_adj_amount,
    case
        when
            clarity_tdl_tran.detail_type in (
                21
            ) and clarity_epg_match.proc_group_name in (
                'CONTRACT ADJ', 'CAP ADJ', 'CAP FEES ADJ', 'ESCHEAT ADJ'
            ) then clarity_tdl_tran.amount
        else 0
    end as agg_contract_adj_amount,
    case
        when
            clarity_tdl_tran.detail_type in (
                21
            ) and (clarity_epg_match.proc_group_name in (
                'FIN/INT ADJ',
                'INTERCO REFUND',
                'OTHER ADJ',
                'REFUND ADJ',
                'PYMT',
                'MED-REC ADJ',
                'SYS ADJ',
                'MAP PYMT IBC',
                'MAP PYMT KMH'
            ) or clarity_epg_match.proc_group_name is null)
            then clarity_tdl_tran.amount
        else 0
    end as agg_other_adj_amount,
    case
        when clarity_tdl_tran.detail_type in (21) then clarity_tdl_tran.amount
        else 0
    end as total_adj,
    clarity_tdl_tran.orig_price,
    clarity_tdl_tran.price_contract_id,
    clarity_tdl_tran.contract_discount,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then COALESCE(clarity_tdl_tran.rvu_work, 0)
        else 0
    end as base_work_rvu,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then COALESCE(clarity_tdl_tran.rvu_overhead, 0)
        else 0
    end as base_overhead_rvu,
    case
        when
            clarity_tdl_tran.detail_type in (
                1, 10
            ) then COALESCE(clarity_tdl_tran.rvu_malpractice, 0)
        else 0
    end as base_malpractice_rvu,
    COALESCE(
        clarity_tdl_tran.rvu_work, 0
    ) * clarity_tdl_tran.procedure_quantity * 1 as work_rvu,
    COALESCE(
        clarity_tdl_tran.rvu_overhead, 0
    ) * clarity_tdl_tran.procedure_quantity * 1 as overhead_rvu,
    COALESCE(
        clarity_tdl_tran.rvu_malpractice, 0
    ) * clarity_tdl_tran.procedure_quantity * 1 as malpractice_rvu,
    case
        when clarity_tdl_tran.detail_type = 1 then
            COALESCE(clarity_tdl_tran.base_units, 0) + COALESCE(timed_units, 0)
            + COALESCE(
                phy_status_units, 0
            ) + COALESCE(emergency_units, 0) + COALESCE(age_units, 0)
        when clarity_tdl_tran.detail_type = 10 then
            (COALESCE(clarity_tdl_tran.base_units, 0) + COALESCE(timed_units, 0)
                + COALESCE(
                    phy_status_units, 0
                ) + COALESCE(emergency_units, 0) + COALESCE(age_units, 0)
            ) * -1
        else 0
    end as anesthesia_rvu,
    case
        when clarity_tdl_tran.detail_type = 1 then
            (
                COALESCE(
                    clarity_tdl_tran.rvu_work, 0
                ) * clarity_tdl_tran.procedure_quantity * 1
            )
            + (
                COALESCE(
                    clarity_tdl_tran.rvu_overhead, 0
                ) * clarity_tdl_tran.procedure_quantity * 1
            )
            + (
                COALESCE(
                    clarity_tdl_tran.rvu_malpractice, 0
                ) * clarity_tdl_tran.procedure_quantity * 1
            )
            + (
                COALESCE(
                    base_units, 0
                ) + COALESCE(timed_units, 0) + COALESCE(phy_status_units, 0)
                + COALESCE(emergency_units, 0) + COALESCE(age_units, 0))
        when clarity_tdl_tran.detail_type = 10 then
            (
                COALESCE(
                    clarity_tdl_tran.rvu_work, 0
                ) * clarity_tdl_tran.procedure_quantity * 1
            )
            + (
                COALESCE(
                    clarity_tdl_tran.rvu_overhead, 0
                ) * clarity_tdl_tran.procedure_quantity * 1
            )
            + (
                COALESCE(
                    clarity_tdl_tran.rvu_malpractice, 0
                ) * clarity_tdl_tran.procedure_quantity * 1
            )
            + (
                (
                    COALESCE(
                        base_units, 0
                    ) + COALESCE(timed_units, 0) + COALESCE(phy_status_units, 0)
                    + COALESCE(emergency_units, 0) + COALESCE(age_units, 0)
                ) * -1)
        else 0
    end as total_rvu,
    clarity_tdl_tran.pat_enc_csn_id,
    clarity_tdl_tran.void_user_id,
    clarity_tdl_tran.r_new_chg_tx_id,
    clarity_tdl_tran.transferred_tx_id,
    clarity_tdl_tran.is_tranfered_yn,
    clarity_tdl_tran.reference_number,
    clarity_tdl_tran.relative_value_unit,
    clarity_tdl_tran.tdl_extract_date
from
    {{ source('clarity_ods', 'clarity_tdl_tran') }} as clarity_tdl_tran
inner join
    {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions on
        arpb_transactions.tx_id = clarity_tdl_tran.tx_id
left join
    {{ source('clarity_ods', 'clarity_eap') }} as clarity_eap on
        clarity_eap.proc_id = clarity_tdl_tran.proc_id
left join
    {{ source('clarity_ods', 'clarity_eap') }} as clarity_eap_match on
        clarity_eap_match.proc_id = clarity_tdl_tran.match_proc_id
left join
    {{ source('clarity_ods', 'clarity_epg') }} as clarity_epg_match on
        clarity_epg_match.proc_group_id = clarity_eap_match.proc_group_id
left join
    fin_class_dt2
    on clarity_tdl_tran.match_trx_id = fin_class_dt2.tx_id
left join
    fin_class_dt4
    on clarity_tdl_tran.match_trx_id = fin_class_dt4.tx_id
where
    clarity_tdl_tran.detail_type in (1, 10, 20, 21)
    and {{ limit_dates_for_dev(ref_date = 'clarity_tdl_tran.post_date') }}
{% if is_incremental() %}
    and DATE(
        clarity_tdl_tran.tdl_extract_date
    ) >= (select MAX(DATE(tdl_extract_date) ) from {{ this }})
{% endif %}    

union all

-- UNDISTRIBUTED TRANSACTIONS
select
    clarity_tdl_tran.tdl_id,
    clarity_tdl_tran.tx_id,
    null as chrg_tx_id,
    clarity_tdl_tran.tx_num,
    clarity_tdl_tran.detail_type,
    clarity_tdl_tran.post_date,
    clarity_tdl_tran.orig_post_date,
    null as chrg_post_date,
    clarity_tdl_tran.orig_service_date,
    null as chrg_service_date,
    clarity_tdl_tran.tran_type, -- 1=Charge; 2=Payment; 3=Adjustment
    clarity_tdl_tran.match_trx_id,
    clarity_tdl_tran.match_tx_type,
    clarity_tdl_tran.match_proc_id,
    clarity_tdl_tran.match_loc_id,
    clarity_tdl_tran.match_prov_id,
    clarity_tdl_tran.charge_slip_number,
    clarity_tdl_tran.account_id,
    clarity_tdl_tran.int_pat_id as pat_id,
    clarity_tdl_tran.amount,
    clarity_tdl_tran.patient_amount,
    clarity_tdl_tran.insurance_amount,
    clarity_tdl_tran.user_id,
    REPLACE(clarity_tdl_tran.debit_gl_num, '|', '') as debit_gl_num,
    REPLACE(clarity_tdl_tran.credit_gl_num, '|', '') as credit_gl_num,
    clarity_tdl_tran.billing_provider_id as billing_prov_id,
    clarity_tdl_tran.performing_prov_id as servicing_prov_id,
    clarity_tdl_tran.original_cvg_id as original_cvg_id,
    case
        when clarity_tdl_tran.original_fin_class = 4 then -2
        else clarity_tdl_tran.original_plan_id
    end as original_plan_id,
    COALESCE(clarity_tdl_tran.original_payor_id, -2) as original_payor_id,
    clarity_tdl_tran.original_fin_class as original_fin_class,
    case
        when
            clarity_tdl_tran.detail_type in (
                2, 3, 4, 5, 6, 11, 12, 13, 30, 31, 32, 33
            ) then COALESCE(clarity_tdl_tran.cur_payor_id, -2)
        when
            clarity_tdl_tran.detail_type in (
                22, 23
            ) then COALESCE(clarity_tdl_tran.match_payor_id, -2)
    end as action_payor_id,
    case
        when
            clarity_tdl_tran.detail_type in (
                2, 3, 4, 5, 6, 11, 12, 13, 30, 31, 32, 33
            )
            then CAST(clarity_tdl_tran.cur_fin_class as int)
        when
            clarity_tdl_tran.detail_type = 22 then CAST(fin_class_dt2.action_fin_class2 as int)
        when
            clarity_tdl_tran.detail_type = 23 then CAST(fin_class_dt4.action_fin_class4 as int)
    end as action_fin_class,
    clarity_tdl_tran.proc_id as proc_id,
    case
        when
            clarity_tdl_tran.detail_type in (
                2, 3, 4, 5, 6, 11, 12, 13, 30, 31, 32, 33
            ) then clarity_tdl_tran.proc_id
        else clarity_tdl_tran.match_proc_id
    end as derived_proc_id,
    0 as procedure_quantity,
    null as cpt_code,
    null as modifier_one,
    null as modifier_two,
    null as modifier_three,
    null as modifier_four,
    null as dx_one_id,
    null as dx_two_id,
    null as dx_three_id,
    null as dx_four_id,
    null as dx_five_id,
    null as dx_six_id,
    clarity_tdl_tran.loc_id,
    clarity_tdl_tran.dept_id,
    clarity_tdl_tran.pos_id,
    clarity_tdl_tran.copay_indicator,
    null as referral_source_id,
    null as referral_id,
    clarity_tdl_tran.posting_batch_num,
    0 as base_units,
    0 as timed_units,
    0 as phy_status_units,
    0 as emergency_units,
    0 as age_units,
    0 as caa_anes_case_count,
    0 as caa_anes_units,
    clarity_tdl_tran.visit_number,
    0 as exp_vs_act_pmt_variance,
    0 as allowed_amount,
    clarity_tdl_tran.customer_item_one as program_id,
    clarity_tdl_tran.tx_comment,
    0 as charge_amt,
    case
        when
            clarity_tdl_tran.detail_type in (
                2, 5, 11, 22, 32, 33
            ) then clarity_tdl_tran.amount
        else 0
    end as pmt_pract,
    case
        when
            clarity_tdl_tran.detail_type in (
                2, 3, 11, 33
            )
            and (
                (
                    clarity_epg.proc_group_name in (
                        'PYMT', 'PREPAY', 'PYMT-REV ADJ'
                    ) and detail_type = 3
                )
                or (clarity_tdl_tran.detail_type in (2, 11))
                or (
                    clarity_tdl_tran.detail_type = 33 and clarity_eap_match.proc_code in (
                        '1100', '1038', '1015', '5041', '5093', '5060', '5094', '5065', '5095'
                    )
                )
            )
            then clarity_tdl_tran.amount
        else 0
    end as pmt_gl,
    case
        when
            (
                clarity_tdl_tran.detail_type in (
                    3, 4, 6, 12, 13, 30, 31
                ) and clarity_epg.proc_group_name in (
                    'BD EXP ADJ', 'BD REC ADJ', 'BD SYS ADJ'
                )
            )
            or (
                clarity_tdl_tran.detail_type in (
                    23
                ) and clarity_epg_match.proc_group_name in (
                    'BD EXP ADJ', 'BD REC ADJ', 'BD SYS ADJ'
                )
            ) then clarity_tdl_tran.amount
        else 0
    end as agg_bad_debt_adj_amount,
    case
        when
            (
                clarity_tdl_tran.detail_type in (
                    3, 4, 6, 12, 13, 30, 31
                ) and clarity_epg.proc_group_name in (
                    'CONTRACT ADJ', 'CAP ADJ', 'CAP FEES ADJ', 'ESCHEAT ADJ'
                )
            )
            or (
                clarity_tdl_tran.detail_type in (
                    23
                ) and clarity_epg_match.proc_group_name in (
                    'CONTRACT ADJ', 'CAP ADJ', 'CAP FEES ADJ', 'ESCHEAT ADJ'
                )
            ) then clarity_tdl_tran.amount
        else 0
    end as agg_contract_adj_amount,
    case
        when
            (
                (
                    clarity_tdl_tran.detail_type in (
                        3, 4, 6, 12, 13, 30, 31
                    ) and (clarity_epg.proc_group_name in (
                        'FIN/INT ADJ',
                        'INTERCO REFUND',
                        'OTHER ADJ',
                        'REFUND ADJ',
                        'PYMT',
                        'MED-REC ADJ',
                        'SYS ADJ',
                        'MAP PYMT IBC',
                        'MAP PYMT KMH',
                        ' PROCEDURES'
                    ) or clarity_epg.proc_group_name is null)
                )
                or (
                    clarity_tdl_tran.detail_type in (
                        23
                    ) and (clarity_epg_match.proc_group_name in (
                        'FIN/INT ADJ',
                        'INTERCO REFUND',
                        'OTHER ADJ',
                        'REFUND ADJ',
                        'PYMT',
                        'MED-REC ADJ',
                        'SYS ADJ',
                        'MAP PYMT IBC',
                        'MAP PYMT KMH',
                        ' PROCEDURES'
                    ) or clarity_epg_match.proc_group_name is null)
                )
            ) then clarity_tdl_tran.amount
        else 0
    end as agg_other_adj_amount,
    case
        when
            clarity_tdl_tran.detail_type in (
                3, 4, 6, 12, 13, 23, 30, 31
            ) then clarity_tdl_tran.amount
        else 0
    end as total_adj,
    0 as orig_price,
    0 as price_contract_id,
    0 as contract_discount,
    0 as base_work_rvu,
    0 as base_overhead_rvu,
    0 as base_malpractice_rvu,
    0 as work_rvu,
    0 as overhead_rvu,
    0 as malpractice_rvu,
    0 as anesthesia_rvu,
    0 as total_rvu,
    clarity_tdl_tran.pat_enc_csn_id,
    clarity_tdl_tran.void_user_id,
    clarity_tdl_tran.r_new_chg_tx_id,
    clarity_tdl_tran.transferred_tx_id,
    clarity_tdl_tran.is_tranfered_yn,
    clarity_tdl_tran.reference_number,
    clarity_tdl_tran.relative_value_unit,
    clarity_tdl_tran.tdl_extract_date
from
    {{ source('clarity_ods', 'clarity_tdl_tran') }} as clarity_tdl_tran
inner join
    {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions on
        arpb_transactions.tx_id = clarity_tdl_tran.tx_id
left join
    {{ source('clarity_ods', 'clarity_eap') }} as clarity_eap on
        clarity_eap.proc_id = clarity_tdl_tran.proc_id
left join
    {{ source('clarity_ods', 'clarity_eap') }} as clarity_eap_match on
        clarity_eap_match.proc_id = clarity_tdl_tran.match_proc_id
left join
    {{ source('clarity_ods', 'clarity_epg') }} as clarity_epg_match on
        clarity_epg_match.proc_group_id = clarity_eap_match.proc_group_id
left join
    {{ source('clarity_ods', 'clarity_epg') }} as clarity_epg on
        clarity_epg.proc_group_id = clarity_eap.proc_group_id
left join
    fin_class_dt2 on
        clarity_tdl_tran.match_trx_id = fin_class_dt2.tx_id
left join
    fin_class_dt4 on
        clarity_tdl_tran.match_trx_id = fin_class_dt4.tx_id
where
    clarity_tdl_tran.detail_type in (
        2, 3, 4, 5, 6, 11, 12, 13, 22, 23, 30, 31, 32, 33
    )
    and {{ limit_dates_for_dev(ref_date = 'clarity_tdl_tran.post_date') }}
{% if is_incremental() %}
    and DATE(
        clarity_tdl_tran.tdl_extract_date
    ) >= (select MAX(DATE(tdl_extract_date) ) from {{ this }})
{% endif %}
