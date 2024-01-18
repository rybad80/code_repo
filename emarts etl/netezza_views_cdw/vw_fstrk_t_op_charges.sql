with br_cls_mojrnl_nums as (
    select
        phi.branch_num,
        phi.cls_dt_key,
        phi.last_mo_sales_journal_num,
        lag(
            phi.last_mo_sales_journal_num,
            1,
            ('0' :: numeric) :: numeric(18, 0)
        ) over (
            partition by phi.branch_num
            order by
                phi.last_mo_sales_journal_num
        ) as last_val
    from
        {{source('cdw', 'period_end_hi_hc')}} as phi
    order by
        phi.last_mo_sales_journal_num
),
phi as (
    select
        br_cls_mojrnl_nums.branch_num,
        br_cls_mojrnl_nums.cls_dt_key,
        br_cls_mojrnl_nums.last_mo_sales_journal_num,
        br_cls_mojrnl_nums.last_val
    from
        br_cls_mojrnl_nums
),
negmd as (
    select
        md.dt_key,
        md.full_dt as neg_full_dt
    from
        {{source('cdw', 'master_date')}} as md
    where
        (md.dt_key = -1)
),
phase1 as (
    select
        100 as facility_code,
        case
            when (v.enc_id isnull) then '-9999999999' :: numeric(10, 0)
            else v.enc_id
        end as account_number,
        c.cost_cntr_cd as revenue_center_code,
        (
            fhc.prod_num || case
                when (fhc.ndc_cd notnull) then ('NDC' :: "VARCHAR" || fhc.ndc_cd)
                else '' :: "VARCHAR"
            end
        ) as activity_code,
        case
            when (fhc.svc_dt_key > 0) then date(("VARCHAR"(fhc.svc_dt_key)) :: varchar(8))
            else negmd.neg_full_dt
        end as service_date,
        case
            when (fhc.post_dt_key > 0) then date(("VARCHAR"(fhc.post_dt_key)) :: varchar(8))
            else negmd.neg_full_dt
        end as posting_date,
        fhc.proc_qty as procedure_quantity,
        p.proc_cd as hcpcs_code,
        fhc.amt as detail_total_charges,
        '' as cpt_modifier_1,
        '' as cpt_modifier_2,
        '' as cpt_modifier_3,
        v.enc_id as series_encounter_number,
        '' as start_time,
        '' as stop_time,
        svc_prov.prov_id as service_provider,
        svc_prov.prov_id as billing_provider,
        dept.dept_id as department_id,
        pyr.payor_id as original_payor_id,
        '' as account_id,
        '' as udf_asa_cpt_cd,
        '' as udf_caa_anes_units,
        '' as udf_caa_anes_case_cnt,
        phi.cls_dt_key as udf_post_period,
        -1 as professional_flag,
        dict2.src_id as division,
        dept.specialty,
        fhc.num_of_vials as num_vials,
        fhc.ndc_cd as ndc_code,
        fhc.rx_avg_wh_price as wholesale_price,
        fhc.rx_last_cost as last_cost,
        fhc.rx_reg_cost as regular_cost,
        fhc.rx_avg_cost as average_cost,
        fhc.match_hc_tx_id as invoice_number,
        fhc.src_pat_id,
        fhc.branch_num,
        fhc.journal_num
    from
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                {{source('cdw', 'fact_transaction_hc')}} as fhc
                                                left join phi on (
                                                    (
                                                        (phi.branch_num = fhc.branch_num)
                                                        and (
                                                            (
                                                                (fhc.journal_num <= phi.last_mo_sales_journal_num)
                                                                and (fhc.journal_num > phi.last_val)
                                                            )
                                                            and (phi.branch_num = fhc.branch_num)
                                                        )
                                                    )
                                                )
                                            )
                                            left join {{source('cdw', 'provider')}} as svc_prov on ((fhc.svc_prov_key = svc_prov.prov_key))
                                        )
                                        left join negmd on ((-1 = negmd.dt_key))
                                    )
                                    left join {{source('cdw', 'procedure')}} as p on ((fhc.proc_key = p.proc_key))
                                )
                                left join {{source('cdw', 'visit')}} as v on ((fhc.visit_key = v.visit_key))
                            )
                            left join {{source('cdw', 'payor')}} as pyr on ((fhc.payor_key = pyr.payor_key))
                        )
                        left join {{source('cdw', 'department')}} as dept on ((fhc.dept_key = dept.dept_key))
                    )
                    left join {{source('cdw', 'cdw_dictionary')}} as dict2 on (
                        (
                            (dict2.dict_nm = dept.rpt_grp_10)
                            and (dict2.dict_cat_key = 100333)
                        )
                    )
                )
                left join {{source('cdw', 'patient')}} as pat on ((fhc.pat_key = pat.pat_key))
            )
            left join {{source('cdw', 'cost_center')}} as c on ((fhc.cost_cntr_key = c.cost_cntr_key))
        )
    where
        (upper(fhc.trans_type) = 'C' :: "VARCHAR")
),
phase2 as (
    select
        phase1.facility_code,
        phase1.account_number,
        phase1.revenue_center_code,
        phase1.activity_code,
        phase1.service_date,
        phase1.posting_date,
        phase1.procedure_quantity,
        phase1.hcpcs_code,
        phase1.detail_total_charges,
        phase1.cpt_modifier_1,
        phase1.cpt_modifier_2,
        phase1.cpt_modifier_3,
        phase1.series_encounter_number,
        phase1.start_time,
        phase1.stop_time,
        phase1.service_provider,
        phase1.billing_provider,
        phase1.department_id,
        phase1.original_payor_id,
        phase1.account_id,
        phase1.udf_asa_cpt_cd,
        phase1.udf_caa_anes_units,
        phase1.udf_caa_anes_case_cnt,
        phase1.udf_post_period,
        phase1.professional_flag,
        phase1.division,
        phase1.specialty,
        phase1.num_vials,
        phase1.ndc_code,
        phase1.wholesale_price,
        phase1.last_cost,
        phase1.regular_cost,
        phase1.average_cost,
        phase1.invoice_number,
        phase1.src_pat_id,
        phase1.branch_num,
        phase1.journal_num,
        int4(
            btrim(
                substr(
                    ("VARCHAR"(phase1.udf_post_period)) :: varchar(18),
                    1,
                    6
                )
            )
        ) as postperiodint,
        int4(
            btrim(
                substr(
                    ("VARCHAR"(phase1.udf_post_period)) :: varchar(18),
                    7,
                    2
                )
            )
        ) as postperioddaydint
    from
        phase1
),
phase3 as (
    select
        phase2.facility_code,
        phase2.account_number,
        phase2.revenue_center_code,
        phase2.activity_code,
        phase2.service_date,
        phase2.posting_date,
        phase2.procedure_quantity,
        phase2.hcpcs_code,
        phase2.detail_total_charges,
        phase2.cpt_modifier_1,
        phase2.cpt_modifier_2,
        phase2.cpt_modifier_3,
        phase2.series_encounter_number,
        phase2.start_time,
        phase2.stop_time,
        phase2.service_provider,
        phase2.billing_provider,
        phase2.department_id,
        phase2.original_payor_id,
        phase2.account_id,
        phase2.udf_asa_cpt_cd,
        phase2.udf_caa_anes_units,
        phase2.udf_caa_anes_case_cnt,
        phase2.udf_post_period,
        phase2.professional_flag,
        phase2.division,
        phase2.specialty,
        phase2.num_vials,
        phase2.ndc_code,
        phase2.wholesale_price,
        phase2.last_cost,
        phase2.regular_cost,
        phase2.average_cost,
        phase2.invoice_number,
        phase2.src_pat_id,
        phase2.branch_num,
        phase2.journal_num,
        phase2.postperiodint,
        phase2.postperioddaydint,
        case
            when (phase2.postperioddaydint >= 15) then phase2.postperiodint
            else (phase2.postperiodint - 1)
        end as adjusted_udf_post_period
    from
        phase2
)
select
    phase3.facility_code,
    phase3.account_number,
    phase3.revenue_center_code,
    phase3.activity_code,
    phase3.service_date,
    phase3.posting_date,
    phase3.procedure_quantity,
    phase3.hcpcs_code,
    phase3.detail_total_charges,
    phase3.cpt_modifier_1,
    phase3.cpt_modifier_2,
    phase3.cpt_modifier_3,
    phase3.series_encounter_number,
    phase3.start_time,
    phase3.stop_time,
    phase3.service_provider,
    phase3.billing_provider,
    phase3.department_id,
    phase3.original_payor_id,
    phase3.account_id,
    phase3.udf_asa_cpt_cd,
    phase3.udf_caa_anes_units,
    phase3.udf_caa_anes_case_cnt,
    phase3.udf_post_period,
    phase3.professional_flag,
    phase3.division,
    phase3.specialty,
    phase3.num_vials,
    phase3.ndc_code,
    phase3.wholesale_price,
    phase3.last_cost,
    phase3.regular_cost,
    phase3.average_cost,
    phase3.invoice_number,
    phase3.src_pat_id,
    phase3.branch_num,
    phase3.journal_num,
    phase3.adjusted_udf_post_period
from
    phase3
