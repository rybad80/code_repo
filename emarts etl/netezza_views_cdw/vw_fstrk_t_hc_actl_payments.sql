with br_cls_mojrnl_nums as (
    select
        phi.branch_num,
        phi.cls_dt_key,
        phi.last_mo_cash_journal_num,
        lag(
            phi.last_mo_cash_journal_num,
            1,
            ('0' :: numeric) :: numeric(18, 0)
        ) over (
            partition by phi.branch_num
            order by
                phi.last_mo_cash_journal_num
        ) as last_val
    from
        {{source('cdw', 'period_end_hi_hc')}} as phi
    order by
        phi.last_mo_cash_journal_num
),
phi as (
    select
        br_cls_mojrnl_nums.cls_dt_key,
        br_cls_mojrnl_nums.branch_num,
        br_cls_mojrnl_nums.last_mo_cash_journal_num,
        br_cls_mojrnl_nums.last_val
    from
        br_cls_mojrnl_nums
),
phase1 as (
    select
        100 as facility_code,
        case
            when (
                (v.enc_id isnull)
                or (v.enc_id = '-1' :: numeric(1, 0))
            ) then '-9999999999' :: numeric(10, 0)
            else v.enc_id
        end as account_number,
        case
            when (fhc2.svc_dt_key > 0) then date(("VARCHAR"(fhc2.svc_dt_key)) :: varchar(8))
            else (
                select
                    md.full_dt
                from
                    {{source('cdw', 'master_date')}} as md
                where
                    (md.dt_key = -1)
            )
        end as billing_date,
        case
            when (fhc.post_dt_key > 0) then date(("VARCHAR"(fhc.post_dt_key)) :: varchar(8))
            else (
                select
                    md.full_dt
                from
                    {{source('cdw', 'master_date')}} as md
                where
                    (md.dt_key = -1)
            )
        end as payment_date,
        fhc.trans_type as payment_type,
        bp.bp_id as payor_plan_code,
        p.proc_cd as transaction_code,
        fhc.amt as amount,
        pyr.payor_id as udf_action_payor_id,
        phi.cls_dt_key as udf_post_period,
        fhc.match_hc_tx_id,
        fhc.journal_num
    from
        (
            (
                (
                    (
                        (
                            (
                                {{source('cdw', 'fact_transaction_hc')}} as fhc
                                left join {{source('cdw', 'fact_transaction_hc')}} as fhc2 on (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (fhc.match_hc_tx_id = fhc2.match_hc_tx_id)
                                                        and (fhc.pat_key = fhc2.pat_key)
                                                    )
                                                    and (fhc.bill_type = fhc2.bill_type)
                                                )
                                                and (fhc.payor_key = fhc2.payor_key)
                                            )
                                            and (fhc.branch_num = fhc2.branch_num)
                                        )
                                        and (fhc2.trans_type = 'C' :: "VARCHAR")
                                    )
                                )
                            )
                            left join phi on (
                                (
                                    (phi.branch_num = fhc.branch_num)
                                    and (
                                        (
                                            (fhc.journal_num <= phi.last_mo_cash_journal_num)
                                            and (fhc.journal_num > phi.last_val)
                                        )
                                        and (phi.branch_num = fhc.branch_num)
                                    )
                                )
                            )
                        )
                        left join {{source('cdw', 'procedure')}} as p on ((fhc.proc_key = p.proc_key))
                    )
                    left join {{source('cdw', 'visit')}} as v on ((fhc.visit_key = v.visit_key))
                )
                left join {{source('cdw', 'payor')}} as pyr on ((fhc.payor_key = pyr.payor_key))
            )
            left join {{source('cdw', 'benefit_plan')}} as bp on ((fhc.bp_key = bp.bp_key))
        )
    where
        (
            (
                (
                    fhc.trans_type in (
                        ('A' :: "VARCHAR") :: varchar(20),
                        ('W' :: "VARCHAR") :: varchar(20),
                        ('P' :: "VARCHAR") :: varchar(20)
                    )
                )
                or (fhc.trans_type isnull)
            )
            and (
                fhc.gl_cd in (
                    ('9502' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9507' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9545' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9560' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9570' :: numeric(38, 0)) :: numeric(38, 0),
                    ('7005' :: numeric(38, 0)) :: numeric(38, 0),
                    ('7010' :: numeric(38, 0)) :: numeric(38, 0),
                    ('7050' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9540' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9530' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9525' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9520' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9506' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9505' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9503' :: numeric(38, 0)) :: numeric(38, 0),
                    ('9510' :: numeric(38, 0)) :: numeric(38, 0)
                )
            )
        )
),
phase2 as (
    select
        phase1.facility_code,
        phase1.account_number,
        phase1.billing_date,
        phase1.payment_date,
        phase1.payment_type,
        phase1.payor_plan_code,
        phase1.transaction_code,
        phase1.amount,
        phase1.udf_action_payor_id,
        phase1.udf_post_period,
        phase1.match_hc_tx_id,
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
        ) as postperiodaydint
    from
        phase1
),
phase3 as (
    select
        phase2.facility_code,
        phase2.account_number,
        phase2.billing_date,
        phase2.payment_date,
        phase2.payment_type,
        phase2.payor_plan_code,
        phase2.transaction_code,
        phase2.amount,
        phase2.udf_action_payor_id,
        phase2.udf_post_period,
        phase2.match_hc_tx_id,
        phase2.journal_num,
        phase2.postperiodint,
        phase2.postperiodaydint,
        case
            when (phase2.postperiodaydint >= 15) then phase2.postperiodint
            else (phase2.postperiodint - 1)
        end as adjusted_udf_post_period
    from
        phase2
)
select
    phase3.facility_code,
    phase3.account_number,
    phase3.billing_date,
    phase3.payment_date,
    phase3.payment_type,
    phase3.payor_plan_code,
    phase3.transaction_code,
    phase3.amount,
    phase3.udf_action_payor_id,
    phase3.udf_post_period,
    phase3.match_hc_tx_id,
    phase3.journal_num,
    phase3.adjusted_udf_post_period
from
    phase3
