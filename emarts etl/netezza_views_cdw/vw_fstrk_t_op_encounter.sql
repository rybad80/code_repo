with fhcbal as (
    select
        int8(
            (
                "VARCHAR"(md.c_yyyy) || lpad("VARCHAR"(md.c_mm), 2, '0' :: "VARCHAR")
            )
        ) as yyyymm,
        sum(fhc.item_bal) as patient_current_balance,
        fhc.pat_key
    from
        (
            {{source('cdw', 'fact_transaction_hc')}} as fhc
            left join {{source('cdw', 'master_date')}} as md on ((fhc.post_dt_key = md.dt_key))
        )
    where
        (fhc.trans_type = 'C' :: "VARCHAR")
    group by
        fhc.pat_key,
        int8(
            (
                "VARCHAR"(md.c_yyyy) || lpad("VARCHAR"(md.c_mm), 2, '0' :: "VARCHAR")
            )
        )
),
init_diags as (
    select
        vd.visit_key,
        vd.seq_num,
        d.icd9_cd,
        d2.icd9_svc_lvl1,
        d2.icd9_svc_lvl2,
        d2.icd9_svc_lvl3
    from
        (
            (
                {{source('cdw', 'visit_diagnosis')}} as vd
                left join {{source('cdw', 'master_diagnosis')}} as d on ((vd.dx_key = d.dx_key))
            )
            left join {{source('cdw', 'master_service_icd9')}} as d2 on ((d.dx_key = d2.dx_key))
        )
    where
        (vd.create_by = 'FASTRACK' :: "VARCHAR")
),
final as (
    select
        100 as facility_code,
        case
            when (
                (v.enc_id isnull)
                or (v.enc_id = '-1' :: numeric(1, 0))
            ) then '-9999999999' :: numeric(10, 0)
            else v.enc_id
        end as visit_id,
        pat.pat_mrn_id as medical_record_number,
        'O' as patient_type,
        'HC' as custom_patient_type,
        case
            when (v.contact_dt_key > 0) then date(("VARCHAR"(v.contact_dt_key)) :: varchar(8))
            else (
                select
                    md.full_dt
                from
                    {{source('cdw', 'master_date')}} as md
                where
                    (md.dt_key = -1)
            )
        end as admission_date,
        v.eff_dt as soc_date,
        v.dischrg_dt as discharge_date,
        c.subscr_num as insureds_id_number,
        c.subscr_nm as insureds_name,
        pyr.payor_id as payor_code,
        bp.bp_id as payor_plan_code,
        fc.fc_id as financial_class,
        1019 as account_location,
        pat.last_nm as last_name,
        pat.first_nm as first_name,
        substr(
            case
                when (pat.middle_nm notnull) then pat.middle_nm
                when ('' notnull) then '' :: "VARCHAR"
                else null :: "VARCHAR"
            end,
            1,
            1
        ) as middle_initial,
        pat.addr_line1 as address_1,
        pat.addr_line2 as address_2,
        '' as address_3,
        pat.city,
        pat."STATE",
        pat.county,
        pat.zip as zip_code,
        pat.home_ph as phone,
        pat.dob as date_of_birth,
        pat.sex as gender,
        pat.marital_stat,
        pat.lang as "LANGUAGE",
        '' as social_security_number,
        '' as hospital_account_id,
        'HOME CARE' as icd9_svc_lvl1,
        'HOME CARE' as icd9_svc_lvl2,
        'HOME CARE' as icd9_svc_lvl3,
        '' as account_type,
        0 as ar_balance,
        fhcbal.patient_current_balance,
        fhcbal.yyyymm
    from
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        {{source('cdw', 'visit')}} as v
                                        left join {{source('cdw', 'payor')}} as pyr on ((v.payor_key = pyr.payor_key))
                                    )
                                    left join {{source('cdw', 'financial_class')}} as fc on ((pyr.fc_key = fc.fc_key))
                                )
                                left join {{source('cdw', 'patient')}} as pat on ((v.pat_key = pat.pat_key))
                            )
                            left join {{source('cdw', 'pat_acct_cvg')}} as pcvg on (
                                (
                                    (
                                        (
                                            (v.pat_key = pcvg.pat_key)
                                            and (pcvg.create_by = 'FASTRACK' :: "VARCHAR")
                                        )
                                        and (v.bp_key = pcvg.bp_key)
                                    )
                                    and (v.payor_key = pcvg.payor_key)
                                )
                            )
                        )
                        left join {{source('cdw', 'coverage')}} as c on ((pcvg.cvg_key = c.cvg_key))
                    )
                    left join {{source('cdw', 'benefit_plan')}} as bp on ((v.bp_key = bp.bp_key))
                )
                left join init_diags id1 on ((id1.visit_key = v.visit_key))
            )
            left join fhcbal on ((fhcbal.pat_key = v.pat_key))
        )
    where
        (v.create_by = 'FASTRACK' :: "VARCHAR")
)
select
    distinct "FINAL".facility_code,
    "FINAL".visit_id,
    "FINAL".medical_record_number,
    "FINAL".patient_type,
    "FINAL".custom_patient_type,
    "FINAL".admission_date,
    "FINAL".soc_date,
    "FINAL".discharge_date,
    "FINAL".insureds_id_number,
    "FINAL".insureds_name,
    "FINAL".payor_code,
    "FINAL".payor_plan_code,
    "FINAL".financial_class,
    "FINAL".account_location,
    "FINAL".last_name,
    "FINAL".first_name,
    "FINAL".middle_initial,
    "FINAL".address_1,
    "FINAL".address_2,
    "FINAL".address_3,
    "FINAL".city,
    "FINAL"."STATE",
    "FINAL".county,
    "FINAL".zip_code,
    "FINAL".phone,
    "FINAL".date_of_birth,
    "FINAL".gender,
    "FINAL".marital_stat,
    "FINAL"."LANGUAGE",
    "FINAL".social_security_number,
    "FINAL".hospital_account_id,
    "FINAL".icd9_svc_lvl1,
    "FINAL".icd9_svc_lvl2,
    "FINAL".icd9_svc_lvl3,
    "FINAL".account_type,
    "FINAL".ar_balance,
    "FINAL".patient_current_balance,
    "FINAL".yyyymm
from
    "FINAL"
