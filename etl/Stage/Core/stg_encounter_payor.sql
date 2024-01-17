{{ config(
	materialized='table',
	dist='visit_key',
	meta={
		'critical': true
	}
) }}

with hospital_billing as (
    select
        hospital_account_visit.visit_key,
        max(hospital_account.pri_payor_key) as payor_key
    from
        {{source('cdw', 'hospital_account_visit')}} as hospital_account_visit
        inner join {{source('cdw', 'hospital_account')}} as hospital_account
            on hospital_account.hsp_acct_key = hospital_account_visit.hsp_acct_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_bill_stat
            on hospital_account.dict_bill_stat_key = dict_bill_stat.dict_key
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = hospital_account_visit.visit_key
    where
        dict_bill_stat.src_id != '40'
    group by
        hospital_account_visit.visit_key
),

physician_billing as (
    select
        fact_reimbursement.visit_key,
        master_date.full_dt as service_dt,
        fact_reimbursement.pri_payor_key as payor_key,
        patient.pat_mrn_id,
        department.dept_nm,
        fact_reimbursement.cpt_cd,
        fact_reimbursement.chrg_amt,
        rank() over (
            partition by
                fact_reimbursement.visit_key,
                master_date.full_dt
            order by
                fact_reimbursement.chrg_tx_id asc
        ) as rnk
    from (select
            visit_key,
			pri_payor_key,
			dept_key,
			cpt_cd,
			chrg_amt,
			chrg_svc_dt_key,
			pat_key,
			chrg_tx_id
        from
         {{source('cdw', 'fact_reimbursement')}}) as fact_reimbursement
        inner join {{source('cdw', 'department')}} as department
            on department.dept_key = fact_reimbursement.dept_key
        inner join {{source('cdw', 'master_date')}} as master_date
            on master_date.dt_key = fact_reimbursement.chrg_svc_dt_key
        inner join {{source('cdw', 'patient')}} as patient
            on patient.pat_key = fact_reimbursement.pat_key
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = fact_reimbursement.visit_key
),

primary_coverage as (
    select
        visit.visit_key,
        coverage.payor_key,
        ods_coverage.cvg_eff_dt,
        ods_coverage.cvg_term_dt,
        patient_coverage_filing_order.line,
        rank() over(
            partition by visit.visit_key
            order by patient_coverage_filing_order.line asc
        ) as rnk
    from
        {{source('cdw', 'patient')}} as patient
        left join {{source('clarity_ods', 'pat_cvg_file_order')}} as patient_coverage_filing_order
            on patient_coverage_filing_order.pat_id = patient.pat_id
        left join {{source('cdw', 'coverage')}} as coverage
            on coverage.cvg_id = patient_coverage_filing_order.coverage_id
        left join {{source('clarity_ods', 'coverage')}} as ods_coverage
            on ods_coverage.coverage_id = patient_coverage_filing_order.coverage_id
        left join {{source('cdw', 'visit')}} as visit
            on visit.pat_key = patient.pat_key
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = visit.visit_key
    where
        (visit.eff_dt >= ods_coverage.cvg_eff_dt
        and (visit.eff_dt <= ods_coverage.cvg_term_dt or ods_coverage.cvg_term_dt is null))
        or (visit.eff_dt <= ods_coverage.cvg_term_dt
        and (visit.eff_dt >= ods_coverage.cvg_eff_dt or ods_coverage.cvg_eff_dt is null))
)

select
    visit.visit_key,
    case when hospital_billing.payor_key is not null then 1 else 0 end as hospital_billing_ind,
    case when hospital_billing_ind = 0
        and physician_billing.payor_key is not null
        then 1 else 0
    end as physician_billing_ind,
    case when hospital_billing_ind = 0
        and physician_billing_ind = 0
        and primary_coverage.payor_key is not null
        then 1 else 0
    end as primary_coverage_ind,
    case when payor.payor_nm = 'DEFAULT' then 'SELF PAY' else payor.payor_nm end as payor_name,
    case
        when hospital_billing_ind = 1 and payor.rpt_grp_10 is null and payor.payor_nm = 'DEFAULT' then 'SELF PAY'
        when hospital_billing_ind = 1 and payor.rpt_grp_10 is not null then payor.rpt_grp_10
        else payor_group.payor_grp_1
     end as payor_group,
    payor.payor_id,
    visit.pat_key,
    coalesce(
            hospital_billing.payor_key,
            physician_billing.payor_key,
            primary_coverage.payor_key,
            -1
            ) as payor_key
from
    {{source('cdw', 'visit')}} as visit
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = visit.visit_key
    inner join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = visit.pat_key
    left join physician_billing
        on physician_billing.visit_key = visit.visit_key
            and physician_billing.service_dt = visit.eff_dt
            and physician_billing.rnk = 1
    left join primary_coverage
        on primary_coverage.visit_key = visit.visit_key
            and primary_coverage.rnk = 1
    left join hospital_billing
        on hospital_billing.visit_key = visit.visit_key
    left join {{source('cdw', 'payor')}} as payor
        on payor.payor_key = coalesce(
            hospital_billing.payor_key,
            physician_billing.payor_key,
            primary_coverage.payor_key,
            -1
        )
    left join {{source('cdw', 'payor_group')}} as payor_group
        on payor_group.payor_key = payor.payor_key
where
    visit.visit_key > 0
