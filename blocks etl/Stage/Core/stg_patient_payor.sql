{{ config(meta = {
    'critical': true
}) }}

with raw_payors as (
    select
        patient.pat_key,
        payor.payor_key,
        payor.payor_nm,
        payor_group.payor_grp_1,
        patient_coverage_filing_order.filing_order,
        eff_frm_dt,
        rank() over(
            partition by patient.pat_key
            order by patient_coverage_filing_order.filing_order asc, coverage_member_hist.eff_frm_dt asc
        ) as rank_filing_order
    from
        {{source('cdw','patient')}} as patient
        left join {{source('cdw','patient_coverage_filing_order')}} as patient_coverage_filing_order
            on patient.pat_key = patient_coverage_filing_order.pat_key
        left join {{source('cdw','coverage')}} as coverage_infa
            on patient_coverage_filing_order.cvg_key = coverage_infa.cvg_key
        left join {{source('clarity_ods','coverage')}} as coverage
            on coverage.coverage_id = coverage_infa.cvg_id
        left join {{source('cdw','payor')}} as payor on coverage_infa.payor_key = payor.payor_key
        left join {{source('cdw','payor_group')}} as payor_group
            on payor.payor_key = payor_group.payor_key
        left join {{source('cdw','coverage_member_hist')}} as coverage_member_hist
            on coverage_infa.cvg_key = coverage_member_hist.cvg_key
    where
        coalesce(payor.rec_stat, '9999') != '4' --removing hidden payors
        and ((current_date >= coverage.cvg_eff_dt and coverage.cvg_term_dt is null)
        or (current_date <= coverage.cvg_term_dt and coverage.cvg_eff_dt is null)
        or (current_date >= coverage.cvg_eff_dt and current_date <= coverage.cvg_term_dt))
        and current_date <= coverage_member_hist.eff_to_dt
        --removing research, BH and pharmacy only benefit
        and payor.payor_id not in (
            '1188',
            '1183',
            '1184',
            '1185',
            '1186',
            '1182',
            '1192',
            '1119',
            '1031',
            '1194')
)

select
    raw_payors.pat_key,
    raw_payors.payor_key,
    raw_payors.payor_nm as payor_name,
    raw_payors.payor_grp_1 as payor_group,
    raw_payors.eff_frm_dt as start_date
from
    raw_payors
where
    raw_payors.rank_filing_order = 1
group by
    raw_payors.pat_key,
    raw_payors.payor_key,
    raw_payors.payor_nm,
    raw_payors.payor_grp_1,
    raw_payors.eff_frm_dt
