select
    fact_financial_statistic.stats_cd,
    frontier_rare_lung_encounter_cohort.department_name,
    frontier_rare_lung_encounter_cohort.ip_by_note_only_ind,
    master_statistic.stat_nm as statistic_name,
    master_date.full_dt as post_date,
    fact_financial_statistic.stat_measure as statistic_measure,
    hospital_account.hsp_acct_id, --as primary_key
    to_char(master_date.full_dt, 'MM/01/YYYY') as post_date_month_year
from {{ ref('frontier_rare_lung_encounter_cohort')}} as frontier_rare_lung_encounter_cohort
    inner join {{source('cdw_analytics', 'fact_financial_statistic')}} as fact_financial_statistic
        on frontier_rare_lung_encounter_cohort.hsp_acct_key = fact_financial_statistic.fs_acct_key
    inner join {{source('cdw', 'master_statistic')}} as master_statistic
        on fact_financial_statistic.stats_cd = master_statistic.stat_cd
    left join {{source('cdw', 'master_date')}} as master_date
        on fact_financial_statistic.post_dt_key = master_date.dt_key
    left join {{source('cdw', 'hospital_account')}} as hospital_account
        on frontier_rare_lung_encounter_cohort.hsp_acct_key = hospital_account.hsp_acct_key
where
    -- 'admissions', 'discharges', 'observation patient day equivalents', 'ip patient days'
    master_statistic.stat_cd in (8, 12, 14, 32)
    and rare_lung_ip_ind = 1
    --and admit_start_date is not null
    --and (surgical_bx_ind != '1' or surgical_bx_ind is null)
