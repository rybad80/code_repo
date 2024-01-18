select distinct
    fact_financial_statistic.stats_cd,
    frontier_heart_valve_encounter_cohort.admission_department,
    master_statistic.stat_nm as statistic_name,
    master_date.full_dt as post_date,
    fact_financial_statistic.stat_measure as statistic_measure,
    hospital_account.hsp_acct_id, --as primary_key
    to_char(master_date.full_dt, 'MM/01/YYYY') as post_date_month_year
from
    {{ ref('frontier_heart_valve_encounter_cohort') }} as frontier_heart_valve_encounter_cohort
    left join {{source('cdw_analytics', 'fact_financial_statistic')}} as fact_financial_statistic
        on frontier_heart_valve_encounter_cohort.hsp_acct_key = fact_financial_statistic.fs_acct_key
    left join {{source('cdw', 'master_statistic')}} as master_statistic
        on fact_financial_statistic.stats_cd = master_statistic.stat_cd
    left join {{source('cdw', 'master_date')}} as master_date
        on fact_financial_statistic.post_dt_key = master_date.dt_key
    left join {{source('cdw', 'hospital_account')}} as hospital_account
        on frontier_heart_valve_encounter_cohort.hsp_acct_key = hospital_account.hsp_acct_key
where
    -- 'admissions', 'discharges', 'observation patient day equivalents', 'ip patient days'
    master_statistic.stat_cd in (8, 12, 14, 32)
