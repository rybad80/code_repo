with registry_metrics as (--region
    select
        pat_key,
        max(case when rule_id = '1387066'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as last_neph_visit,
        max(case when rule_id = '1387066'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as next_neph_appt,
        max(case when rule_id = '1386535'
            then val
            else null end) as gd_primary_dx_last_neph_visit, --returns dx_id
        max(case when rule_id = '1387067'
            then val
            else null end) as last_neph_prov, --returns prov_id
        max(case when rule_id = '1386009'
            then val
            else null end) as remission_status,
        max(case when rule_id = '1386811'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as remission_status_date,
        max(case when rule_id = '1017522'
            then val
            else null end) as urine_protein,
        max(case when rule_id = '644363'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as last_urinalysis_3yr,
        max(case when rule_id = '1386358'
            then val
            else null end) as admission_count_past_30_days,
        max(case when rule_id = '1386363'
            then val
            else null end) as ip_days_past_30_days,
        max(case when rule_id = '1389514'
            then val
            else null end) as revisit_7_day_acute_3_month,
        max(case when rule_id = '1387274'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as last_covid_19_vaccine,
        max(case when rule_id = '642861'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as most_recent_flu_vaccine_,
        max(case when rule_id = '1386337'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as most_recent_pneumovax,
        max(case when rule_id = '1386339'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as second_most_recent_pneumovax,
        max(case when rule_id = '1386340'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as most_recent_prevnar_13,
        max(case when rule_id = '1386341'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as second_most_recent_prevnar_13,
        max(case when rule_id = '1386343'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as third_most_recent_prevnar_13,
        max(case when rule_id = '1386345'
            then cast('1840-12-31' as date) + cast(val as int)
            else null end) as fourth_most_recent_prevnar_13,
        max(case when rule_id = '1387086'
            then val
            else null end) as last_nephrology_department_id
    from
        {{ ref('stg_glean_nephrology_cohort_metrics')}}
    where
        seq = 1
    group by
        pat_key
)

select
    *
from
    registry_metrics
