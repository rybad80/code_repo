select
    --the monthly reporting point should cover patients in last 15 months with diabetes visit completed:
    master_date.full_dt as diabetes_reporting_month,
    diabetes_visit_cohort.patient_key,
    diabetes_visit_cohort.pat_key,
    case
        when master_date.full_dt between
            date_trunc('month', current_date) - interval('3 month')
            and date_trunc('month', current_date)
        then 'Last 4 Months'
        when master_date.full_dt between
            date_trunc('month', current_date) - interval('7 month')
            and date_trunc('month', current_date) - interval('4 month')
        then 'Prior 4 Months'
        when master_date.full_dt between
            date_trunc('month', current_date) - interval('15 month')
            and date_trunc('month', current_date) - interval('12 month')
        then 'Last 4 Months from Prior Year'
    end as report_card_4mo_pat_category
from
    {{ source('cdw', 'master_date') }} as master_date
    inner join {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
        on diabetes_visit_cohort.endo_vis_dt between master_date.full_dt - interval('15 month')
            and master_date.full_dt - interval('1 day')
where
    --ICR Flowhseets has launched since 2012, started reporting period since 2013:
    master_date.full_dt between '2013-07-01' and current_date
    and master_date.day_of_mm = 1 -- first day of the month
group by
    master_date.full_dt,
    diabetes_visit_cohort.patient_key,
    diabetes_visit_cohort.pat_key
