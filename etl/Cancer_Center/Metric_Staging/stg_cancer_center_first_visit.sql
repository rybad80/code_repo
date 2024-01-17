with first_visit as (
    select
        pat_key,
        min(visit_date) as first_visit_date
    from
        {{ref ('stg_cancer_center_visit')}}
    where first_chop_dx_ind = 1
    group by pat_key
)

select
    first_visit.pat_key,
    first_visit.first_visit_date,
    add_months(first_visit_date, 18) as eighteen_months_after_visit,
    max(case when subsequent_visits.visit_date >= add_months(first_visit_date, 18)
        then 1 else 0 end) as visit_after_18_months_ind
from
    first_visit
    left join {{ref ('stg_cancer_center_visit')}} as subsequent_visits
        on first_visit.pat_key = subsequent_visits.pat_key
        and subsequent_visits.visit_date > first_visit.first_visit_date
group by
    first_visit.pat_key,
    first_visit.first_visit_date
