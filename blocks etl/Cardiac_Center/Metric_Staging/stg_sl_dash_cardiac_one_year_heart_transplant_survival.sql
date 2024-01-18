with cohort as (
    /*
    first_transplant_ind used to determine if this was the first heart transplant
    (by transplant_type) for the patient
    */
    select
        *,
        row_number() over
            (partition by pat_key, organ
                order by pat_key, transplant_date) as first_transplant_ind
    from
        {{ ref('transplant_recipients')}}
    where
        transplant_date is not null --had transplant
        and lower(organ) = 'heart'
        and lower(recipient_donor) = 'recipient'
)

select
    cohort.pat_key,
    cohort.death_less_than_1_year_post_transplant_ind,
    cohort.surgery_date,
    case when cohort.death_less_than_1_year_post_transplant_ind = 1 then 0 else 1 end as num,
    cohort.pat_key as primary_key,
    'cardiac_open_heart_surv' as metric_id
from
    cohort
where
    cohort.surgery_date is not null  --transplant happened at chop
    and cohort.first_transplant_ind = 1 --first transplant
    --only recipients greater than 1 year ago
	and cohort.surgery_date <= add_months(current_date - 1, -12)
