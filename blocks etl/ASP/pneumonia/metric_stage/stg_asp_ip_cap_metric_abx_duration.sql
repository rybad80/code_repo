--duration is a combination of IP and greatest OP
with abx_duration as (
    select
        visit_key,
        --days of inpatient antibiotic administration
        --based on research project:
        --assume doses near 24-hr mark are an intent to continue treatment for an additional day
        max(
            coalesce(
                ip_abx_duration,
                0
            )
        ) as longest_ip_abx_duration,
        max(
            coalesce(
                outpatient_duration_days,
                0
            )
        ) as longest_op_abx_duration,
        longest_ip_abx_duration + longest_op_abx_duration as total_abx_duration_days
    from
        {{ ref('asp_ip_cap_metric_medication') }}
    group by
        visit_key
)

--total antibiotic duration less than 6 days OR greater than 30 days 
select
    visit_key,
    'total_duration_ind' as duration_group,
    cast(total_abx_duration_days as varchar(100)) as duration_type
from
    abx_duration
where
    total_abx_duration_days not between 7 and 29
group by
    visit_key,
    duration_type

union all

select
    visit_key,
    'strep_otitis_duration_ind' as duration_group,
    'Strep / Otitis' as duration_type
from
    {{ ref('asp_ip_cap_cohort') }}
where
    other_dx_ind = 1
group by
    visit_key,
    duration_type

union all

select
    visit_key,
    'culture_infections_duration_ind' as duration_group,
    history_description as duration_type
from
    {{ ref('stg_asp_ip_cap_metric_medical_history') }}
where
    history_type = 'Culture'
    and active_visit_ind = 1
    and regexp_like(
        history_description,
        'aerug|legion|mrsa|methicillin|cult'
    )
group by
    visit_key,
    duration_type

union all

select
    visit_key,
    'complicated_pneumonia_duration_ind' as duration_group,
    'Complicated Pneumonia' as duration_type
from
    {{ ref('asp_ip_cap_cohort') }}
where
    complicated_pneumonia_ind = 1
group by
    visit_key,
    duration_type
