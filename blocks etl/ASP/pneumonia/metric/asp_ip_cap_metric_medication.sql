select
    stg_asp_ip_cap_cohort_abx.*,
    row_number() over(
        partition by
            asp_ip_cap_cohort.visit_key,
            stg_asp_ip_cap_cohort_abx.outpatient_med_ind
        order by stg_asp_ip_cap_cohort_abx.administration_date
    ) as abx_number,
    case
        when stg_asp_ip_cap_cohort_abx.administration_date = last_abx_time
        and stg_asp_ip_cap_cohort_abx.outpatient_med_ind = 0
        then 1 else 0 end as last_administration_ind,
    date(stg_asp_ip_cap_cohort_abx.administration_date) as administration_calendar_date,
    --days of inpatient antibiotic administration
    --based on research project:
    --assume doses near 24-hr mark are an intent to continue treatment for an additional day
    ceil(
        (stg_asp_ip_cap_cohort_abx.hrs_since_first_abx + 2)
        / 24.0
    ) as ip_abx_duration,
    --days of outpatient antibiotic administration
    --based on research project:
    --Number of days prescribed, rounded down
    floor(
        asp_outpatient_prescription.outpatient_duration_days
    ) as outpatient_duration_days
from
    {{ ref('asp_ip_cap_cohort') }} as asp_ip_cap_cohort
    inner join {{ ref('stg_asp_ip_cap_cohort_abx')}} as stg_asp_ip_cap_cohort_abx
        on asp_ip_cap_cohort.visit_key = stg_asp_ip_cap_cohort_abx.visit_key
    left join {{ ref('asp_outpatient_prescription')}} as asp_outpatient_prescription
        on stg_asp_ip_cap_cohort_abx.med_ord_key = asp_outpatient_prescription.med_ord_key
        and asp_outpatient_prescription.outpatient_duration_days <= 30 --ignore unreasonable durations
