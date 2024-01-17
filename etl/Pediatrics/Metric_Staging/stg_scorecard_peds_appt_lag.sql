select
    scheduling_specialty_care_appointments.visit_key as primary_key,
    scheduling_specialty_care_appointments.appointment_made_date,
    scheduling_specialty_care_appointments.specialty_name,
    scheduling_specialty_care_appointments.revenue_location_group,
    scheduling_specialty_care_appointments.npv_lag_incl_ind,
    case
        when
            ((lower(scheduling_specialty_care_appointments.revenue_location_group) = 'chca'
            and lower(scheduling_specialty_care_appointments.specialty_name) in (
            'adolescent',
            'allergy',
            'cardiology',
            'dermatology',
            'developmental pediatrics',
            'endocrinology',
            'gastroenterology',
            'genetics',
            'gi/nutrition',
            'healthy weight',
            'hematology',
            'immunology',
            'infectious disease',
            'metabolism',
            'neonatology',
            'nephrology',
            'neurology',
            'pulmonary',
            'rehab medicine',
            'rheumatology'))
            or lower(scheduling_specialty_care_appointments.specialty_name) = 'oncology')
            and scheduling_specialty_care_appointments.npv_appointment_lag_days < 12
            then 1 else 0
        end as num_new_pts_schd_within_12_days,
    case
        when
            ((lower(scheduling_specialty_care_appointments.revenue_location_group) = 'chca'
            and lower(scheduling_specialty_care_appointments.specialty_name) in (
            'adolescent',
            'allergy',
            'cardiology',
            'dermatology',
            'developmental pediatrics',
            'endocrinology',
            'gastroenterology',
            'genetics',
            'gi/nutrition',
            'healthy weight',
            'hematology',
            'immunology',
            'infectious disease',
            'metabolism',
            'neonatology',
            'nephrology',
            'neurology',
            'pulmonary',
            'rehab medicine',
            'rheumatology'))
            or lower(scheduling_specialty_care_appointments.specialty_name) = 'oncology')
            then 1 else 0
        end as denom_new_pts_schd_within_12_days,
    case
        when
            ((lower(scheduling_specialty_care_appointments.revenue_location_group) = 'chca'
            and lower(scheduling_specialty_care_appointments.specialty_name) in (
            'adolescent',
            'allergy',
            'cardiology',
            'dermatology',
            'developmental pediatrics',
            'endocrinology',
            'gastroenterology',
            'genetics',
            'gi/nutrition',
            'healthy weight',
            'hematology',
            'immunology',
            'infectious disease',
            'metabolism',
            'neonatology',
            'nephrology',
            'neurology',
            'pulmonary',
            'rehab medicine',
            'rheumatology'))
            or lower(scheduling_specialty_care_appointments.specialty_name) = 'oncology')
            then npv_appointment_lag_days
        end as ped_divs_npv_appointment_lag_days
from
     {{ref('scheduling_specialty_care_appointments')}} as scheduling_specialty_care_appointments
    inner join {{ source('cdw', 'visit')}} as visit
    on scheduling_specialty_care_appointments.visit_key = visit.visit_key
    left join {{ source('cdw', 'dim_visit_cncl_rsn')}} as dim_visit_cncl_rsn
    on visit.dim_visit_cncl_rsn_key = dim_visit_cncl_rsn.dim_visit_cncl_rsn_key
where
    scheduling_specialty_care_appointments.appointment_made_date >= '01/01/2019'
    and scheduling_specialty_care_appointments.npv_lag_incl_ind = 1
    and lower(scheduling_specialty_care_appointments.revenue_location_group) in ('chca', 'csa')
