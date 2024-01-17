{{ config(meta = {
    'critical': true
}) }}

with nicu_patients as(
    select
        visit_key,
        mrn,
        patient_name,
        episode_start_date,
        episode_end_date
    from
        {{ref('neo_nicu_episode_phl')}}
    group by
        visit_key,
        mrn,
        patient_name,
        episode_start_date,
        episode_end_date
),
chnd_meds as (
    select
        {{
            dbt_utils.surrogate_key([
                'nicu_patients.mrn',
                'nicu_patients.episode_start_date',
                'medication_order_administration.MED_ORD_KEY'
                ])
        }} as chnd_medication_key,
        nicu_patients.visit_key,
        nicu_patients.mrn,
        nicu_patients.patient_name,
        nicu_patients.episode_start_date,
        nicu_patients.episode_end_date,
        medication_order_administration.therapeutic_class,
        medication_order_administration.pharmacy_class,
        medication_order_administration.generic_medication_name,
        medication_order_administration.medication_name,
        medication_order_administration.medication_route,
        case
            when lower(medication_name) like '%dopamine%' then 'dopamine'
            when lower(medication_name) like '%dobutamine%' then 'dobutamine'
            when lower(medication_name) like '%surfactant%' then 'surfactant'
            when lower(generic_medication_name) like '%poractnant alfa%' then 'surfactant'
        end as chnd_med_category,
        medication_order_administration.administration_date,
        medication_order_administration.medication_start_date,
        medication_order_administration.medication_order_create_date
    from
        nicu_patients
        inner join {{ref('medication_order_administration')}} as medication_order_administration
            on nicu_patients.visit_key = medication_order_administration.visit_key
            and medication_order_administration.administration_date
                between nicu_patients.episode_start_date and nicu_patients.episode_end_date
        inner join {{source('manual_ods', 'neo_abstractor_portal_meds')}} as neo_abstractor_portal_meds
            on lower(medication_order_administration.generic_medication_name) like
                lower('%' || neo_abstractor_portal_meds.generic ||'%')
            and lower(medication_order_administration.order_class) != 'historical med'

)
select
    chnd_medication_key,
    visit_key,
    mrn,
    patient_name,
    episode_start_date,
    episode_end_date,
    therapeutic_class,
    pharmacy_class,
    generic_medication_name,
    medication_name,
    medication_route,
    chnd_med_category,
    min(
        coalesce(
            administration_date,
            medication_start_date,
            medication_order_create_date
            )
    ) as medication_first_dose_date,
    min(
        case
            when coalesce(
                administration_date,
                medication_start_date,
                medication_order_create_date
                ) between episode_start_date and coalesce(
                    episode_end_date, current_date
                    )
                then 1
            else 0
        end
    ) as medication_admin_during_nicu_episode_ind
from
    chnd_meds
group by
    chnd_medication_key,
    visit_key,
    mrn,
    patient_name,
    episode_start_date,
    episode_end_date,
    therapeutic_class,
    pharmacy_class,
    generic_medication_name,
    medication_name,
    medication_route,
    chnd_med_category
