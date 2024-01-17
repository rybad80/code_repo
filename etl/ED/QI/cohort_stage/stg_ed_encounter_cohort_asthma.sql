with
asthma_dx as (

select distinct
    diagnosis_encounter_all.visit_key,
    1 as asthma_ind
from
    {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
where
    (diagnosis_encounter_all.ed_primary_ind = 1 or diagnosis_encounter_all.ed_other_ind = 1)
    and (diagnosis_encounter_all.icd10_code like 'J45%' or diagnosis_encounter_all.icd9_code like '493%')
),

g_steroid as (

select distinct
    stg_encounter_ed.visit_key,
    1 as g_steroid_admin
from
    {{ ref('stg_encounter_ed') }} as stg_encounter_ed
    inner join {{ ref('medication_order_administration') }} as medication_order_administration
        on medication_order_administration.visit_key = stg_encounter_ed.visit_key
where
    lower(medication_order_administration.pharmacy_sub_class) = 'glucocorticosteroids'
    and medication_order_administration.administration_seq_number > 0
    and medication_order_administration.administration_date between
            stg_encounter_ed.ed_arrival_date and stg_encounter_ed.ed_discharge_date
),

albuterol as (

select distinct
    stg_encounter_ed.visit_key,
    1 as albuterol_order_ind
from
    {{ ref('stg_encounter_ed') }} as stg_encounter_ed
    inner join {{ ref('medication_order_administration') }} as medication_order_administration
        on medication_order_administration.visit_key = stg_encounter_ed.visit_key
where
    (medication_name like 'ALBUTEROL%'
    or medication_name like 'LEVALBUTEROL%'
    or generic_medication_name like 'ALBUTEROL%'
    or generic_medication_name like 'LEVALBUTEROL%'
    )
    and medication_order_administration.medication_order_create_date between
                stg_encounter_ed.ed_arrival_date and stg_encounter_ed.ed_discharge_date
)

select
    stg_encounter_ed.visit_key,
    stg_encounter_ed.pat_key,
    'ASTHMA' as cohort,
    null as subcohort
from
    {{ ref('stg_encounter_ed') }} as stg_encounter_ed
    inner join asthma_dx        on asthma_dx.visit_key = stg_encounter_ed.visit_key
    left join albuterol         on albuterol.visit_key = stg_encounter_ed.visit_key
    left join g_steroid         on g_steroid.visit_key = stg_encounter_ed.visit_key
where
    stg_encounter_ed.ed_patients_seen_ind = 1
    and stg_encounter_ed.age_years >= 2
    and year(stg_encounter_ed.encounter_date) >= year(current_date) - 5
    and asthma_dx.asthma_ind = 1
    and (coalesce(g_steroid_admin, 0) + coalesce(albuterol_order_ind, 0)) >= 1
