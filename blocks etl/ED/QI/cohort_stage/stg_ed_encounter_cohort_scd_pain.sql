with
complaint as (

select
    encounter_ed.visit_key,
    1 as scd_complaint
from
    {{ ref('encounter_ed') }} as encounter_ed
	inner join {{ source('cdw', 'visit_reason') }} as visit_reason on encounter_ed.visit_key = visit_reason.visit_key
	inner join {{ source('cdw', 'master_reason_for_visit') }} as master_reason_for_visit
        on master_reason_for_visit.rsn_key = visit_reason.rsn_key
where
     upper(master_reason_for_visit.rsn_nm) like 'SICKLE%CELL%'
group by
    encounter_ed.visit_key

),
med_admin as (

select
    encounter_ed.visit_key,
    1 as admin_opioid
from
    {{ ref('encounter_ed') }} as encounter_ed
    inner join {{ ref('medication_order_administration') }} as medication_order_administration
        on medication_order_administration.visit_key = encounter_ed.visit_key
where
    (lower(medication_name) like '%oxycodone%' or lower(generic_medication_name) like '%oxycodone%'
    or lower(medication_name) like '%morphine%' or lower(generic_medication_name) like '%morphine%'
    or lower(medication_name) like '%hydromorphone%' or lower(generic_medication_name) like '%hydromorphone%'
    or lower(medication_name) like '%fentanyl%' or lower(generic_medication_name) like '%fentanyl%'
    )
    and administration_seq_number > 0
    and medication_order_administration.administration_date <= encounter_ed.ed_discharge_date
group by
    encounter_ed.visit_key

)
select distinct
    encounter_ed.visit_key,
    encounter_ed.pat_key,
    'SCD_PAIN' as cohort,
    null as subcohort
from
    {{ ref('encounter_ed') }} as encounter_ed
    left join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on diagnosis_encounter_all.visit_key = encounter_ed.visit_key
    left join complaint     on complaint.visit_key = encounter_ed.visit_key
    inner join med_admin    on med_admin.visit_key = encounter_ed.visit_key
where
    year(encounter_ed.encounter_date) >= year(current_date) - 5
    and med_admin.admin_opioid = 1
    and (
        ((diagnosis_encounter_all.ed_primary_ind = 1 or diagnosis_encounter_all.ed_other_ind = 1)
        and diagnosis_encounter_all.icd10_code in (
                'D57',
                'D57.0',
                'D57.00',
                'D57.01',
                'D57.02',
                'D57.1',
                'D57.2',
                'D57.20',
                'D57.21',
                'D57.211',
                'D57.212',
                'D57.219',
                'D57.4',
                'D57.40',
                'D57.41',
                'D57.411',
                'D57.412',
                'D57.419',
                'D57.8',
                'D57.80',
                'D57.81',
                'D57.811',
                'D57.812',
                'D57.819'
                )
            )
    or complaint.scd_complaint = 1
    )
