with
used_note_template as (
    select
        note_edit_metadata_history.visit_key,
        max(case when smart_text.smart_text_id = 37342 --NOT BLEED RARE BLEEDING DISORDER
            then 1 else 0 end) as rbd_text_ind
    from {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
    inner join {{source('cdw', 'note_smart_text_id')}} as note_smart_text_id
        on note_smart_text_id.note_visit_key = note_edit_metadata_history.note_visit_key
    inner join {{source('cdw', 'smart_text')}} as smart_text
        on smart_text.smart_text_key = note_smart_text_id.smart_text_key
    where
        note_edit_metadata_history.last_edit_ind = 1
        and smart_text.smart_text_id in (
                                    37341, --NOT BLEED GENE THERAPY CONSULT
                                    37342, --NOT BLEED RARE BLEEDING DISORDER
                                    37654 --NOT BLEED INFUSION
                                    )
    group by note_edit_metadata_history.visit_key
)
select
    encounter_all.visit_key,
    encounter_all.csn,
    encounter_all.patient_name,
    encounter_all.mrn,
    encounter_all.encounter_date,
    encounter_all.provider_name,
    encounter_all.provider_id,
    encounter_all.department_name,
    encounter_all.department_id,
    encounter_all.visit_type,
    encounter_all.visit_type_id,
    encounter_all.encounter_type,
    encounter_all.encounter_type_id,
    encounter_all.appointment_status,
    encounter_all.appointment_status_id,
    encounter_all.inpatient_ind,
    encounter_all.patient_class,
    case when encounter_all.visit_type_id in ('3630', -- 'RBD CONSULT'
                                                '3631' -- 'RBD FOLLOW  UP'
                                            ) or used_note_template.rbd_text_ind = 1
        then 'RBD patient' else 'GT patient' end as sub_cohort,
    year(add_months(encounter_all.encounter_date, 6)) as fiscal_year,
    date_trunc('month', encounter_all.encounter_date) as visual_month,
    encounter_all.pat_key,
    encounter_all.hsp_acct_key
from {{ ref('encounter_all') }} as encounter_all
left join used_note_template
    on encounter_all.visit_key = used_note_template.visit_key
where
    (encounter_all.department_id = 101012085 --BGR HEMATOLOGY
        and encounter_all.visit_type_id in (
                                            '3628', -- GENE THERAPY CONSULT
                                            '3629', -- GENE THERAPY FOLLOW UP
                                            '3630', -- RBD CONSULT
                                            '3631' -- RBD FOLLOW  UP
                                            )
        and encounter_all.provider_id in (
                                        '19184', -- Doshi, Bhavya S
                                        '13965', -- George, Lindsey
                                        '4949', -- Raffini, Leslie
                                        '13962', -- Samelson-jones, Benjamin J
                                        '21954' -- Whitworth, Hilary B
                                        )
    )
    or (encounter_all.department_id = 101001120 --BGR TRANSFUSION
        and encounter_all.visit_type_id in (
                                            '3628', -- GENE THERAPY CONSULT
                                            '3629', -- GENE THERAPY FOLLOW UP
                                            '3632', -- Roctavian VALOCTOCOGENE INFUSION VISIT
                                            '3633' -- Hemgenix ETRANACOGENE INFUSION VISIT
                                            )
        and encounter_all.provider_id = '18815' -- Hematology Day Hosp, Provider 
    )
    or used_note_template.visit_key is not null
