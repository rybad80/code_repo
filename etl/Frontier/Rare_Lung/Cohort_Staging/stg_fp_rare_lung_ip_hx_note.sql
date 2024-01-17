select
    stg_encounter.visit_key,
    stg_encounter.mrn,
    --note_edit_metadata_history.version_author_name,
    note_edit_metadata_history.encounter_date as note_date,
    year(add_months(note_edit_metadata_history.encounter_date, 6)) as note_fiscal_year,
    max(case
        when lower(note_type) = 'consult note'
        then 1 else 0 end)
    as rl_ip_consult_ind,
    max(case
        when lower(note_type) = 'progress notes'
        then 1 else 0 end)
    as rl_ip_progress_ind
from
    {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
left join {{ source('cdw', 'note_text') }} as note_text
    on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
inner join {{ ref('lookup_frontier_program_providers_all')}} as lookup_frontier_program_providers
    on lower(note_edit_metadata_history.version_author_name)
        = lower(lookup_frontier_program_providers.provider_name)
    and lookup_frontier_program_providers.program = 'rare-lung'
inner join {{ ref('stg_encounter') }} as stg_encounter
    on note_edit_metadata_history.visit_key = stg_encounter.visit_key
inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    on stg_encounter.visit_key = diagnosis_encounter_all.visit_key
left join {{ ref('lookup_frontier_program_diagnoses') }} as lookup_frontier_program_diagnoses
    on diagnosis_encounter_all.icd10_code = lookup_frontier_program_diagnoses.lookup_dx_id
        and lookup_frontier_program_diagnoses.program = 'rare-lung'
where
    year(add_months(note_edit_metadata_history.encounter_date, 6)) >= 2022
    and last_edit_ind = 1
    and lower(note_type) in ('consult note', 'progress notes')
    and ((diagnosis_encounter_all.icd10_code = lookup_frontier_program_diagnoses.lookup_dx_id
            and lower(diagnosis_encounter_all.diagnosis_name) = lookup_frontier_program_diagnoses.lookup_dx_label)
        or (lower(diagnosis_encounter_all.icd10_code) in ('r91.1', 'r91.8')
            and lower(diagnosis_encounter_all.diagnosis_name) like '%nodule%'))
    and regexp_like(lower(note_text),
                        'abnormal.*chest.*x-ray|
                        |abnormal.*chest.*ct|
                        |tachypnea|
                        |tachycardia|
                        |interstitial.*lung.*disease,.*ild,.*or.*‘child’,.*or.*‘diffuse.*lung.*disease’|
                        |lung.*biopsy|
                        |hypoxic.*respiratory.*failure|
                        |hypoxemia|
                        |alveolar.*capillary.*dysplasia|
                        |diffuse.*groundglass.*opacities|
                        |pulmonary.*opacities|
                        |immunology.*|
                        |infectious.*disease|
                        |rheumatology|
                        |prednisolone|
                        |pulmonary.*vascular.*panel|
                        |pulse.*dose.*steroids|
                        |gaucher.*disease|
                        |raynaud’s|
                        |surfactant.*disorder|
                        |sle.*(systemic lupus erythematosus)|
                        |sjia.*(systemic juvenille idiopathic arthritis)|
                        |mctd.*(mixed connective tissue disease)|
                        |polymyositis.*or.*dermatomyositis|
                        |unclear.*etiology|
                        |nehi|
                        |decline.*in.*lung.*function.*and.*dlco|
                        |prednisone.*burst|
                        |ild.*case.*conference|
                        |rare.*lung|
                        |methylprednisolone|
                        |rituximab|
                        |ivig|
                        |azathioprine|
                        |cellcept|
                        |methotrexate|
                        |cyclosporine|'
                    )
group by
    stg_encounter.visit_key,
    stg_encounter.mrn,
    --note_edit_metadata_history.version_author_name,
    note_edit_metadata_history.encounter_date
