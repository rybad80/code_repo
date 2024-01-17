select
    diagnosis_encounter_all.pat_key,
    diagnosis_encounter_all.mrn,
    diagnosis_encounter_all.visit_key,
    diagnosis_encounter_all.csn,
    diagnosis_encounter_all.patient_name,
    diagnosis_encounter_all.encounter_date,
    diagnosis_encounter_all.hsp_acct_key,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'general motility'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: blue'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: i.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: ii.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: iii.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: iv.%'
            ) then 1 else 0 end) as general_motility_blu_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'general motility'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: yellow'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: i.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: ii.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: iii.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: iv.%'
            ) then 1 else 0 end) as general_motility_yel_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'general motility'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: black'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: i.%'
            ) then 1 else 0 end) as general_motility_blk_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'general motility'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: green'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: i.%'
            ) then 1 else 0 end) as general_motility_grn_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'defecation disorder'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: blue'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: v.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: vi.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: vii.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: viii.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: x.%'
            ) then 1 else 0 end) as defecation_disorder_blu_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'defecation disorder'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: yellow'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: v.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: vi.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: viii.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: ix.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: x.%'
            ) then 1 else 0 end) as defecation_disorder_yel_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'defecation disorder'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: black'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: viii.%'
            ) then 1 else 0 end) as defecation_disorder_blk_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'defecation disorder'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: green'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: v.%'
            ) then 1 else 0 end) as defecation_disorder_grn_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'multi disciplinary'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: blue'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xii.%'
            ) then 1 else 0 end) as multi_disciplinary_blu_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'multi disciplinary'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: green'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xii.%'
            ) then 1 else 0 end) as multi_disciplinary_grn_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'multi system involvement'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: blue'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xi.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xiii.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xiv.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xv.%'
            ) then 1 else 0 end) as life_bowel_dysmotility_blu_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'multi system involvement'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: yellow'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xi.%'
            or lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xiii.%'
            ) then 1 else 0 end) as life_bowel_dysmotility_yel_ind,
    max(case when lower(lookup_frontier_program_diagnoses.category) = 'neuromodulation'
        and lower(lookup_frontier_program_diagnoses.sub_category_1) = 'severity group: blue'
        and (lower(lookup_frontier_program_diagnoses.sub_category_2) like 'diagnosis group: xvi.%'
            ) then 1 else 0 end) as neuromodulation_blu_ind
from
    {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
    inner join {{ ref('lookup_frontier_program_diagnoses') }} as lookup_frontier_program_diagnoses
        on ((diagnosis_encounter_all.icd10_code = cast(
                lookup_frontier_program_diagnoses.lookup_dx_id as nvarchar(20))
                    and lookup_frontier_program_diagnoses.code_type = 'icd-10'
                    and lookup_frontier_program_diagnoses.program = 'motility')
            or (diagnosis_encounter_all.icd9_code = cast(
                lookup_frontier_program_diagnoses.lookup_dx_id as nvarchar(20))
                    and lookup_frontier_program_diagnoses.code_type = 'icd-9'
                    and lookup_frontier_program_diagnoses.program = 'motility'))
group by
    diagnosis_encounter_all.pat_key,
    diagnosis_encounter_all.mrn,
    diagnosis_encounter_all.visit_key,
    diagnosis_encounter_all.csn,
    diagnosis_encounter_all.patient_name,
    diagnosis_encounter_all.encounter_date,
    diagnosis_encounter_all.hsp_acct_key
having
        general_motility_blu_ind
         + general_motility_yel_ind
         + general_motility_blk_ind
         + general_motility_grn_ind
         + defecation_disorder_blu_ind
         + defecation_disorder_yel_ind
         + defecation_disorder_blk_ind
         + defecation_disorder_grn_ind
         + multi_disciplinary_blu_ind
         + multi_disciplinary_grn_ind
         + life_bowel_dysmotility_blu_ind
         + life_bowel_dysmotility_yel_ind
         + neuromodulation_blu_ind > 0
