with arc_raw as (
    select
        surgery_encounter.log_key,
        surgery_encounter.log_id,
        clinical_concept.concept_id,
        cast(smart_data_element_value.elem_val as varchar(100)) as raw_value
    from
        {{ ref('surgery_encounter') }} as surgery_encounter
        inner join
             {{ source('cdw', 'or_case') }} as or_case
                on or_case.or_case_id = surgery_encounter.log_id
        inner join
            {{ source('cdw', 'anesthesia_encounter_link') }} as anesthesia_encounter_link
                on anesthesia_encounter_link.or_case_key = or_case.or_case_key
        inner join
            {{ source('cdw', 'smart_data_element_info') }} as smart_data_element_info
                on smart_data_element_info.visit_key = anesthesia_encounter_link.anes_event_visit_key
        inner join
            {{ source('cdw', 'smart_data_element_value') }} as smart_data_element_value
                on smart_data_element_info.sde_key = smart_data_element_value.sde_key
        inner join
            {{ source('cdw', 'clinical_concept') }} as clinical_concept
                on clinical_concept.concept_key = smart_data_element_info.concept_key
    where
        lower(smart_data_element_info.src_sys_val) = 'smartform 283' -- unique ID of the anes pre plan smart form
    group by
        surgery_encounter.log_key,
        surgery_encounter.log_id,
        clinical_concept.concept_id,
        smart_data_element_value.elem_val
)

select
    log_key,
    log_id,
    max(case when concept_id = 'CHOP#1096' then raw_value end) as pae_status_i,
    max(case when concept_id = 'CHOP#1095' then raw_value end) as pae_status_ii,
    max(case when concept_id = 'CHOP#1063' then raw_value end) as reporting_to_loc,
    group_concat(case when concept_id = 'CHOP#1039' then raw_value end, ';') as initial_pat_dest,
    group_concat(case when concept_id = 'CHOPANES#008' then raw_value end, ';') as final_pat_dest,
    max(case when concept_id = 'CHOPANES#013' then raw_value end) as final_pat_dest_comment,
    max(case when concept_id = 'CHOPANES#021' then raw_value end) as post_anes_adm_svc,
    max(case when concept_id = 'CHOPANES#023' then raw_value end) as post_anes_adm_svc_comment,
    max(case when concept_id = 'CHOPANES#029' then raw_value end) as pat_criteria,
    max(case when concept_id = 'CHOPANES#030' then raw_value end) as pat_criteria_comment,
    max(case when concept_id = 'CHOPANES#004' then raw_value end) as stbur_snore_half_time,
    max(case when concept_id = 'CHOPANES#005' then raw_value end) as stbur_snore_loudly,
    max(case when concept_id = 'CHOPANES#007' then raw_value end) as stbur_trouble_breathing,
    max(case when concept_id = 'CHOPANES#009' then raw_value end) as stbur_stop_breathing,
    max(case when concept_id = 'CHOPANES#014' then raw_value end) as stbur_unrefreshed,
    max(case when concept_id = 'CHOPANES#018' then raw_value end) as stbur_total_score,
    max(case when concept_id = 'CHOP#1060' then raw_value end) as addl_testing_needed,
    max(case when concept_id = 'CHOP#1045' then raw_value end) as addl_consult_needed,
    max(case when concept_id = 'CHOP#1069' then raw_value end) as anes_team,
    max(case when concept_id = 'CHOP#1068' then raw_value end) as asc_candidate,
    max(case when concept_id = 'CHOP#1059' then raw_value end) as asc_prov_name,
    max(case when concept_id = 'CHOP#1036' then raw_value end) as asc_prov_review,
    max(case when concept_id = 'CHOP#1061' then raw_value end) as consult_needed,
    max(case when concept_id = 'CHOPANES#025' then raw_value end) as consulting_svc,
    max(case when concept_id = 'CHOPANES#026' then raw_value end) as consulting_svc_comment,
    max(case when concept_id = 'CHOPANES#012' then raw_value end) as disposition_comment,
    max(case when concept_id = 'CHOP#1099' then raw_value end) as extended_type_and_screen,
    max(case when concept_id = 'CHOPANES#003' then raw_value end) as extended_type_and_screen_comment,
    max(case when concept_id = 'CHOPANES#002' then raw_value end) as extended_type_and_screen_complete,
    max(case when concept_id = 'CHOP#1064' then raw_value end) as gi_proc_rec,
    max(case when concept_id = 'CHOP#1037' then raw_value end) as intraop_considerations,
    max(case when concept_id = 'CHOP#1038' then raw_value end) as monitoring_lines,
    max(case when concept_id = 'CHOP#1041' then raw_value end) as nursing_assessment,
    max(case when concept_id = 'CHOP#1074' then raw_value end) as pain_svc_involved,
    max(case when concept_id = 'CHOP#1043' then raw_value end) as parental_presence,
    max(case when concept_id = 'CHOP#1044' then raw_value end) as parental_presence_comment,
    max(case when concept_id = 'CHOP#1089' then raw_value end) as pre_assignment,
    max(case when concept_id = 'CHOP#1087' then raw_value end) as pre_med_route,
    max(case when concept_id = 'CHOP#1070' then raw_value end) as pre_plan_assessment,
    max(case when concept_id = 'CHOP#1088' then raw_value end) as spec_svc_recommend,
    max(case when concept_id = 'CHOP#1072' then raw_value end) as pre_plan_note,
    max(case when concept_id = 'CHOP#1046' then raw_value end) as preop_sedation,
    max(case when concept_id = 'CHOP#1090' then raw_value end) as preop_sedation_over_18,
    max(case when concept_id = 'CHOP#1040' then raw_value end) as pre_proc_pain_plan,
    max(case when concept_id = 'CHOP#1065' then raw_value end) as spec_care_letter,
    max(case when concept_id = 'CHOP#1067' then raw_value end) as spec_care_letter_date_loc,
    max(case when concept_id = 'CHOP#1066' then raw_value end) as spec_care_letter_needed,
    max(case when concept_id = 'CHOP#1062' then raw_value end) as type_of_consult,
    max(case when concept_id = 'EPIC#PECA0003' then raw_value end) as findings_murmer,
    max(case when concept_id = 'EPIC#PESK0018' then raw_value end) as findings_rash,
    max(case when concept_id = 'EPIC#PEPU0108' then raw_value end) as findings_normal_air_entry,
    max(case when concept_id = 'EPIC#49519' then raw_value end) as symptoms_oxygen_sat
from
    arc_raw
group by
    log_key,
    log_id
