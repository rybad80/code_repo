with patient_info as (
    select
        subject_no,
        ptinforeferraltype
    from
        {{source('ods', 'urm_patientinfov4_linking')}}
    group by
        subject_no, --there are duplicate entries in this table, deduping to maintain granularity
        ptinforeferraltype
)

select
    urm_brain_tumors_link.subject_no as subject_id,
    sv_subject.subject_last_name as patient_last_name,
    sv_subject.subject_first_name as patient_first_name,
    sv_subject.subject_mrn as mrn,
    urm_brain_tumors_link.cnscbttc as cns_cbttc,
    urm_brain_tumors_link.braintumormstatus as brain_tumor_status,
    urm_brain_tumors_link.cnsmblsubgroups as cns_mbl_subgroups,
    urm_brain_tumors_link.cnsmblclinicalstage as cns_mbl_clinical_stage,
    urm_brain_tumors_link.cnsgeneticalterations as cns_genetic_alterations,
    urm_brain_tumors_link.cnsothergenetic as cns_other_genetic,
    urm_brain_tumors_link.cnsmorbidities as cns_morbidities,
    urm_brain_tumors_link.cnsothermorbidities as cns_other_morbidities,
    urm_diagnosis_link.dxdiagnosistype as diagnosis_type,
    urm_diagnosis_link.dxdatediagnosisrelapse as diagnosis_relapse_date,
    urm_diagnosis_link.dxprimarysite as diagnosis_primary_site,
    urm_diagnosis_link.dxhistology as diagnosis_histology,
    patient_info.ptinforeferraltype as patient_info_referral_type
from
    {{source('ods', 'urm_brain_tumors_link')}} as urm_brain_tumors_link
left join
    {{source('ods', 'urm_diagnosis_link')}} as urm_diagnosis_link
    on urm_diagnosis_link.subject_no = urm_brain_tumors_link.subject_no
    and urm_diagnosis_link.node_instance_id = urm_brain_tumors_link.node_instance_id
inner join
    {{source('ods', 'sv_subject')}} as sv_subject
    on urm_brain_tumors_link.subject_no = sv_subject.subject_no
inner join
    patient_info
    on urm_brain_tumors_link.subject_no = patient_info.subject_no
inner join
    {{ref('stg_patient')}} as stg_patient
    on sv_subject.subject_mrn = stg_patient.mrn --removing test patients
