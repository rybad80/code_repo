select
    sv_urm_urm_diagnosisv1.form_instance_id as form_instance_id,
    sv_urm_urm_diagnosisv1.urm_form_instance_id as urm_form_instance_id,
    sv_urm_urm_diagnosisv1.node_instance_id as node_instance_id,
    sv_urm_urm_diagnosisv1.node_setup_form_id as node_setup_form_id,
    coalesce(patient.pat_key, -1) as pat_key,
    patient.pat_mrn_id as pat_mrn_id,
    sv_urm_urm_diagnosisv1.dxdiagnosistype as dx_diagnosis_type,
    sv_urm_urm_diagnosisv1.dxdatediagnosisrelapse as dx_timestamp_diagnosis_relapse,
    sv_urm_urm_diagnosisv1.dxdatediagnosisrelapse_ext as dx_timestamp_diagnosis_relapse_ext,
    sv_urm_urm_diagnosisv1.dxsecondcancercause as dx_second_cancer_cause,
    sv_urm_urm_diagnosisv1.dxprimarysite as dx_primary_site,
    sv_urm_urm_diagnosisv1.dxprimarylaterality as dx_primary_laterality,
    sv_urm_urm_diagnosisv1.dxprimarymultiplicity as dx_primary_multiplicity,
    sv_urm_urm_diagnosisv1.dxprimarydiagnosissource as dx_primary_diagnosis_source,
    sv_urm_urm_diagnosisv1.dxotherprimarydiagnosissource as dx_other_primary_diagnosis_source,
    sv_urm_urm_diagnosisv1.dxhistology as dx_histology,
    sv_urm_urm_diagnosisv1.dxbehavior as dx_behavior,
    sv_urm_urm_diagnosisv1.dxextentofdisease as dx_extent_of_disease,
    current_timestamp as create_dt,
    'OnCore' as create_by,
    current_timestamp as upd_dt,
    'OnCore' as upd_by
  from
    {{ source('oncore_ods', 'sv_urm_urm_diagnosisv1' ) }} as sv_urm_urm_diagnosisv1
    left join {{ source('oncore_ods', 'rv_urm_form_detail' ) }} as rv_urm_form_detail
        on rv_urm_form_detail.form_instance_id = sv_urm_urm_diagnosisv1.form_instance_id
    left join {{ source('oncore_ods', 'sv_subject' ) }} as sv_subject
        on sv_subject.subject_no = rv_urm_form_detail.subject_no
    left join {{source('cdw', 'patient' )}} as patient
        on patient.pat_mrn_id = sv_subject.subject_mrn
