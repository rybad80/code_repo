select
    sv_urm_urm_surgeryv1.form_instance_id,
    sv_urm_urm_surgeryv1.urm_form_instance_id,
    sv_urm_urm_surgeryv1.node_instance_id,
    sv_urm_urm_surgeryv1.node_setup_form_id,
    coalesce(sv_urm_urm_surgeryv1.section_instance_id, 0) as section_instance_id,
    coalesce(patient.pat_key, -1) as pat_key,
    patient.pat_mrn_id as pat_mrn_id,
    sv_urm_urm_surgeryv1.surgprocedures,
    sv_urm_urm_surgeryv1.surglaterality,
    sv_urm_urm_surgeryv1.surgmargins,
    sv_urm_urm_surgeryv1.surgsite,
    sv_urm_urm_surgeryv1.surgextentresect,
    sv_urm_urm_surgeryv1.surgindication,
    sv_urm_urm_surgeryv1.surgdate,
    sv_urm_urm_surgeryv1.surgdate_ext,
    sv_urm_urm_surgeryv1.surgcomment,
    current_timestamp as create_dt,
    'OnCore' as create_by,
    current_timestamp as upd_dt,
    'OnCore' as upd_by
  from
    {{ source ('oncore_ods', 'sv_urm_urm_surgeryv1')}} as sv_urm_urm_surgeryv1
    left join {{ source('oncore_ods', 'rv_urm_form_detail' ) }} as rv_urm_form_detail
        on rv_urm_form_detail.form_instance_id = sv_urm_urm_surgeryv1.form_instance_id
    left join {{ source('oncore_ods', 'sv_subject' ) }} as sv_subject
        on sv_subject.subject_no = rv_urm_form_detail.subject_no
    left join {{source('cdw', 'patient' )}} as patient
        on patient.pat_mrn_id = sv_subject.subject_mrn
