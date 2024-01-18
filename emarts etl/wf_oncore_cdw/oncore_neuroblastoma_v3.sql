select
    sv_urm_urm_neuroblastomav3.form_instance_id,
    sv_urm_urm_neuroblastomav3.urm_form_instance_id,
    sv_urm_urm_neuroblastomav3.node_instance_id,
    sv_urm_urm_neuroblastomav3.node_setup_form_id,
    coalesce(patient.pat_key, -1) as pat_key,
    patient.pat_mrn_id as pat_mrn_id,
    sv_urm_urm_neuroblastomav3.nblinssstage,
    sv_urm_urm_neuroblastomav3.inrgstage,
    sv_urm_urm_neuroblastomav3.nblhistology,
    sv_urm_urm_neuroblastomav3.nblriskgroup,
    sv_urm_urm_neuroblastomav3.nblmycnstatus,
    sv_urm_urm_neuroblastomav3.nblsegchromosomealt,
    sv_urm_urm_neuroblastomav3.nblalkstatus,
    sv_urm_urm_neuroblastomav3.nbldnaindex,
    sv_urm_urm_neuroblastomav3.nblparaneoplsyndr,
    sv_urm_urm_neuroblastomav3.nblparaneoplsyndrcomment,
    sv_urm_urm_neuroblastomav3.nblpresentingfeatures,
    current_timestamp as create_dt,
    'OnCore' as create_by,
    current_timestamp as upd_dt,
    'OnCore' as upd_by
 from
    {{ source ('oncore_ods', 'sv_urm_urm_neuroblastomav3' ) }} as sv_urm_urm_neuroblastomav3
    left join {{ source('oncore_ods', 'rv_urm_form_detail' ) }} as rv_urm_form_detail
        on rv_urm_form_detail.form_instance_id = sv_urm_urm_neuroblastomav3.form_instance_id
    left join {{ source('oncore_ods', 'sv_subject' ) }} as sv_subject
        on sv_subject.subject_no = rv_urm_form_detail.subject_no
    left join {{source('cdw', 'patient' )}} as patient
        on patient.pat_mrn_id = sv_subject.subject_mrn
