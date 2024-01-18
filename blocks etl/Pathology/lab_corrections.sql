select res_db_main.result_id,
    res_vld_audit.line as result_validation_audit_line,
    res_db_main.res_inst_validtd_tm as result_validation_datetime,
    res_db_main.res_inst_unval_tm as result_unvalidation_datetime,
    lookup_lab_result_unvalidation_reason.correction_type as result_correction_type,
    zc_res_unvld_rsn.name as result_unvalidation_reason,
    res_vld_audit.res_unvld_rsn_com as result_unvalidation_reason_comment,
    spec_db_main.specimen_id,
    spec_test_rel.spec_number_rltd as specimen_number,
    spec_db_main.spec_dtm_collected as specimen_collected_datetime,
    test_mstr_db_main.test_id,
    test_mstr_db_main.test_name,
    test_mstr_db_main.test_abbr as test_abbreviation,
    cast(timezone(spec_test_rel.test_ver_utc_dttm, 'UTC', 'America/New_York') as datetime)
        as test_verification_datetime,
    stg_patient.patient_name,
    stg_patient.mrn,
    date(stg_patient.dob) as dob,
    dim_lab_section.lab_section_key,
    dim_lab_section.lab_section_id,
    dim_lab_section.lab_section_name,
    dim_lab.lab_key,
    dim_lab.lab_id,
    dim_lab.lab_name,
    dim_lab.chop_lab_ind,
    res_vld_audit.res_vld_user as correcting_user_id,
    clarity_emp.name as correcting_user_name
from
    {{source('clarity_ods', 'spec_db_main')}} as spec_db_main
    inner join {{source('clarity_ods', 'res_db_main')}} as res_db_main
        on spec_db_main.specimen_id = res_db_main.res_specimen_id
    inner join {{source('clarity_ods', 'test_mstr_db_main')}} as test_mstr_db_main
        on res_db_main.res_test_id = test_mstr_db_main.test_id
    inner join {{source('clarity_ods', 'res_vld_audit')}} as res_vld_audit
        on res_db_main.result_id = res_vld_audit.result_id
    left join {{source('clarity_ods', 'clarity_emp')}} as clarity_emp
        on clarity_emp.user_id = res_vld_audit.res_vld_user
    inner join {{source('clarity_ods', 'zc_lab_corr_type')}} as zc_lab_corr_type
        on res_vld_audit.unvalidation_type_c = zc_lab_corr_type.lab_corr_type_c
    inner join {{source('clarity_ods', 'zc_res_unvld_rsn')}} as zc_res_unvld_rsn
        on res_vld_audit.res_unvld_rsn_c = zc_res_unvld_rsn.res_unvld_rsn_c
    inner join {{ref('lookup_lab_result_unvalidation_reason')}}
        as lookup_lab_result_unvalidation_reason
        on lookup_lab_result_unvalidation_reason.res_unvld_rsn_c = zc_res_unvld_rsn.res_unvld_rsn_c
    inner join {{source('clarity_ods', 'zc_res_val_status')}} as zc_res_val_status
        on zc_res_val_status.res_val_status_c = res_db_main.res_val_status_c
    left join {{ref('stg_patient')}} as stg_patient
        on spec_db_main.spec_ept_pat_id = stg_patient.pat_id
    inner join {{source('clarity_ods', 'spec_test_rel')}} as spec_test_rel
        on spec_db_main.specimen_id = spec_test_rel.specimen_id
            and test_mstr_db_main.test_id = spec_test_rel.spec_tst_id
            and res_db_main.result_id = spec_test_rel.spec_unvld_result
    left join {{ref('lookup_lab_section_correction')}} as lookup_lab_section_correction
        on spec_test_rel.spec_tst_sec_id = lookup_lab_section_correction.section_id
            and regexp_extract(cast(spec_test_rel.spec_number_rltd as varchar(30)), '[^-]+', 1, 2)
                = lookup_lab_section_correction.specimen_number_prefix
    left join {{ref('dim_lab_section')}} as dim_lab_section
        on (dim_lab_section.lab_section_id
            = coalesce(lookup_lab_section_correction.corrected_section_id, spec_test_rel.spec_tst_sec_id)
            -- When case_id is populated, it is an Anatomic Pathology specimen
            or (dim_lab_section.lab_section_id = '123013' and spec_db_main.case_id is not null))
    left join {{ref('dim_lab')}} as dim_lab
        on (dim_lab.lab_id = spec_test_rel.spec_tst_acc_lab_id
            -- When case_id is populated, it is an Anatomic Pathology specimen
            or (dim_lab.lab_id = '123001' and spec_db_main.case_id is not null))
where
    zc_lab_corr_type.name = 'Correction'
    and zc_res_val_status.name = 'Corrected'
    -- Excluding blood bank 'interface received corrected result' because it's not a meaningful correction
    and not (lower(res_vld_audit.res_unvld_rsn_com) = 'interface received corrected result'
        and lower(dim_lab_section.lab_section_name) like '%blood bank%')
