select
    stg_usnwr_diabetes_primary_pop.submission_year,
    stg_usnwr_diabetes_primary_pop.primary_key,
    patient.pat_mrn_id as mrn,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%lipid%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%ldl%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%hdl%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%low%density%lip%'
        then 1 else 0
    end) as lipid_scans,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%lipid%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%ldl%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%hdl%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%low%density%lip%'
        then docs_rcvd_rslts.result_inst_dttm
    end) as lipid_scan_date,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%lipid%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%ldl%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%hdl%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%low%density%lip%'
        then docs_rcvd_rslt_comps.result_value
    end) as lipid_scan_result_value,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%microa%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%albumin%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%urina%'
        then 1 else 0
    end) as microa_scans,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%microa%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%albumin%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%urina%'
        then docs_rcvd_rslts.result_inst_dttm
    end) as microa_scan_date,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%microa%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%albumin%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%urina%'
        then docs_rcvd_rslt_comps.result_value
    end) as microa_scan_result_value,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%diabetic retinopathy%'
        then 1 else 0
    end) as retinopathy_scans,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%diabetic retinopathy%'
        then docs_rcvd_rslts.result_inst_dttm
    end) as retinopathy_scan_date,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%diabetic retinopathy%'
        then docs_rcvd_rslt_comps.result_value
    end) as retinopathy_scan_result_value,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%thyroid stimulating%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%tsh%'
        then 1 else 0
    end) as tsh_scans,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%thyroid stimulating%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%tsh%'
        then docs_rcvd_rslts.result_inst_dttm
    end) as tsh_scan_date,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%thyroid stimulating%'
            or lower(docs_rcvd_rslt_comps.result_comp_name) like '%tsh%'
        then docs_rcvd_rslt_comps.result_value
    end) as tsh_scan_result_value,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%triglyceride%'
        then 1 else 0
    end) as triglycerides_scans,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%triglyceride%'
        then docs_rcvd_rslts.result_inst_dttm
    end) as triglycerides_scan_date,
    max(case
        when lower(docs_rcvd_rslt_comps.result_comp_name) like '%triglyceride%'
        then docs_rcvd_rslt_comps.result_value
    end) as triglycerides_scan_result_value,
    '1' as scan_labs
from
    {{ref('stg_usnwr_diabetes_primary_pop')}} as stg_usnwr_diabetes_primary_pop
    inner join {{source('clarity_ods', 'patient')}} as patient
        on stg_usnwr_diabetes_primary_pop.mrn = patient.pat_mrn_id
    inner join {{source('clarity_ods', 'docs_rcvd')}} as docs_rcvd
        on docs_rcvd.pat_id = patient.pat_id
    inner join {{source('clarity_ods', 'docs_rcvd_rslt_comps')}} as docs_rcvd_rslt_comps
        on docs_rcvd_rslt_comps.document_id = docs_rcvd.document_id
    inner join {{source('clarity_ods', 'docs_rcvd_rslts')}} as docs_rcvd_rslts
        on docs_rcvd_rslts.document_id = docs_rcvd_rslt_comps.document_id
            and docs_rcvd_rslts.contact_date_real = docs_rcvd_rslt_comps.contact_date_real
            and docs_rcvd_rslts.result_key = docs_rcvd_rslt_comps.result_comp_key
where
    (lower(docs_rcvd_rslt_comps.result_comp_name) like '%lipid%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%ldl%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%hdl%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%low%density%lip%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%microal%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%albumin%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%urina%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%triglycerides%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%diabetic retinopathy%'
        or lower(docs_rcvd_rslt_comps.result_comp_name) like '%thyroid stimulating%'
    )
    and length(trim(translate(replace(replace(trim(
        upper(docs_rcvd_rslt_comps.result_value)), '>', ''), '<', ''), ' +-.0123456789', ''))) = 0
    and docs_rcvd_rslt_comps.result_value not like '<%'
    and docs_rcvd_rslt_comps.result_value not like '>%'
group by
    stg_usnwr_diabetes_primary_pop.submission_year,
    stg_usnwr_diabetes_primary_pop.primary_key,
    patient.pat_mrn_id
