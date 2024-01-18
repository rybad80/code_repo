{% set db =  env_var('ENVIRONMENT', 'UAT') if target.type == 'snowflake' else  env_var("DATA_LAKE_DB","CDW_ODS_UAT") %}
{% set fetch_tables = dbt_utils.get_relations_by_pattern(
    schema_pattern='admin',
    table_pattern='service_qa_program_fy%',
    database=db
)%}

with
all_surveys as (
    {{ dbt_utils.union_relations(
        relations=fetch_tables
    ) }}
) ,

incl_employee as (
    select  
            cast("RECORD_ID" as bigint) as record_id,
            cast(replace("SVC_DATE", '-', '') as bigint) as date_key,
            "SVC_DATE" as svc_date,
            "MONTH" as month ,
            emp.emp_key,
            "EMP_NAME" as emp_name,
            "SUPERVISOR" as supervisor,
            "PATIENT_NAME" as patient_name,
            "PT_DEMO" as pt_demo,
            "EMERGENCY_RECORDED" as emergency_recorded,
            case "DOC_SGND" when 'Yes' then 1 when 'No' then 0 else null end as doc_sgnd,
            case "PCP_RECORDED" when 'Yes' then 1 when 'No' then 0 else null end as pcp_recorded ,
            case "COV_ATTACHED" when 'Yes' then 1 when 'No' then 0 else null end as cov_attached ,
            case "PHARMACY_RECORDED" when 'Yes' then 1 when 'No' then 0 else null end as pharmacy_recorded ,
            case "TRAVEL" when 'Yes' then 1 when 'No' then 0 else null end as travel ,
            cast("DEMO_NUM" as decimal(18,2)) as demo_num ,
            cast("DEMO_DEN" as int) as demo_den ,
            case "INS_RECORDED" when 'Yes' then 1 when 'No' then 0 else null end as ins_recorded ,
            case "RESPONSE_HISTORY" when 'Yes' then 1 when 'No' then 0 else null end as  response_history,
            case "VERIFIED_MAN" when 'Yes' then 1 when 'No' then 0 else null end as verified_man ,
            case "GUAR_SUBS_INFO" when 'Yes' then 1 when 'No' then 0 else null end as guar_subs_info ,
            case coalesce("COPAY","COPAY_INDICATED") when 'Yes' then 1 when 'No' then 0 else null end as copay_indicated,
            case "INS_RX_SCANNED" when 'Yes' then 1 when 'No' then 0 else null end as ins_rx_scanned ,
            case "NONPAR_INS" when 'Yes' then 1 when 'No' then 0 else null end as nonpar_ins ,
            case "CDC_COMPLETED" when 'Yes' then 1 when 'No' then 0 else null end as cdc_completed ,
            case "COPAY_COLLECT" when 'Yes' then 1 when 'No' then 0 else null end as copay_collect ,
            cast("COVER_NUM" as int) as cover_num ,
            cast("COVER_DEN" as int) as cover_den ,
            case "AUTH_REF" when 'Yes' then 1 when 'No' then 0 else null end as auth_ref ,
            cast("VISIT_NUM" as int) as visit_num ,
            cast("VISIT_DEN" as int) as  visit_den,
            case "MYCHOP_SETUP" when 'Yes' then 1 when 'No' then 0 else null end as mychop_setup,
            cast("MYCHOP_NUM" as int) as mychop_num ,
            cast("MYCHOP_DEN" as int) as mychop_den,
            "COMMENTS" as comments ,
            cast("NUM" as decimal(18,2)) as num ,
            cast("DEN" as int) as den ,
            coalesce("ADMISSION_MANAGEMENT_DEPARTMENT_AC_AUDIT_TOOL_COMPLETE" ,
                            "EMERGENCY_DEPARTMENT_PSA_AUDIT_TOOL_COMPLETE" ,
                            "ED_SURGICAL_UNIT_AUDIT_TOOL_COMPLETE" ,
                            "OUTPATIENT_REGISTRATION_PSR_AUDIT_TOOL_COMPLETE",
                            "KOPH_AUDIT_TOOL_COMPLETE",
                            "SCC_PSR_AUDIT_TOOL_COMPLETE",
                            "URGENT_CARE_PSR_AUDIT_TOOL_COMPLETE"
                            ) as audit_tool_complete ,
            case 
                when _dbt_source_relation LIKE '%SERVICE_QA_PROGRAM_FY%_ADMISSION_MANAGEMENT_AC' then 'Admissions'
                when _dbt_source_relation LIKE '%SERVICE_QA_PROGRAM_FY%_ED_PSA' then 'ED'
                when _dbt_source_relation LIKE '%SERVICE_QA_PROGRAM_FY%_ED_SURGICAL' then 'Surgical Unit'
                when _dbt_source_relation LIKE '%SERVICE_QA_PROGRAM_FY%_OUTPATIENT_REGISTRATION_PSR' then 'OP Registration'
                when _dbt_source_relation LIKE '%SERVICE_QA_PROGRAM_FY%_KOPH' then 'KOPH'
                when _dbt_source_relation LIKE '%SERVICE_QA_PROGRAM_FY%_SCC_PSR' then 'SCC'
                when _dbt_source_relation LIKE '%SERVICE_QA_PROGRAM_FY%_URGENT_CARE_PSR' then 'SCC'
                else null
                end as department,   
            case "INS_TERM" when 'Yes' then 1 when 'No' then 0 else null end as ins_term ,
            case "IA_MVA_ACCTS" when 'Yes' then 1 when 'No' then 0 else null end as ia_mva_accts ,
            case "SELFPAY" when 'Yes' then 1 when 'No' then 0 else null end as selfpay ,
            case "REF_PHYSICIAN_RECORDED" when 'Yes' then 1 when 'No' then 0 else null end as ref_physician_recorded,
            cast(regexp_extract(_dbt_source_relation, '\d+') as int) AS fiscal_year,
            row_number() over(partition by record_id, _dbt_source_relation, month order by active_ind desc, create_dt desc ) as rownum,
            all_surveys."UPD_DT" as upd_dt,
            _dbt_source_relation
from all_surveys 
left join   {{source('cdw', 'employee')}} as emp
on lower(all_surveys.emp_name) = lower(emp.full_nm) and 
emp.comp_key = 1
)

select
  {{
            dbt_utils.surrogate_key([
                'incl_employee.record_id',
                'incl_employee.department',
                'incl_employee.fiscal_year',
                '_dbt_source_relation'
            ])
        }} as audit_record_key,
        record_id,
        date_key,
        svc_date,
        month,
        emp_key,
        emp_name,
        supervisor,
        patient_name,
        pt_demo,
        emergency_recorded,
        doc_sgnd,
        pcp_recorded,
        cov_attached,
        pharmacy_recorded,
        travel,
        demo_num,
        demo_den,
        ins_recorded,
        response_history,
        verified_man,
        guar_subs_info,
        copay_indicated,
        ins_rx_scanned,
        nonpar_ins,
        cdc_completed,
        copay_collect,
        cover_num,
        cover_den,
        auth_ref,
        visit_num,
        visit_den,
        mychop_setup,
        mychop_num,
        mychop_den,
        comments,
        audit_tool_complete,
        num,
        den,
        department,
        ins_term,
        ia_mva_accts,
        selfpay,
        ref_physician_recorded,
        fiscal_year,
        upd_dt
from incl_employee
WHERE rownum = 1
 



