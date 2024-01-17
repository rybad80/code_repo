{{
    config(
        materialized='incremental',
        unique_key = 'note_visit_key'
    )
}}
select
    note_visit_info.note_visit_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.sex,
    stg_encounter.age_years,
    stg_encounter.provider_name,
    stg_encounter.provider_id,
    stg_encounter.department_name,
    stg_encounter.specialty_name,
    dim_ip_note_type.ip_note_type_nm as note_type,
    dim_ip_note_type.ip_note_type_id as note_type_id,
    stg_note_info.note_id,
    final_employee.full_nm as final_author_name,
    final_provider.full_nm as version_author_provider_name,
    final_provider.title as version_author_provider_title,
    version_employee.title as version_author_employee_title,
    coalesce(final_provider.title, final_employee.title) as final_author_title,
    coalesce(version_provider.full_nm, version_employee.full_nm) as version_author_name,
    coalesce(version_provider.title, version_employee.title) as version_author_title,
    dim_hospital_patient_service.hosp_pat_svc_nm as version_author_service_name,
    dim_hospital_patient_service.hosp_pat_svc_id as version_author_service_id,
    cast(stg_note_visit_info.contact_num as int) as edit_seq_number,
    case when row_number() over(
        partition by stg_note_info.note_id order by edit_seq_number desc
        ) = 1 then 1 else 0
    end as last_edit_ind,
    stg_note_visit_info.spec_time_loc_dttm as service_date,
    stg_note_visit_info.ent_inst_local_dttm as note_entry_date,
    dim_note_status.note_stat_nm as note_status,
    dim_note_status.note_stat_id as note_status_id,
    dim_note_sensitivity.note_snstvty_nm as note_sensitivity,
    dim_note_sensitivity.note_snstvty_id as note_sensitivity_id,
    note_info.note_key,
    note_info.vsi_key,
    stg_encounter.patient_key,
    stg_encounter.provider_key,
    stg_encounter.pat_key,
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    stg_encounter.prov_key,
    stg_note_visit_info.contact_serial_num as note_enc_id,
    version_employee.emp_key as version_author_emp_key,
    version_provider.prov_key as version_author_prov_key,
    final_employee.emp_key as final_author_emp_key,
    final_employee.prov_key as final_author_prov_key,
    case when dim_note_status.note_stat_id = '4' then 1 else 0 end as note_deleted_ind,
    case
        when stg_note_visit_info.last_updated_date >= coalesce(stg_note_info.last_updated_at, '1900-01-01')
            then stg_note_visit_info.last_updated_date
        else stg_note_info.last_updated_at
    end as block_last_update_date
from
    {{ref('stg_note_visit_info')}} as stg_note_visit_info
    inner join {{ref('stg_note_info')}} as stg_note_info
        on stg_note_visit_info.note_id = stg_note_info.note_id
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.csn = stg_note_info.pat_enc_csn_id
    left join {{source('cdw','dim_ip_note_type')}} as dim_ip_note_type
        on dim_ip_note_type.ip_note_type_id = stg_note_visit_info.contact_note_type_id
    left join {{source('cdw','dim_note_status')}} as dim_note_status
        on dim_note_status.note_stat_id = stg_note_visit_info.note_status_id
    left join {{source('cdw','dim_hospital_patient_service')}} as dim_hospital_patient_service
        on dim_hospital_patient_service.hosp_pat_svc_id = stg_note_visit_info.author_service_id
    left join {{source('cdw','employee')}} as final_employee
        on final_employee.emp_id = stg_note_info.current_author_id
    left join {{source('cdw','provider')}} as final_provider
        on final_provider.prov_key = final_employee.prov_key
    left join  {{source('cdw','employee')}} as version_employee
        on version_employee.emp_id = stg_note_visit_info.author_user_id
    left join {{source('cdw','provider')}} as version_provider
        on version_provider.prov_key = version_employee.prov_key
    left join {{source('cdw','dim_note_sensitivity')}} as dim_note_sensitivity
        on dim_note_sensitivity.note_snstvty_id = stg_note_visit_info.sensitive_status_id
    -- Use only for note_visit_key:
    left join {{source('cdw','note_visit_info')}} as note_visit_info
        on note_visit_info.note_enc_id = stg_note_visit_info.contact_serial_num
    -- Use only for vsi_key and note_key:
    left join {{source('cdw','note_info')}} as note_info
        on stg_note_visit_info.note_id = note_info.note_id
where 1 = 1
    and (
    	dim_note_status.note_stat_id not in (
        	'-1' --invalid
    	)
    	or
    	dim_note_status.note_stat_id is null
	)
    and {{ limit_dates_for_dev(ref_date = 'stg_encounter.encounter_date') }}
{% if is_incremental() %}
    and date(block_last_update_date) > (select max(date(block_last_update_date)) from {{ this }})
{% endif %}
