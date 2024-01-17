{{ config(
    materialized='table',
    dist='visit_key',
    meta = {
        'critical': true
    }
) }}

with online_scheduling as (
select
    pat_enc.pat_enc_csn_id as csn,
    pat_enc.contact_date as encounter_date,
    date_trunc('day', pat_enc_audit.es_audit_time) as original_appointment_made_date,
    pat_enc_audit.es_audit_user_id as original_appointment_made_user_id,
    case when coalesce(pat_enc.is_walk_in_yn, 'N') = 'Y' then 1 else 0 end as walkin_ind,
    case
        when pat_enc_audit.pat_online_yn like 'Y'
            or lower(pat_enc_audit.es_audit_user_id) like 'queuedrpd'
        then 1
        else 0
        end as online_scheduled_ind
from {{source('clarity_ods', 'pat_enc')}} as pat_enc
    inner join {{source('clarity_ods', 'pat_enc_es_aud_act')}} as pat_enc_audit
        on pat_enc.pat_enc_csn_id = pat_enc_audit.pat_enc_csn_id
        and pat_enc_audit.line = 1
        and pat_enc_audit.es_audit_action_c in (1, 8) -- scheduling and rescheduling action
)

select
    visit.visit_key,
    visit.enc_id as csn,
    stg_visit_dates.encounter_date,
    {{
        dbt_utils.surrogate_key([
            'floor(visit.enc_id)',
            'visit.pat_id',
            'visit.create_by'
        ])
    }} as encounter_key,
    ---------- patient ----------
    visit.pat_key,
    stg_patient_ods.pat_id,
    stg_patient_ods.patient_key,
    stg_patient_ods.patient_name,
    stg_patient_ods.dob,
    stg_patient_ods.mrn,
    stg_patient_ods.sex,
    coalesce(visit.age_days, (date(stg_visit_dates.encounter_date) - date(stg_patient_ods.dob))) as age_days,
    coalesce(visit.age, (date(stg_visit_dates.encounter_date) - date(stg_patient_ods.dob)) / 365.25) as age_years,
    age_years * 12.0 as age_months,
    ---------- department and location ----------
    case when visit.eff_dept_key = 0 then visit.dept_key else visit.eff_dept_key end as dept_key,
    stg_department_all.department_key,
    stg_department_all.department_id,
    stg_department_all.department_name,
    stg_department_all.specialty_name,
    stg_department_all.intended_use_name,
    visit.los_proc_cd,
    visit.svc_area_key,
    ---------- hospital admissions ----------
    visit.hosp_admit_type as admission_type,
    stg_visit_dates.hospital_admit_date,
    stg_visit_dates.hospital_discharge_date,
    ---------- visit types and patient class ----------
    master_visit_type.visit_type_nm as visit_type,
    master_visit_type.visit_type_id,
    cast(dict_appt_stat.src_id as integer) as appointment_status_id,
    dict_appt_stat.dict_nm as appointment_status,
    cast(dim_patient_class.pat_class_id as integer) as patient_class_id,
    dim_patient_class.pat_class_nm as patient_class,
    cast(dict_enc_type.src_id as integer) as encounter_type_id,
    initcap(dict_enc_type.dict_nm) as encounter_type,
    ---------- address at time of encounter ----------
    stg_encounter_address.seq_num as patient_address_seq_num,
    stg_encounter_address.zip as patient_address_zip_code,
    upper(stg_encounter_address.county) as county,
    upper(stg_encounter_address.state) as state,
    zip_market_mapping.chop_market as chop_market_raw,
    zip_market_mapping.region_category as region_category_raw,
    ---------- visit provider ----------
    -- switch downstream dependencies to full_nm for consistancy
    dim_provider.provider_key,
    initcap(dim_provider.full_name) as provider_name,
    dim_provider.provider_type as prov_type,
    -- switch downstream dependencies the replace with surrogate_key
    dim_provider.prov_id as provider_id,
    case
        when visit.dischrg_prov_key != 0 then visit.dischrg_prov_key
        else coalesce(visit.visit_prov_key, -1)
    end as prov_key,
    case
        when visit.dischrg_prov_id != '0' then visit.dischrg_prov_id
        else coalesce(visit.visit_prov_id, '-1')
    end as prov_id,
    visit.visit_prov_key,
    stg_secondary_provider.secondary_provider_name,
    stg_secondary_provider.secondary_provider_type,
    visit.rfl_req_ind,
    visit.rfl_key,
    ---------- appointments ----------
    visit.appt_dt as appointment_date,
    visit.appt_made_dt as appointment_made_date,
    visit.appt_cancel_dt as appointment_cancel_date,
    stg_visit_dates.enc_instant as encounter_instant,
    visit.enc_close_dt as encounter_close_date,
    stg_visit_dates.enc_close_time as encounter_close_time,
    visit.enc_closed_ind as encounter_closed_ind,
    visit.eff_dt,
    stg_checkin.begin_checkin_date,
    visit.appt_checkin_dt as check_in_date,
    visit.appt_checkout_dt as check_out_date,
    visit.appt_lgth_min as scheduled_length_min,
    visit.appt_entry_emp_key,
    stg_visit_dates.enc_closed_user_id,
    ---------- cancellations ----------
    visit.dim_visit_cncl_rsn_key,
    visit.appt_cancel_24hr_ind as cancel_24hr_ind,
    visit.appt_cancel_48hr_ind as cancel_48hr_ind,
    case when dict_appt_stat.src_id = 3 then 1 else 0 end as cancel_ind,
    case when dict_appt_stat.src_id = 4 then 1 else 0 end as noshow_ind,
    case when dict_appt_stat.src_id = 5 then 1 else 0 end as lws_ind, --left without seen
    case when dict_appt_stat.src_id in (3, 4) then 1 else 0 end as cancel_noshow_ind,
    case when dict_appt_stat.src_id in (3, 4, 5) then 1 else 0
        end as cancel_noshow_lws_ind,
    ---------- online scheduleing ----------
    online_scheduling.original_appointment_made_date,
    online_scheduling.original_appointment_made_user_id,
    coalesce(online_scheduling.walkin_ind, 0) as walkin_ind,
    coalesce(online_scheduling.online_scheduled_ind, 0) as online_scheduled_ind,
    stg_checkin.echeckin_status_name,
    stg_checkin.echeckin_complete_ind,
    --indicator for appointments scheduled through mychop
    coalesce(mychop_scheduled_ind, 0) as mychop_scheduled_ind
from
    {{source('cdw', 'visit')}} as visit
    inner join {{ref('stg_visit_dates')}} as stg_visit_dates
        on stg_visit_dates.visit_key = visit.visit_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_appt_stat
        on dict_appt_stat.dict_key = visit.dict_appt_stat_key
    left join {{source('clarity_ods', 'pat_enc_2')}} as pat_enc_2
        on pat_enc_2.pat_enc_csn_id = visit.enc_id
    left join {{source('cdw', 'dim_patient_class')}} as dim_patient_class
        on dim_patient_class.pat_class_id = coalesce(pat_enc_2.adt_pat_class_c, '0')
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_enc_type
        on dict_enc_type.dict_key = visit.dict_enc_type_key
    inner join {{ref('stg_patient_ods')}} as stg_patient_ods
        on stg_patient_ods.pat_id = visit.pat_id
    inner join {{source('cdw', 'master_visit_type')}} as master_visit_type
        on master_visit_type.visit_type_key = visit.appt_visit_type_key
    inner join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.dept_key
        = case when visit.eff_dept_key = 0 then visit.dept_key else visit.eff_dept_key end
    left join {{ref('dim_provider')}} as dim_provider
        on dim_provider.prov_id
        = case when visit.dischrg_prov_id != '0' then visit.dischrg_prov_id
            else coalesce(visit.visit_prov_id, '-1') end
    left join {{ref('stg_secondary_provider')}} as stg_secondary_provider
        on stg_secondary_provider.visit_key = visit.visit_key
    left join {{ ref('stg_mychop_scheduled')}} as stg_mychop_scheduled
        on stg_mychop_scheduled.visit_key = visit.visit_key
    left join {{ref('stg_encounter_address')}} as stg_encounter_address
        on stg_encounter_address.visit_key = visit.visit_key
        and stg_encounter_address.line_most_recent_address = 1
        and stg_encounter_address.intl_other_ind = 0
    left join {{ref('stg_dim_zip_market_mapping')}} as zip_market_mapping
        on zip_market_mapping.zip = stg_encounter_address.zip_5_digit
    left join online_scheduling
        on visit.enc_id = online_scheduling.csn
    left join {{ref('stg_checkin')}} as stg_checkin
        on visit.visit_key = stg_checkin.visit_key
where
    visit.visit_key > 0
    and {{ limit_dates_for_dev(ref_date = 'stg_visit_dates.encounter_date') }}
