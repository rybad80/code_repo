{{ config(meta = {
    'critical': true
}) }}

with log_surgeon as (
    select
        or_log.log_key,
        provider.full_nm as primary_surgeon,
        provider.prov_key as surgeon_prov_key,
        row_number() over (partition by or_log.log_key order by or_log_surgeons.start_dt) as surgeon_number
    from
        {{source('cdw', 'or_log')}} as or_log
        inner join {{source('cdw', 'or_log_surgeons')}} as or_log_surgeons
            on or_log_surgeons.log_key = or_log.log_key
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = or_log_surgeons.surg_prov_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_role
            on dict_or_role.dict_key = or_log_surgeons.dict_or_role_key
    where
        dict_or_role.src_id in (1.0000, 1.0030) --primary
        and or_log_surgeons.panel_num = 1
    group by
        or_log.log_key,
        provider.full_nm,
        provider.prov_key,
        or_log_surgeons.start_dt
)

select
    or_log.log_key as or_key,
    or_log.case_key,
    or_case.or_case_id as case_id,
    or_log.log_key,
    or_log.log_id,
    or_log.vsi_key,
    master_date.full_dt as surgery_date,
    or_log.dict_pat_class_key,
    or_log.dict_or_svc_key,
    or_log.dict_or_asa_rating_key,
    or_log.loc_key,
    or_log.room_prov_key,
    or_log.dict_or_case_type_key,
    or_log.dict_or_case_class_key,
    or_log.create_by as source_system,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.sex,
    stg_patient.pat_key,
    stg_encounter.csn,
    cast(pat_or_adm_link.pat_enc_csn_id as numeric(14, 3)) as surgery_csn,
    stg_encounter.encounter_date,
    stg_encounter.hospital_discharge_date,
    log_surgeon.primary_surgeon,
    log_surgeon.surgeon_prov_key,
    stg_encounter.encounter_key,
    stg_encounter.visit_key,
    case when dict_or_stat.src_id = 2 then 1 else 0 end as posted_ind,
    or_log.admit_visit_key
from
    {{source('cdw', 'or_log')}} as or_log
    left join {{source('cdw', 'or_case')}} as or_case
        on or_log.case_key = or_case.or_case_key
    left join log_surgeon
        on log_surgeon.log_key = or_log.log_key
            and log_surgeon.surgeon_number = 1
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = or_log.pat_key
    left join {{source('cdw', 'master_date')}} as master_date
        on master_date.dt_key = or_log.surg_dt_key
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = or_log.admit_visit_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_not_perf
        on dict_not_perf.dict_key = or_log.dict_not_perf_key
    inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_stat
        on dict_or_stat.dict_key = or_log.dict_or_stat_key
    inner join {{source('cdw', 'location')}} as location --noqa: L029
        on location.loc_key = or_log.loc_key
    left join {{source('clarity_ods','pat_or_adm_link')}}  as pat_or_adm_link
        on pat_or_adm_link.log_id = or_log.log_id
where
    dict_not_perf.src_id = -2 --'not applicable'
    and dict_or_stat.src_id not in (4, 6) --'voided', 'canceled'
    and or_log.log_id > 0
    and or_log.pat_key > 0
    --'chop virtual anesthesia','CHOP INTERVENTIONAL RADIOLOGY SURGICAL LOCATION'
    -- 'CHOP RADIOLOGY NON INVASIVE LOCATION'
    and location.loc_id not in (46, 60046, 60047)
