with panel_times as (
    select
        surgery_procedure.or_key,
        surgery_procedure.panel_number,
        surgery_procedure.surgeon_prov_key,
        min(or_log_surgeons.start_dt) as panel_start_time,
        max(or_log_surgeons.end_dt) as panel_end_time,
        case
            when panel_start_time is null or panel_end_time is null then null
            when panel_end_time < panel_start_time then null
            else floor(extract(epoch from panel_end_time - panel_start_time) / 60.0)
        end as panel_length_minutes
    from
        {{ ref('surgery_procedure') }} as surgery_procedure
        inner join {{source('cdw', 'or_log_surgeons')}} as or_log_surgeons
            on or_log_surgeons.log_key = surgery_procedure.log_key
            and or_log_surgeons.panel_num = surgery_procedure.panel_number
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_role
            on dict_or_role.dict_key = or_log_surgeons.dict_or_role_key
            and dict_or_role.src_id = 1 --'primary'
    where
        surgery_procedure.log_key is not null
    group by
        surgery_procedure.or_key,
        surgery_procedure.panel_number,
        surgery_procedure.surgeon_prov_key
    having
        panel_length_minutes < 2000
)
select
    surgery_procedure.or_key,
    surgery_procedure.patient_name,
    surgery_procedure.mrn,
    surgery_encounter.dob,
    stg_patient.sex,
    stg_patient.race_ethnicity,
    surgery_procedure.csn,
    surgery_procedure.encounter_date,
    surgery_procedure.surgery_date,
    surgery_procedure.case_status,
    surgery_procedure.panel_number,
    surgery_procedure.procedure_seq_num,
    initcap(surgery_procedure.primary_surgeon) as primary_surgeon,
    provider.prov_id as primary_surgeon_provider_id,
    surgery_procedure.service,
    nvl2(lookup_surgery_division_service.surgery_division, 1, 0) as surgery_department_ind,
    lookup_surgery_division_service.surgery_division,
    surgery_procedure.log_id,
    lower(surgery_procedure.or_procedure_name) as or_procedure_name,
    surgery_procedure.or_proc_id,
    surgery_procedure.cpt_code,
    surgery_procedure.wound_class,
    lower(surgery_procedure.laterality) as laterality,
    surgery_procedure.nhsn_category,
    surgery_encounter.surgery_age_years,
    surgery_encounter.location,
    surgery_encounter.location_group,
    surgery_encounter.room,
    surgery_encounter.patient_class,
    panel_times.panel_start_time,
    panel_times.panel_end_time,
    panel_times.panel_length_minutes,
    or_case.rec_create_dt as surgery_placed_date,
    year(add_months(surgery_procedure.surgery_date, 6)) as fiscal_year,
    year(surgery_procedure.surgery_date) as calendar_year,
    date_trunc('month', surgery_procedure.surgery_date) as calendar_month,
    surgery_procedure.case_key,
    surgery_procedure.log_key,
    surgery_procedure.pat_key,
    surgery_procedure.hsp_acct_key,
    surgery_procedure.visit_key,
    surgery_procedure.or_proc_key,
    surgery_procedure.surgeon_prov_key,
    surgery_procedure.source_system
from
    {{ ref('surgery_procedure') }} as surgery_procedure
    inner join {{ ref('surgery_encounter') }} as surgery_encounter
        on surgery_procedure.or_key = surgery_encounter.or_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = surgery_procedure.pat_key
    left join {{source('cdw', 'or_case')}} as or_case
        on surgery_procedure.case_key = or_case.or_case_key
    left join {{source('cdw', 'provider')}} as provider
        on surgery_procedure.surgeon_prov_key = provider.prov_key
    left join panel_times
        on surgery_procedure.or_key = panel_times.or_key
        and surgery_procedure.panel_number = panel_times.panel_number
        and surgery_procedure.surgeon_prov_key = panel_times.surgeon_prov_key
    left join {{ref('lookup_surgery_division_service')}} as lookup_surgery_division_service
        on surgery_procedure.service = lookup_surgery_division_service.service
