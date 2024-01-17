with procedures as (
    select
        r_surg_key,
        group_concat(r_proc_nm, ';') as r_proc_nm,
        group_concat(r_proc_term_32, ';') as r_proc_term_32
    from
        {{source('cdw', 'registry_sts_procedure')}} as registry_sts_procedure
        inner join {{source('cdw', 'registry_sts_surgery_procedure')}} as registry_sts_surgery_procedure
            on registry_sts_surgery_procedure.r_proc_key = registry_sts_procedure.r_proc_key
    group by
        r_surg_key
),
cardiac_surgery as (
    select
        cast(registry_sts_surgery.r_surg_key as varchar(10)) as cardiac_study_id,
        registry_hospital_visit.pat_key,
        r_surg_dt as cardiac_study_date,
        r_proc_term_32,
        'Surgery' as cardiac_type,
        case
            when r_hsp_id = 4 then r_proc_loc
            else r_proc_loc || '-' || r_hsp_nm
        end as study_location,
        registry_sts_provider.r_prov_last_nm || ', ' || registry_sts_provider.r_prov_first_nm as study_provider
    from
        {{source('cdw', 'registry_sts_surgery')}} as registry_sts_surgery
        inner join procedures
            on registry_sts_surgery.r_surg_key = procedures.r_surg_key
        inner join {{source('cdw', 'registry_hospital_visit')}} as registry_hospital_visit
            on registry_sts_surgery.r_hsp_vst_key = registry_hospital_visit.r_hsp_vst_key
        inner join {{ref('stg_patient')}} as stg_patient
            on registry_hospital_visit.pat_key = stg_patient.pat_key
        inner join {{source('cdw', 'registry_sts_provider')}} as registry_sts_provider
            on registry_sts_surgery.surgn_r_prov_key = registry_sts_provider.r_prov_key
        inner join {{source('cdw', 'registry_hospital')}} as registry_hospital
            on registry_hospital.r_hsp_key = registry_hospital_visit.r_hsp_key
    where
        r_surg_dt >= '2003-01-01'
        and registry_sts_surgery.cur_rec_ind = 1
),
cath as (
    select
        cath_study.cath_study_id as cardiac_study_id,
        pat_key,
        cast(cast(study_date_key as varchar(8)) as timestamp) as study_date,
        cath_study.procedure_type as study_type,
        'Cath' as cardiac_type,
        cath_study.procedure_location as study_location,
        cath_study.cathing_physician as study_provider
    from
        {{source('cdw', 'cath_study')}} as cath_study
        inner join {{source('cdw', 'patient')}} as patient
            on cath_study.patient_key = patient.pat_key
    where
        not(
            (lower(case_id) like 'cp%'
            and lower(case_id) not like 'cpru%'
            and lower(case_id) not like 'cpr%'
            )
        or lower(case_id) like 'cx%'
        )
),
echo as (
    select
        echo_study.echo_study_id,
        pat_key,
        cast(cast(study_date_key as varchar(8)) as timestamp) as study_date,
        upper(echo_study.study_type) as study_type,
        'Ped Echo' as cardiac_type,
        echo_study.site as study_location,
        echo_study.attending as study_provider
    from
        {{source('cdw', 'echo_study')}} as echo_study
        inner join {{source('cdw', 'patient')}} as patient
            on echo_study.patient_key = patient.pat_key
),
fetalecho as (
    select
        echo_fetal_study.echo_fetal_study_id as cardiac_study_id,
        pat_key,
        cast(cast(study_date_key as varchar(8)) as timestamp) as study_date,
        upper(echo_fetal_study.study_type) as study_type,
        'Fetal Echo' as cardiac_type,
        echo_fetal_study.site as study_location,
        echo_fetal_study.attending as study_provider
    from
        {{source('cdw', 'echo_fetal_study')}} as echo_fetal_study
        inner join {{source('cdw', 'patient')}} as patient
            on echo_fetal_study.patient_key = patient.pat_key
),
ekg as (
    select
        ekg_study.ekg_study_id as cardiac_study_id,
        pat_key,
        cast(cast(study_date_key as varchar(8)) as timestamp) as study_date,
        ekg_study.study_type,
        'EKG' as cardiac_type,
        ekg_study.location as study_location,
        ekg_study.confirming_physician as study_provider
    from
        {{source('cdw', 'ekg_study')}} as ekg_study
        inner join {{source('cdw', 'patient')}} as patient
            on ekg_study.patient_key = patient.pat_key
        inner join {{source('ccis_ods', 'muse_tsttests')}} as muse_tsttests
            on muse_tsttests.testid = ekg_study.source_system_id
),
exercise as (
    select
        cast(visit_key as varchar(20)) as cardiac_study_id,
        pat_key,
        encounter_date,
        initcap(visit_type) as study_type,
        initcap(visit_type) as cardiac_type,
        department_name,
        initcap(provider.full_nm) as provider_name
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
    where
        lower(visit_type) = 'exercise'
        and lower(appointment_status) = 'completed'
),
holter as (
    select
        cast(visit_key as varchar(20)) as cardiac_study_id,
        pat_key,
        encounter_date,
        initcap(visit_type) as study_type,
        initcap(visit_type) as cardiac_type,
        department_name,
        initcap(provider.full_nm) as provider_name
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
    where
        lower(visit_type) = 'holter'
        and lower(appointment_status) = 'completed'
),
mri as (
    select
        cast(stg_encounter.visit_key as varchar(20)) as cardiac_study_id,
        stg_encounter.pat_key,
        stg_encounter.encounter_date,
        stg_encounter.visit_type as study_type,
        'Cardiac MRI' as cardiac_type,
        stg_encounter.department_name as visit_department,
        initcap(provider.full_nm) as provider_name
    from
        {{ref('stg_encounter')}} as stg_encounter
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
    where
        lower(stg_encounter.appointment_status) in ('completed', 'arrived')
        and lower(stg_encounter.visit_type) like '%mr%heart%'
),
mrlymph as (
    select distinct
        cast(procedure_order_appointment.visit_key as varchar(20)),
        stg_encounter.pat_key,
        stg_encounter.encounter_date,
        procedure_order_clinical.procedure_name as study_type,
        'MR Lymphangiogram' as cardiac_type,
        stg_encounter.department_name as visit_department,
        initcap(provider.full_nm) as provider_name
    from
        {{ref('procedure_order_clinical')}} as procedure_order_clinical
        inner join {{source('cdw', 'procedure_order_appointment')}} as procedure_order_appointment
            on procedure_order_appointment.proc_ord_key = procedure_order_clinical.proc_ord_key
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = procedure_order_clinical.visit_key
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
    where
        lower(procedure_name) like '%mr%lymph%'
        and lower(order_status) in ('not applicable', 'completed')
),
ttm as (
    select
        cast(ord.proc_ord_key as varchar(20)),
        ord.pat_key,
        stg_encounter.encounter_date,
        ord.procedure_name,
        'TTM' as cardiac_type,
        stg_encounter.department_name as visit_department,
        initcap(provider.full_nm) as provider_name
    from
        {{ref('procedure_order_clinical')}} as ord
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.visit_key = ord.visit_key
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
    where
        lower(procedure_name) = 'op card transtelephonic monitor'
        and lower(order_status) in ('not applicable', 'completed')
        and lower(procedure_order_type) = 'future order'
),
studies as (
    select * from cath
    union all
    select * from echo
    union all
    select * from fetalecho
    union all
    select * from ekg
    union all
    select * from cardiac_surgery
    union all
    select * from holter
    union all
    select * from exercise
    union all
    select * from mri
    union all
    select * from ttm
    union all
    select * from mrlymph
),
cardiac_mapping as (
    select
        record as rec,
        lower(field_nm) as field_nm,
        cast(value as varchar(150)) as val,
        field_order
    from
        {{source('cdw', 'master_redcap_project')}} as proj
        inner join {{source('cdw', 'master_redcap_question')}} as q
            on proj.mstr_project_key = q.mstr_project_key
        inner join {{source('cdw', 'redcap_detail')}} as a
            on a.mstr_redcap_quest_key = q.mstr_redcap_quest_key
    where
        proj.project_id = 952
        and proj.cur_rec_ind = 1
        and q.cur_rec_ind = 1
        and a.cur_rec_ind = 1
),
provider_list as (
    select
        rec,
        max(case when field_nm = 'study_provider' then val end) as study_provider,
        max(case when field_nm = 'provider_last_nm' then val end) as provider_last_nm,
        max(case when field_nm = 'provider_first_nm' then val end) as provider_first_nm
    from
        cardiac_mapping
    group by
            rec
),
location_list as (
    select
        rec,
        max(case when field_nm = 'study_location' then val end) as study_location,
        max(case when field_nm = 'study_location_mapped' then val end) as study_location_mapped
    from
        cardiac_mapping
    group by
        rec
),
locations as (
    select distinct
        study_location,
        study_location_mapped
    from
        location_list
),
providers as (
    select distinct
        study_provider,
        case
            when provider_first_nm is null then provider_last_nm
            else provider_last_nm || ', ' || provider_first_nm
        end as provider_name
    from
        provider_list
)
select distinct
    studies.cardiac_study_id,
    studies.pat_key,
    patient.pat_mrn_id as mrn,
    patient.full_nm as patient_name,
    patient.sex,
    patient.dob,
    studies.study_date,
    studies.study_type,
    studies.cardiac_type,
    upper(coalesce(locations.study_location_mapped, studies.study_location)) as study_location,
    upper(coalesce(providers.provider_name, studies.study_provider)) as study_provider,
    case when encounter_inpatient.csn is not null then 1 else 0 end as inpatient_ind
from
    studies
    inner join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = studies.pat_key
    left join {{ref('encounter_inpatient')}} as encounter_inpatient
        on studies.pat_key = encounter_inpatient.pat_key
        and date(studies.study_date)
            between date(encounter_inpatient.hospital_admit_date)
            and coalesce(encounter_inpatient.hospital_discharge_date, '2199-01-01') --noqa:L016
    left join providers
        on upper(providers.study_provider) = upper(trim(both ' ' from studies.study_provider))
    left join locations
        on upper(locations.study_location) = upper(studies.study_location)
   where
        date(studies.study_date) < date(now())
