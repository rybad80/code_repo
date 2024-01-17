with demo_question as (
    select
        pg_survey_demographics.survey_id,
        max(case
            when lower(pg_survey_demographics.varname) = 'itservty'
            then pg_survey_demographics.value end) as surv_line_id,
        max(case
            when lower(pg_survey_demographics.varname) = 'prilang'
                or lower(pg_survey_demographics.varname) = 'primlang'
            then pg_survey_demographics.value end) as pg_language,
        max(case
            when lower(pg_survey_demographics.varname) = 'pg_race1'
                or lower(pg_survey_demographics.varname) = 'pg_sphla'
            then pg_survey_demographics.value end) as pg_race_ethnicity,
        max(case
            when lower(pg_survey_demographics.varname) in('itunique', 'ituniq')
            then pg_survey_demographics.value end) as patient_csn
        from {{source('ods', 'pg_survey_demographics')}} as pg_survey_demographics
    group by
        pg_survey_demographics.survey_id
),

    meta as (
        select
           pg_survey_metadata.survey_id,
            case when lower(demo_question.surv_line_id) = 'ch0101u' then 'PD0101'
            when lower(demo_question.surv_line_id) = 'ch0101ue' then 'PD0101E'
            when lower(demo_question.surv_line_id) = 'ch0102ue' then 'PD0102E'
            else demo_question.surv_line_id
            end as survey_line_id,
            case when (pg_survey_metadata.client_id) = 6210 then 'PHL Campus'
            when (pg_survey_metadata.client_id) = 38427 then 'KOPH Campus'
            end as campus,
        pg_survey_metadata.client_id,
        pg_survey_metadata.service,
        pg_survey_metadata.recdate as survey_returned_date,
           demo_question.pg_language,
           demo_question.pg_race_ethnicity,
           cast(demo_question.patient_csn as numeric(14, 3)) as csn
       from {{source('ods', 'pg_survey_metadata')}} as pg_survey_metadata
    inner join demo_question
        on pg_survey_metadata.survey_id = demo_question.survey_id
    group by
        pg_survey_metadata.survey_id,
        survey_line_id,
        campus,
        pg_survey_metadata.client_id,
        pg_survey_metadata.service,
        pg_survey_metadata.recdate,
        demo_question.pg_language,
        demo_question.pg_race_ethnicity,
        cast(demo_question.patient_csn as numeric(14, 3))
)

        select distinct
        meta.survey_id,
        meta.survey_line_id,
        meta.pg_language,
        meta.pg_race_ethnicity,
        meta.service,
        meta.client_id,
        meta.campus,
        meta.survey_returned_date,
        stg_encounter.encounter_key,
        stg_encounter.patient_name,
        stg_encounter.mrn,
        stg_encounter.csn,
        stg_encounter.pat_id,
        stg_encounter.dob,
        stg_encounter.encounter_date,
        stg_encounter.provider_name,
        stg_encounter.provider_id,
        stg_encounter.specialty_name,
        stg_encounter.department_name,
        stg_encounter.department_id,
        stg_encounter.department_key,
        stg_encounter.patient_key,
        stg_encounter.provider_key,
        stg_hsp_acct_xref.hospital_account_key,
        stg_encounter.hospital_admit_date,
        stg_encounter.hospital_discharge_date,
        stg_encounter.appointment_date,
        stg_patient.preferred_name,
        stg_patient.preferred_language as epic_language,
        stg_patient.race_ethnicity as epic_race_ethnicity,
        case when lower(meta.survey_line_id) in (
            'as0101', 'as0101e',
            'as0102', 'as0102e',
            'ch0101u', 'ch0101ue',
            'ch0102ue',
            'pd0101', 'pd0101e',
            'pd0102e',
            'iz0101u', 'iz0101ue',
            'iz0102u', 'iz0102ue',
            'nc0101', 'nc0101e',
            'pe0101', 'pe0101e',
            'uc0101', 'uc0101e',
            'as0103', 'as0103e')
            then stg_encounter.hospital_discharge_date
        when lower(meta.survey_line_id) in (
            'md0101', 'md0101e',
            'mt0101ce',
            'md0102', 'md0102e',
            'mt0102ce',
            'md0103e',
            'on0101', 'on0101e',
            'ou0101', 'ou0101e',
            'ov0101', 'ov0101e',
            'oy0101', 'oy0101e',
            'bt0101', 'bt0101e',
            'ut0101e')
            then stg_encounter.appointment_date
        when lower(meta.survey_line_id) in (
            'rh0101',
            'rh0101e',
            'hh0101',
            'hh0101e')
            then coalesce(stg_encounter.hospital_discharge_date, stg_encounter.appointment_date)
        else null
    end as visit_date,
    cast('PRESSGANEY' as varchar(20)) as create_by
from
    {{ref('stg_encounter')}} as stg_encounter
left join {{ref('stg_patient')}} as stg_patient
    on stg_encounter.pat_key = stg_patient.pat_key
left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
    on stg_encounter.encounter_key = stg_hsp_acct_xref.encounter_key
inner join meta
    on stg_encounter.csn = meta.csn
where stg_encounter.encounter_date >= '2016-01-01'
