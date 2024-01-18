{{ config(meta = {
    'critical': false
}) }}

with h9e_dx_inclusion as (
    select
        usnews_billing.tx_id,
        usnews_billing.source_summary,
        max(coalesce(usnwr_dx.inclusion_ind, 0)) as inclusion_ind
    from
        {{ ref('usnews_billing') }} as usnews_billing
        inner join
            {{ref('usnews_metadata_calendar')}} as usnwr_dx
            on usnews_billing.icd10_code = usnwr_dx.code
            and lower(usnwr_dx.code_type) = 'icd10_code'
            and usnwr_dx.question_number = 'h9'
    group by
        tx_id,
        usnews_billing.source_summary
),

stage as (
select distinct
    stg_usnews_neuro_cont_eeg_volume.domain, --noqa: L029
    'finance' as subdomain,
    stg_usnews_neuro_cont_eeg_volume.primary_key,
    stg_usnews_neuro_cont_eeg_volume.division,
    stg_usnews_neuro_cont_eeg_volume.question_number,
    stg_usnews_neuro_cont_eeg_volume.metric_name,
    stg_usnews_neuro_cont_eeg_volume.submission_year,
    stg_usnews_neuro_cont_eeg_volume.mrn,
    stg_usnews_neuro_cont_eeg_volume.patient_name,
    stg_usnews_neuro_cont_eeg_volume.dob,
    stg_usnews_neuro_cont_eeg_volume.metric_date,
    stg_usnews_neuro_cont_eeg_volume.metric_id,
    stg_usnews_neuro_cont_eeg_volume.num,
    null as denom,
    stg_usnews_neuro_cont_eeg_volume.age_years,
    stg_usnews_neuro_cont_eeg_volume.index_date,
    stg_usnews_neuro_cont_eeg_volume.cpt_code,
    stg_usnews_neuro_cont_eeg_volume.procedure_name,
    null as subsequent_date,
    stg_usnews_neuro_cont_eeg_volume.department_specialty,
    stg_usnews_neuro_cont_eeg_volume.provider_specialty,
    stg_usnews_neuro_cont_eeg_volume.provider_name,
    '0' as visit_key
from
    {{ ref('stg_usnews_neuro_cont_eeg_volume') }} as stg_usnews_neuro_cont_eeg_volume
union distinct
/* Unique Patients */
select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    usnews_billing.pat_key as primary_key,
    usnews_billing.division,
    usnews_billing.question_number,
    usnews_billing.metric_name,
    usnews_billing.submission_year,
    usnews_billing.mrn,
    usnews_billing.patient_name,
    usnews_billing.dob,
    usnews_billing.service_date as metric_date,
    usnews_billing.metric_id,
    usnews_billing.mrn as num,
    null as denom,
    usnews_billing.age_years,
    usnews_billing.service_date as index_date,
    usnews_billing.cpt_code,
    usnews_billing.procedure_name,
    null as subsequent_date,
    usnews_billing.department_specialty,
    usnews_billing.provider_specialty,
    usnews_billing.provider_name,
    '0' as visit_key
from
    {{ ref('usnews_billing') }} as usnews_billing
where
    usnews_billing.metric_id in (
        'h16a1',
        'h16b1',
        'h16c1',
        'h16d1',
        'h16e1',
        'h16f1',
        'h16g1',
        'h16h1',
        'h16i1',
        'h16j1',
        'h16k1',
        'h16l1',
        'h33',
        'h31a1',
        'h31a2',
        'h31b1',
        'h31b2',
        'h31c1',
        'h31c2',
        'h31.1a', -- question changed from h31a to h31.1
        'h31.1b',
        'h31.1c',
        'h31.2a', -- question changed from h31b to h31.2
        'h31.2b',
        'h31.2c',
        'h31.3a', -- question changed from h31c to h31.3
        'h31.3b',
        'h31.3c',
        'h29_denom'
    )
    and (lower(usnews_billing.department_specialty) in ('neurosurgery', 'neurology')
        or lower(usnews_billing.provider_specialty) in ('neu', 'nrs'))
    and (lower(usnews_billing.department_specialty) != 'genetics')

union all

/* Unique Patients with special exclusion criteria */
select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    stg_neuro_epilepsy_surgery.pat_key as primary_key,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_neuro_epilepsy_surgery.mrn,
    stg_neuro_epilepsy_surgery.patient_name,
    stg_neuro_epilepsy_surgery.dob,
    stg_neuro_epilepsy_surgery.surgery_date as metric_date,
    usnews_metadata_calendar.metric_id,
    stg_neuro_epilepsy_surgery.mrn as num,
    null as denom,
    stg_neuro_epilepsy_surgery.surgery_age_years,
    stg_neuro_epilepsy_surgery.surgery_date as index_date,
    null as cpt_code,
    null as procedure_name,
    null as subsequent_date,
    null as department_specialty,
    null as provider_specialty,
    null as provider_name,
    stg_neuro_epilepsy_surgery.visit_key
from
    {{ ref('stg_neuro_epilepsy_surgery') }} as stg_neuro_epilepsy_surgery
    inner join {{ ref('usnews_metadata_calendar') }} as usnews_metadata_calendar
        on usnews_metadata_calendar.metric_id = 'h8'
        and stg_neuro_epilepsy_surgery.surgery_date
            between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
union all
/* Unique Patients with special inclusion criteria */
select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    usnews_billing.pat_key as primary_key,
    usnews_billing.division,
    usnews_billing.question_number,
    usnews_billing.metric_name,
    usnews_billing.submission_year,
    usnews_billing.mrn,
    usnews_billing.patient_name,
    usnews_billing.dob,
    usnews_billing.service_date as metric_date,
    usnews_billing.metric_id,
    usnews_billing.mrn as num,
    null as denom,
    usnews_billing.age_years,
    usnews_billing.service_date as index_date,
    usnews_billing.cpt_code,
    usnews_billing.procedure_name,
    null as subsequent_date,
    usnews_billing.department_specialty,
    usnews_billing.provider_specialty,
    usnews_billing.provider_name,
    '0' as visit_key
from
    {{ ref('usnews_billing') }} as usnews_billing
    inner join h9e_dx_inclusion
        on usnews_billing.tx_id = h9e_dx_inclusion.tx_id
            and usnews_billing.source_summary = h9e_dx_inclusion.source_summary
where
    usnews_billing.metric_name
        = 'Number of first-time surgical procedures for epilepsy' -- to account for question change from h9d to h9e
    and (lower(usnews_billing.department_specialty) in ('neurosurgery', 'neurology')
        or lower(usnews_billing.provider_specialty) in ('neu', 'nrs'))
    and h9e_dx_inclusion.inclusion_ind = 1

union all

/* Patient Deaths */
select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    usnews_billing.pat_key as primary_key,
    usnews_billing.division,
    usnews_billing.question_number,
    usnews_billing.metric_name,
    usnews_billing.submission_year,
    usnews_billing.mrn,
    usnews_billing.patient_name,
    usnews_billing.dob,
    usnews_billing.service_date as metric_date,
    usnews_billing.metric_id,
    usnews_billing.mrn as num,
    null as denom,
    usnews_billing.age_years,
    usnews_billing.service_date as index_date,
    usnews_billing.cpt_code,
    usnews_billing.procedure_name,
    stg_patient.death_date as subsequent_date,
    usnews_billing.department_specialty,
    usnews_billing.provider_specialty,
    usnews_billing.provider_name,
    '0' as visit_key
from
    {{ ref('usnews_billing') }} as usnews_billing
    inner join {{ ref('stg_patient') }} as stg_patient
        on usnews_billing.pat_key = stg_patient.pat_key
        and death_date is not null
where
    usnews_billing.metric_id in (
        'h16a2',
        'h16b2',
        'h16c2',
        'h16d2',
        'h16e2',
        'h16f2',
        'h16k2',
        'h16l2'
    )
    and (lower(usnews_billing.department_specialty) in ('neurosurgery', 'neurology')
        or lower(usnews_billing.provider_specialty) in ('neu', 'nrs'))
    and date(stg_patient.death_date) - date(usnews_billing.service_date) <= 30

union all

/* Readmissions */
select
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    usnews_billing.pat_key as primary_key,
    usnews_billing.division,
    usnews_billing.question_number,
    usnews_billing.metric_name,
    usnews_billing.submission_year,
    usnews_billing.mrn,
    usnews_billing.patient_name,
    usnews_billing.dob,
    usnews_billing.service_date as metric_date,
    usnews_billing.metric_id,
    case
        when usnews_billing.metric_id like 'h17_1'
        then usnews_billing.mrn
        else encounter_inpatient.mrn
        end as num,
    case
        when usnews_billing.metric_id like 'h17_3'
        then usnews_billing.mrn
    end as denom,
    usnews_billing.age_years,
    usnews_billing.service_date as index_date,
    usnews_billing.cpt_code,
    usnews_billing.procedure_name,
    case
        when usnews_billing.metric_id like 'h17_2' or usnews_billing.metric_id like 'h17_3'
        then min(encounter_inpatient.hospital_admit_date)
    end as subsequent_date,
    usnews_billing.department_specialty,
    usnews_billing.provider_specialty,
    usnews_billing.provider_name,
    '0' as visit_key
from
    {{ref('usnews_billing')}} as usnews_billing
    left join {{ref('encounter_inpatient')}} as encounter_inpatient
        on usnews_billing.pat_key = encounter_inpatient.pat_key
        and lower(encounter_inpatient.hsp_acct_patient_class) = 'inpatient'
        and lower(encounter_inpatient.admission_type) = 'emergent'
        and date(encounter_inpatient.hospital_admit_date) > usnews_billing.service_date
        and days_between(usnews_billing.service_date, encounter_inpatient.hospital_admit_date) <= 30
where
    usnews_billing.metric_id in (
        'h17a1',
        'h17b1',
        'h17c1',
        'h17d1',
        'h17e1',
        'h17f1',
        'h17a2',
        'h17b2',
        'h17c2',
        'h17d2',
        'h17e2',
        'h17f2',
        'h17a3',
        'h17b3',
        'h17c3',
        'h17d3',
        'h17e3',
        'h17f3'
        )
    and (lower(usnews_billing.department_specialty) in ('neurosurgery', 'neurology')
        or lower(usnews_billing.provider_specialty) in ('neu', 'nrs'))
    and (lower(usnews_billing.department_specialty) != 'genetics')
    and (num is not null or denom is not null)
group by
    usnews_billing.pat_key,
    usnews_billing.division,
    usnews_billing.question_number,
    usnews_billing.metric_name,
    usnews_billing.submission_year,
    usnews_billing.mrn,
    usnews_billing.patient_name,
    usnews_billing.dob,
    usnews_billing.service_date,
    usnews_billing.metric_id,
    encounter_inpatient.mrn,
    usnews_billing.age_years,
    usnews_billing.cpt_code,
    usnews_billing.procedure_name,
    usnews_billing.department_specialty,
    usnews_billing.provider_specialty,
    usnews_billing.provider_name

union
select distinct
    stg_usnews_neuro_h9.domain,
    stg_usnews_neuro_h9.subdomain,
    stg_usnews_neuro_h9.primary_key,
    stg_usnews_neuro_h9.division,
    stg_usnews_neuro_h9.question_number,
    stg_usnews_neuro_h9.metric_name,
    stg_usnews_neuro_h9.submission_year,
    stg_usnews_neuro_h9.mrn,
    stg_usnews_neuro_h9.patient_name,
    stg_usnews_neuro_h9.dob,
    stg_usnews_neuro_h9.metric_date,
    stg_usnews_neuro_h9.metric_id,
    stg_usnews_neuro_h9.num,
    null as denom,
    stg_usnews_neuro_h9.age_years,
    stg_usnews_neuro_h9.index_date,
    stg_usnews_neuro_h9.cpt_code,
    stg_usnews_neuro_h9.procedure_name,
    null as subsequent_date,
    stg_usnews_neuro_h9.department_specialty,
    stg_usnews_neuro_h9.provider_specialty,
    stg_usnews_neuro_h9.provider_name,
    '0' as visit_key
from
    {{ref('stg_usnews_neuro_h9')}} as stg_usnews_neuro_h9
)

select
    stage.domain,
    stage.subdomain,
    stage.primary_key,
    stage.division,
    stage.question_number,
    stage.metric_name,
    stage.submission_year,
    stage.mrn,
    stage.patient_name,
    stage.dob,
    stage.metric_date,
    stage.metric_id,
    stage.num,
    stage.denom,
    stage.age_years,
    stage.index_date,
    stage.cpt_code,
    stage.procedure_name,
    stage.subsequent_date,
    stage.department_specialty,
    stage.provider_specialty,
    stage.provider_name,
    stage.visit_key
from
    stage
