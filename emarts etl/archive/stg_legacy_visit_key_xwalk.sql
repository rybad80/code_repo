/*
This will be a one-time run on deployment-day
*/
with max_visit as (
    select max(visit_key) + 1 as max_visit_key
    from {{ source('cdw', 'visit') }}
),
stage as (
    select
        visit.visit_key as legacy_visit_key,
        visit.enc_id::int::varchar(50) as encounter_id,
        '' as patient_id, /* Remove patient_id for Clarity due to patient merged */
        lower(visit.create_by) as source_name
    from
        {{ source('cdw', 'visit') }} as visit
        left join {{ source('cdw', 'patient') }} as patient
            on patient.pat_key = visit.pat_key
    where
        lower(visit.create_by) = 'clarity'
    union all
    /* Fastrack encounter id in CDWPRD..VISIT == fastrack enc id + 3 digit padded patient id + ".005"
       So length - 7 (3 digit pat id + .005) gives enc id */
    select
        visit.visit_key as legacy_visit_key,
        substring(replace(enc_id, '.005', ''), -3)::int::varchar(50) as encounter_id,
        strleft(enc_id, length(enc_id)-7)::int::varchar(50) as patient_id,
        lower(visit.create_by) as source_name
    from
        {{ source('cdw', 'visit') }} as visit
    where
        lower(visit.create_by) = 'fastrack'
    union all
    select
        visit.visit_key as legacy_visit_key,
        visit.enc_id::varchar(50) as encounter_id,
        patient.pat_id::varchar(50) as patient_id,
        lower(visit.create_by) as source_name
    from
        {{ source('cdw', 'visit') }} as visit
        left join {{ source('cdw', 'patient') }} as patient
            on patient.pat_key = visit.pat_key
    where
        lower(visit.create_by) in ('idx', 'idxrad')
)
select
    legacy_visit_key,
    abs({{
        dbt_utils.surrogate_key([
            'encounter_id',
            'patient_id',
            'source_name'
        ])
    }}) + max_visit.max_visit_key as dbt_visit_key,
    encounter_id,
    patient_id,
    source_name
from
    stage
    inner join max_visit on 1 = 1
union all
select distinct
    visit_key as legacy_visit_key,
    visit_key as dbt_visit_key,
    null as encounter_id,
    null as patient_id,
    null as source_name
from
    {{ source('cdw', 'visit') }}
where
    visit_key <= 0
