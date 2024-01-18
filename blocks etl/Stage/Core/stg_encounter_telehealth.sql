{{ config(
	materialized='table',
	dist='visit_key',
	meta={
		'critical': true
	}
) }}

select
    visit_key,
    mrn,
    patient_name,
    dob,
    age_years,
    encounter_date,
    cohort_logic,
    csn,
    visit_type,
    visit_type_id,
    count(distinct provider_key) as n_visit_providers,
    '[' || group_concat(
        '{"'
            || 'provider_id": "' || provider_id
            || '", "provider_name": "' || provider_name
            || '", "prov_key": "' || provider_key
            || '"}',
        ','
    ) || ']' as providers_seen,
    pat_key
from
    {{ ref('stg_telehealth_encounter_provider') }}
group by
    visit_key,
    mrn,
    patient_name,
    dob,
    age_years,
    encounter_date,
    cohort_logic,
    csn,
    visit_type,
    visit_type_id,
    pat_key
