with same_visit as (
    select
        telehealth_encounter_all.visit_key,
        initcap(provider.full_nm) as interpreter_provider_name
    from
        {{ref ('telehealth_encounter_all') }} as telehealth_encounter_all
        inner join {{source('cdw', 'visit_appointment') }} as visit_appointment
            on telehealth_encounter_all.visit_key = visit_appointment.visit_key
        inner join {{source('cdw', 'provider') }} as provider
            on visit_appointment.prov_key = provider.prov_key
        left join {{source('cdw', 'provider_specialty') }} as  provider_specialty
            on provider.prov_key = provider_specialty.prov_key
            and provider_specialty.line = 1
    where
        telehealth_encounter_all.visit_modality = 'telehealth'
        and telehealth_encounter_all.department_id != '1015002'
        and (
            lower(provider.prov_type) = 'interpreter'
            or lower(provider.full_nm) like '%interpreter%'
        )
    group by
        telehealth_encounter_all.visit_key,
        provider.full_nm
),

different_visit  as (
    select
        interpreter_encounter.visit_key,
        telehealth_encounter_all.provider_name as interpreter_provider_name
    from
        {{ref ('stg_encounter') }} as interpreter_encounter
        inner join {{ ref('telehealth_encounter_all') }} as telehealth_encounter_all
            on telehealth_encounter_all.pat_key = interpreter_encounter.pat_key
            and telehealth_encounter_all.appointment_date = interpreter_encounter.appointment_date
    where
        telehealth_encounter_all.visit_modality = 'telehealth'
        and interpreter_encounter.department_id = '1015002'
    group by
        interpreter_encounter.visit_key,
        telehealth_encounter_all.provider_name
)

select
    stg_encounter.visit_key,
    stg_encounter.encounter_date,
    coalesce(
        same_visit.interpreter_provider_name,
        different_visit.interpreter_provider_name
    ) as interpreter_provider_name
from
    {{ ref('stg_encounter') }} as stg_encounter
    left join same_visit
        on same_visit.visit_key = stg_encounter.visit_key
    left join different_visit
        on different_visit.visit_key = stg_encounter.visit_key
where
    coalesce(same_visit.visit_key, different_visit.visit_key) is not null
