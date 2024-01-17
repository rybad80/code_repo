-- to be replaced with department_care_network once the definition is finalized
with visit_reason as (
    select
        pat_enc_csn_id
    from {{ source('clarity_ods', 'pat_enc_rsn_visit') }}
    where enc_reason_id in (
        28,
        47,
        82,
        115,
        120,
        141
    )
    and date(contact_date) >= '2018-07-01'
group by pat_enc_csn_id
)

select
    stg_encounter.csn,
    stg_encounter.encounter_key,
    stg_encounter.visit_key,
    stg_encounter.patient_key,
    stg_encounter.pat_key,
    stg_encounter.encounter_date,
    stg_encounter.encounter_type_id,
    stg_encounter.encounter_type,
    stg_encounter.department_id,
    stg_encounter.department_name
from {{ ref('stg_encounter') }} as stg_encounter
left join
    {{ ref('stg_erroneous_encounters_nurse_triage')}}
        as stg_erroneous_encounters_nurse_triage
    on stg_encounter.encounter_key = stg_erroneous_encounters_nurse_triage.encounter_key
inner join {{ ref('lookup_care_network_department_cost_center_sites') }} as pc_dept
    on stg_encounter.department_id = pc_dept.department_id
inner join visit_reason
    on stg_encounter.csn = visit_reason.pat_enc_csn_id
where stg_encounter.encounter_type_id in (70, 71)
    and encounter_date between '2018-07-01' and (current_date - 1)
    and stg_erroneous_encounters_nurse_triage.encounter_key is null
    and stg_encounter.department_id != 37 -- not applicable for After Hours triage
