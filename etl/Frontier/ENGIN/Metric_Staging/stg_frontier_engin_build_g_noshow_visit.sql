-- g:growth
with
noshow_visits as (
    select
        stg_encounter.mrn,
        stg_encounter.visit_key,
        stg_encounter.visit_type,
        initcap(provider.full_nm) as provider_name,
        stg_encounter.encounter_date
    from
        {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ ref('stg_frontier_engin_op_enc_generic')}} as enc_generic
        on stg_encounter.visit_key = enc_generic.visit_key
    left join {{ ref('stg_frontier_engin_op_enc_engin')}} as enc_engin
        on stg_encounter.visit_key = enc_engin.visit_key
    where
        (enc_generic.appointment_status_id = 4 -- no show
        or enc_engin.appointment_status_id = 4 -- no show
        ) and stg_encounter.encounter_type_id != 1066 -- exclude error
)
select
    'Program-Specific: No-Show Visits' as metric_name,
    visit_key as primary_key,
    visit_type as drill_down_one,
    provider_name as drill_down_two,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'fp_engin_noshow' as metric_id,
    visit_key as num
from
    noshow_visits
