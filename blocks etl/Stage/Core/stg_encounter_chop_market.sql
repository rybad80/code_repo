{{ config(
    materialized='table',
    dist='encounter_key',
    meta={
        'critical': true
    }
) }}

with refugee as (
    select distinct
        stg_encounter.visit_key,
        stg_encounter.patient_key,
        stg_encounter.pat_key,
        stg_encounter.encounter_key,
        '1' as refugee_patient_ind
    from
        {{ ref('stg_encounter')}} as stg_encounter
        inner join {{ ref('stg_dx_all_combos')}} as stg_dx_all_combos
            on stg_encounter.encounter_key = stg_dx_all_combos.encounter_key
        inner join {{ source('clarity_ods', 'ept_sel_smartsets')}} as ept_sel_smartsets
            on ept_sel_smartsets.pat_enc_csn_id = stg_encounter.csn
        inner join {{ source('clarity_ods', 'cl_prl_ss') }} as cl_prl_ss
            on cl_prl_ss.protocol_id = ept_sel_smartsets.selected_sset_id
    where
        (
        lower(stg_encounter.visit_type) like ('ref%') /*SEEN IN REFUGEE CLINIC*/
        or stg_dx_all_combos.diagnosis_id = '147596' /*DIAGNOSIS SPECIFIC FOR REFUGEE PATIENT EXAMINATION*/
        /*use of the smart set and appplicable dx*/
        or (
            cl_prl_ss.protocol_id in ('1051', '305', '310', '96', '1009', '1052')
            and stg_dx_all_combos.icd10_code = ('Z02.89')
            /*DIAGNOSIS SPECIFIC FOR REFUGEE PATIENT EXAMINATION*/
            and (stg_dx_all_combos.diagnosis_id = ('147596')
                or stg_dx_all_combos.diagnosis_name like ('%subpopulation%')
                )
            )
        )
)

select
    stg_encounter.encounter_key,
    stg_encounter.visit_key,
    stg_encounter.patient_key,
    stg_encounter.pat_key,
    case
        when lower(stg_patient_ods.country) != 'united states'
            and lower(stg_encounter.visit_type) like 'research%'
        then 'international'
        when refugee.refugee_patient_ind = '1'
        then 'international'
        when stg_encounter_gps.global_patient_services_ind = 1
            or stg_encounter.department_id = '101033100' --BGR GPS clinic
        then 'international'
        else coalesce(stg_encounter.chop_market_raw, 'Unknown')
    end as chop_market,
    case
        when lower(stg_patient_ods.country) != 'united states'
            and lower(stg_encounter.visit_type) like 'research%'
        then 'international'
        when refugee.refugee_patient_ind = '1'
        then 'international'
        when stg_encounter_gps.global_patient_services_ind = 1
            or stg_encounter.department_id = '101033100' --BGR GPS clinic
        then 'international'
        else coalesce(stg_encounter.region_category_raw, 'Unknown')
    end as region_category
from
    {{ ref('stg_encounter') }} as stg_encounter
    inner join {{ ref('stg_patient_ods') }} as stg_patient_ods
        on stg_patient_ods.patient_key = stg_encounter.patient_key
    left join {{ ref('stg_encounter_gps') }} as stg_encounter_gps
        on stg_encounter_gps.encounter_key = stg_encounter.encounter_key
    left join refugee
        on refugee.encounter_key = stg_encounter.encounter_key
