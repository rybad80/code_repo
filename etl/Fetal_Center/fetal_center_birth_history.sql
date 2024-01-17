with post_sdu_department as (
    select
        adt_department.csn,
        adt_department.enter_date,
        adt_department.department_name as post_sdu_department_name,
        adt_department.department_group_name as post_sdu_department_group_name,
        row_number() over (
            partition by adt_department.csn
            order by adt_department.enter_date
        ) as post_sdu_department_order
    from
        {{ ref('adt_department') }} as adt_department
    where
        lower(adt_department.department_group_name) not in ('sdu', 'ormain')
)

select
    ob_hsb_delivery.summary_block_id as delivery_episode_id,
    stg_fetal_center_birth_history.pregnancy_episode_id,
    stg_encounter.csn,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.dob,
    stg_fetal_center_birth_history.sex,
    stg_fetal_center_birth_history.gestational_age_complete_weeks,
    stg_fetal_center_birth_history.gestational_age_remainder_days,
    stg_fetal_center_birth_history.gestational_age_total_days,
    timezone(stg_fetal_center_birth_history.ob_del_birth_dttm,
                'UTC', 'America/New_York')::timestamp as delivery_date,
    zc_ped_delivr_meth.name as delivery_method,
    zc_ob_hx_is_living.name as living_status,
    ob_hsb_delivery.ob_del_apgar_1_c as apgar_1_min,
    ob_hsb_delivery.ob_del_apgar_5_c as apgar_5_min,
    ob_hsb_delivery.ob_del_apgar_10_c as apgar_10_min,
    post_sdu_department.post_sdu_department_group_name,
    encounter_inpatient_baby.discharge_department,
    encounter_inpatient_baby.discharge_disposition,
    case
        when post_sdu_department.post_sdu_department_name is not null
            then post_sdu_department.post_sdu_department_name
        when zc_ob_hx_is_living.name = 'Fetal Demise' then 'Fetal Demise'
        when zc_ob_hx_is_living.name = 'Neonatal Demise' then 'Neonatal Demise'
        when encounter_inpatient_baby.discharge_disposition = 'Expired' then 'Expired'
        when encounter_inpatient_baby.discharge_department = '6 WEST' then '6 WEST'
    end as sdu_disposition,
    encounter_inpatient_baby.visit_key,
    encounter_inpatient_baby.pat_key,
    encounter_inpatient_mom.visit_key as mother_visit_key,
    encounter_inpatient_mom.pat_key as mother_pat_key
from
    {{ ref('stg_fetal_center_birth_history') }} as stg_fetal_center_birth_history
    left join {{ source('clarity_ods', 'ob_hsb_delivery') }} as ob_hsb_delivery
        on stg_fetal_center_birth_history.delivery_episode_id = ob_hsb_delivery.summary_block_id
    left join {{ source('clarity_ods', 'zc_ped_delivr_meth') }} as zc_ped_delivr_meth
        on ob_hsb_delivery.ob_del_deliv_meth_c = zc_ped_delivr_meth.ped_delivr_meth_c
    left join {{ source('clarity_ods', 'delivery_liv_sts') }} as delivery_liv_sts
        on ob_hsb_delivery.summary_block_id = delivery_liv_sts.summary_block_id
        and delivery_liv_sts.line = 1
    left join {{ source('clarity_ods', 'zc_ob_hx_is_living') }} as zc_ob_hx_is_living
        on delivery_liv_sts.del_living_status_c = zc_ob_hx_is_living.ob_hx_is_living_c
    left join post_sdu_department
        on stg_fetal_center_birth_history.csn_baby = post_sdu_department.csn
        and post_sdu_department.post_sdu_department_order = 1
    left join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.csn = stg_fetal_center_birth_history.csn_baby
    left join {{ ref('encounter_inpatient') }} as encounter_inpatient_baby
        on encounter_inpatient_baby.csn = stg_fetal_center_birth_history.csn_baby
    left join {{ ref('encounter_inpatient') }} as encounter_inpatient_mom
        on encounter_inpatient_mom.csn = stg_fetal_center_birth_history.csn_mother
