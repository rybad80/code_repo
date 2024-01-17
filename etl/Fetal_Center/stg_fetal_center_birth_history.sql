with sdu_babies as (
    select distinct
        pat_key
    from
        {{ ref('stg_adt_all') }}
    where
        department_group_name = 'SDU'
)

select
    ob_hsb_delivery.summary_block_id as delivery_episode_id,
    episode_pregnancy.episode_id as pregnancy_episode_id,
    pat_enc_baby.pat_enc_csn_id as csn_baby,
    patient_baby.gestational_age_complete_weeks,
    patient_baby.gestational_age_remainder_days,
    patient_baby.sex,
    pat_enc_mom.pat_enc_csn_id as csn_mother,
    patient_mom.pat_key as mother_pat_key,
    ob_hsb_delivery.ob_hx_gest_age as gestational_age_total_days,
    ob_hsb_delivery.ob_del_birth_dttm,
    episode_pregnancy.ob_wrk_edd_dt as estimated_delivery_date,
    episode_pregnancy.number_of_babies
from
    {{ source('clarity_ods', 'ob_hsb_delivery') }} as ob_hsb_delivery
    left join {{ source('clarity_ods', 'episode') }} as episode_delivery
        on ob_hsb_delivery.summary_block_id = episode_delivery.episode_id
    inner join {{ source('clarity_ods', 'episode') }} as episode_pregnancy
        on episode_delivery.ob_del_preg_epi_id = episode_pregnancy.episode_id
    left join {{ source('clarity_ods', 'pat_enc') }} as pat_enc_mom
        on ob_hsb_delivery.delivery_date_csn = pat_enc_mom.pat_enc_csn_id
    left join {{ ref('stg_patient') }} as patient_mom
        on pat_enc_mom.pat_id = patient_mom.pat_id
    left join {{ source('clarity_ods', 'pat_enc') }} as pat_enc_baby
        on ob_hsb_delivery.baby_birth_csn = pat_enc_baby.pat_enc_csn_id
    left join {{ ref('stg_patient') }} as patient_baby
        on pat_enc_baby.pat_id = patient_baby.pat_id
    left join sdu_babies
        on sdu_babies.pat_key = patient_baby.pat_key
where
    ob_hsb_delivery.ob_del_epis_type_c = '10'
    /* Remove patients who were seen at CHOP, and weren't delivered here but WERE seen here at a later date */
    and (
        (sdu_babies.pat_key is null and patient_baby.dob is null
            and ob_hsb_delivery.ob_del_deliv_meth_c is null) -- includes babies that are not yet born
        or (sdu_babies.pat_key is not null and patient_baby.dob is not null) --includes already born babies
        or (sdu_babies.pat_key is null and ob_hsb_delivery.ob_del_deliv_meth_c is not null
            and ob_hsb_delivery.ob_del_birth_dttm is not null) --includes stillborn babies
    )
