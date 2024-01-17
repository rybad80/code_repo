with delivery_summary as (
    select
        stg_fetal_center_birth_history.pregnancy_episode_id,
        /*
            these fields _should_ be unique at the pregnancy level but, rarely,
            some bad data makes it through. Aggregating to bypass those cases
        */
        min(
            timezone(stg_fetal_center_birth_history.ob_del_birth_dttm, 'UTC', 'America/New_York')
        ) as delivery_date,
        min(zc_ped_delivr_meth.name) as delivery_method
    from
        {{ ref('stg_fetal_center_birth_history') }} as stg_fetal_center_birth_history
        left join {{ source('clarity_ods', 'ob_hsb_delivery') }} as ob_hsb_delivery
            on stg_fetal_center_birth_history.delivery_episode_id = ob_hsb_delivery.summary_block_id
        left join {{ source('clarity_ods', 'zc_ped_delivr_meth') }} as zc_ped_delivr_meth
            on ob_hsb_delivery.ob_del_deliv_meth_c = zc_ped_delivr_meth.ped_delivr_meth_c
    group by
        stg_fetal_center_birth_history.pregnancy_episode_id
),

pregnancy_summary as (
    select
        stg_pregnancy.episode_id as pregnancy_episode_id,
        stg_pregnancy.pat_link_id,
        stg_pregnancy.number_of_babies as viable_birth_count,
        stg_pregnancy.ob_wrk_edd_dt as estimated_delivery_date,
        dim_episode_status.epsd_stat_nm as pregnancy_episode_status,
        to_timestamp(delivery_summary.delivery_date, 'YYYY-MM-DD HH24:MI:SS') as delivery_date,
        delivery_summary.delivery_method
    from
        {{ source('clarity_ods', 'episode') }} as stg_pregnancy
        left join delivery_summary
            on stg_pregnancy.episode_id = delivery_summary.pregnancy_episode_id
        left join {{ source('cdw', 'dim_episode_status') }} as dim_episode_status
            on stg_pregnancy.status_c = dim_episode_status.epsd_stat_id
    where
        stg_pregnancy.sum_blk_type_id = 8
        and stg_pregnancy.pat_link_id is not null
)

select
    pregnancy_summary.pregnancy_episode_id,
    pregnancy_summary.pregnancy_episode_status,
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.dob,
    stg_fetal_center_pregnancy_metrics.gravida_count,
    stg_fetal_center_pregnancy_metrics.para_count,
    stg_fetal_center_pregnancy_metrics.sab_count,
    stg_fetal_center_pregnancy_metrics.ectopic_count,
    pregnancy_summary.viable_birth_count,
    stg_fetal_center_pregnancy_metrics.term_count,
    stg_fetal_center_pregnancy_metrics.preterm_count,
    stg_fetal_center_pregnancy_metrics.tab_count,
    pregnancy_summary.estimated_delivery_date,
    pregnancy_summary.delivery_date,
    coalesce(pregnancy_summary.delivery_date::timestamp, pregnancy_summary.estimated_delivery_date)
        as pregnancy_reference_date,
    pregnancy_summary.delivery_method,
    stg_patient.pat_key
from
    pregnancy_summary
    left join {{ ref('stg_fetal_center_pregnancy_metrics') }} as stg_fetal_center_pregnancy_metrics
        on pregnancy_summary.pregnancy_episode_id = stg_fetal_center_pregnancy_metrics.episode_id
    left join {{ ref('stg_patient') }} as stg_patient
        on pregnancy_summary.pat_link_id = stg_patient.pat_id
    where stg_patient.mrn is not null
