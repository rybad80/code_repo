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
        stg_fetal_center_birth_history.pregnancy_episode_id,
        stg_fetal_center_birth_history.mother_pat_key as pat_key,
        ob_total.pat_enc_date_real,
        episode.number_of_babies as viable_birth_count,
        episode.ob_wrk_edd_dt as estimated_delivery_date,
        dim_episode_status.epsd_stat_nm as pregnancy_episode_status,
        coalesce(ob_total.ob_living - episode.number_of_babies, 0) as living_at_delivery_count,
        case when ob_total.ob_multiple_births > 0 then 1 else 0 end as multiple_births_ind,
        to_timestamp(delivery_summary.delivery_date, 'YYYY-MM-DD HH24:MI:SS') as delivery_date,
        delivery_summary.delivery_method,
        row_number() over (
            partition by stg_fetal_center_birth_history.pregnancy_episode_id
            order by ob_total.pat_enc_date_real desc
        ) as row_num
    from
        {{ ref('stg_fetal_center_birth_history') }} as stg_fetal_center_birth_history
        left join delivery_summary
            on stg_fetal_center_birth_history.pregnancy_episode_id = delivery_summary.pregnancy_episode_id
        left join {{ source('clarity_ods', 'ob_total') }} as ob_total
            on stg_fetal_center_birth_history.csn_mother = ob_total.hx_link_enc_csn_id
            and date(stg_fetal_center_birth_history.ob_del_birth_dttm) = ob_total.contact_date
        inner join {{ source('clarity_ods', 'episode') }} as episode
            on stg_fetal_center_birth_history.pregnancy_episode_id = episode.episode_id
        left join {{ source('cdw', 'dim_episode_status') }} as dim_episode_status
            on episode.status_c = dim_episode_status.epsd_stat_id

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
    pregnancy_summary.living_at_delivery_count,
    pregnancy_summary.multiple_births_ind,
    pregnancy_summary.delivery_date,
    pregnancy_summary.delivery_method,
    pregnancy_summary.pat_key
from
    pregnancy_summary
    left join {{ ref('stg_fetal_center_pregnancy_metrics') }} as stg_fetal_center_pregnancy_metrics
        on pregnancy_summary.pregnancy_episode_id = stg_fetal_center_pregnancy_metrics.episode_id
    left join {{ ref('stg_patient') }} as stg_patient
        on pregnancy_summary.pat_key = stg_patient.pat_key
where
    pregnancy_summary.row_num = 1
