with most_recent_encounters as (
    select
        ob_hx_hsb.pat_id,
        max(social_hx.pat_enc_date_real) as most_recent_encounter_date
    from
        {{ source('clarity_ods', 'ob_hx_hsb') }} as ob_hx_hsb
        inner join {{ source('clarity_ods', 'social_hx') }} as social_hx
            on social_hx.pat_id = ob_hx_hsb.pat_id
    group by
       ob_hx_hsb.pat_id
),

later_pregnancies as (
    select
        ob_hx_hsb.pat_enc_csn_id,
        ob_hx_hsb.ob_hx_preg_epis_id,
        episode.ob_hx_order as ob_hx_pregnancy_order
    from
        {{ source('clarity_ods', 'ob_hx_hsb') }} as ob_hx_hsb
        inner join most_recent_encounters
            on most_recent_encounters.pat_id = ob_hx_hsb.pat_id
        inner join  {{ source('clarity_ods', 'episode') }} as episode
            on ob_hx_hsb.ob_hx_preg_epis_id = episode.episode_id
    where
        most_recent_encounters.most_recent_encounter_date = ob_hx_hsb.pat_enc_date_real
    group by
        ob_hx_hsb.pat_enc_csn_id,
        ob_hx_hsb.ob_hx_preg_epis_id,
        episode.ob_hx_order
),

earlier_pregnancies as (
    select
        ob_hx_hsb.pat_enc_csn_id,
        ob_hx_hsb.ob_hx_preg_epis_id,
        episode.ob_hx_order as ob_hx_pregnancy_order,
        1 as gra_count,
        case
            when count(case when ob_hsb_delivery.ob_hx_outcome_c in (2, 3, 6) then 1 end) > 0 then 1
            else 0
        end as par_count,
        case
            when count(case when ob_hsb_delivery.ob_hx_outcome_c = 2 then 1 end) > 0 then 1
            else 0
        end as trm_count,
        case
            when count(case when ob_hsb_delivery.ob_hx_outcome_c = 3 then 1 end) > 0 then 1
            else 0
        end as pre_count,
        case
            when count(
                case
                    when ob_hsb_delivery.ob_hx_outcome_c in (4, 7, 8, 9) then 1
                    when emr_system_defs.facility_id is null and ob_hsb_delivery.ob_hx_outcome_c = 10 then 1
                end
            ) > 0 then 1
            else 0
        end as ab_count,
        case
            when count(case when ob_hsb_delivery.ob_hx_outcome_c = 7 then 1 end) > 0 then 1
            else 0
        end as tab_count,
        case
            when count(case when ob_hsb_delivery.ob_hx_outcome_c = 8 then 1 end) > 0 then 1
            else 0
        end as sab_count,
        case
            when count(case when ob_hsb_delivery.ob_hx_outcome_c = 9 then 1 end) > 0 then 1
            else 0
        end as ect_count
    from
        {{ source('clarity_ods', 'ob_hx_hsb') }} as ob_hx_hsb
        inner join {{ source('clarity_ods', 'episode') }} as episode
            on ob_hx_hsb.ob_hx_preg_epis_id = episode.episode_id
        left join {{ source('clarity_ods', 'ob_hsb_delivery') }} as ob_hsb_delivery
            on ob_hx_hsb.ob_hx_del_rec_id = ob_hsb_delivery.summary_block_id
        left join {{ source('clarity_ods', 'emr_system_defs') }} as emr_system_defs
            on emr_system_defs.ob_molar_is_abortion_yn = 'n'
    group by
        ob_hx_hsb.pat_enc_csn_id,
        ob_hx_hsb.ob_hx_preg_epis_id,
        episode.ob_hx_order
)

select
    later_pregnancies.ob_hx_preg_epis_id as episode_id,
    case
        when sum(earlier_pregnancies.gra_count) is null then 1 else sum(earlier_pregnancies.gra_count) + 1
    end as gravida_count,
    coalesce(sum(earlier_pregnancies.par_count), 0) as para_count,
    coalesce(sum(earlier_pregnancies.trm_count), 0) as term_count,
    coalesce(sum(earlier_pregnancies.pre_count), 0) as preterm_count,
    coalesce(sum(earlier_pregnancies.ab_count), 0) as abort_count,
    coalesce(sum(earlier_pregnancies.tab_count), 0) as tab_count,
    coalesce(sum(earlier_pregnancies.sab_count), 0) as sab_count,
    coalesce(sum(earlier_pregnancies.ect_count), 0) as ectopic_count
from
    later_pregnancies
    left join earlier_pregnancies
        on earlier_pregnancies.pat_enc_csn_id = later_pregnancies.pat_enc_csn_id
        and later_pregnancies.ob_hx_pregnancy_order > earlier_pregnancies.ob_hx_pregnancy_order
group by
    later_pregnancies.ob_hx_preg_epis_id
