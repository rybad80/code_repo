with bh_notes_npv_follow as (
select
    stg_bh_notes_smart_texts.note_key,
    stg_bh_notes_smart_texts.npv_ind,
    stg_bh_notes_smart_texts.follow_ind
from
    {{ref('stg_bh_notes_smart_texts')}} as stg_bh_notes_smart_texts
union
select
    stg_bh_notes_sdes_npvs.note_key,
    stg_bh_notes_sdes_npvs.npv_ind,
    stg_bh_notes_sdes_npvs.follow_ind
from
    {{ref('stg_bh_notes_sdes_npvs')}} as stg_bh_notes_sdes_npvs
)

select
    bh_notes_npv_follow.note_key,
    max(bh_notes_npv_follow.npv_ind) as npv_ind,
    max(bh_notes_npv_follow.follow_ind) as follow_ind
from
    bh_notes_npv_follow
group by
    bh_notes_npv_follow.note_key
