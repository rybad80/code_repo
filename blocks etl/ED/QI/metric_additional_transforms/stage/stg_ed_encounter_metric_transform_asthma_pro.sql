with epic_texting as (
    select
      stg_ed_encounter_cohort_all.visit_key,
      1 as epic_text_enabled_ind,
      group_concat(pat_vis_notif_cncts.visitor_cell_ph_num) as epic_text_cell_numbers
    from
      {{ref('stg_ed_encounter_cohort_all')}} as stg_ed_encounter_cohort_all
      inner join {{ref('stg_encounter_ed')}} as stg_encounter_ed
          on stg_ed_encounter_cohort_all.visit_key = stg_encounter_ed.visit_key
      inner join {{source('clarity_ods','pat_vis_notif_cncts')}} as pat_vis_notif_cncts
          on stg_encounter_ed.csn = pat_vis_notif_cncts.pat_enc_csn_id
    where
      lower(pat_vis_notif_cncts.notify_with_text_yn) = 'y'
    group by
      stg_ed_encounter_cohort_all.visit_key
)

select
  stg_ed_encounter_cohort_all.visit_key,
  coalesce(epic_texting.epic_text_enabled_ind, 0) as epic_text_enabled_ind,
  epic_texting.epic_text_cell_numbers
from
  {{ref('stg_ed_encounter_cohort_all')}} as stg_ed_encounter_cohort_all
  left join epic_texting as epic_texting
    on stg_ed_encounter_cohort_all.visit_key = epic_texting.visit_key
