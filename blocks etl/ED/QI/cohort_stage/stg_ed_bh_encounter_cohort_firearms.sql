with notes_raw as (
    select
        stg_encounter_ed.visit_key,
        stg_encounter_ed.pat_key,
        hno_info.note_id,
        max(case
                when hno_info_2.starting_smarttext_id = 18771
                    then 1
                    else 0
            end) as starting_smarttext_ind,
        max(case
                when note_smarttext_ids.smarttexts_id = 18771
                    then 1
                    else 0
            end) as smarttext_ind,
        max(case
                when hno_info.delete_instant_dttm is not null
                    then 1
                    else 0
            end) as deleted_note_ind
    from
        {{ ref('stg_encounter_ed') }} as stg_encounter_ed
        inner join  {{ source('clarity_ods', 'hno_info') }} as hno_info
          on hno_info.pat_enc_csn_id = stg_encounter_ed.csn
        inner join  {{ source('clarity_ods', 'hno_info_2') }} as hno_info_2
          on hno_info.note_id = hno_info_2.note_id
        left join  {{ source('clarity_ods', 'note_smarttext_ids') }} as note_smarttext_ids
          on note_smarttext_ids.note_id = hno_info.note_id
    where
        cast(stg_encounter_ed.ed_arrival_date as date) between
          max(
            cast('20201001' as date),
            (date_trunc('year', current_date) - interval '5 years')
          )
          and current_date
        and stg_encounter_ed.ed_patients_seen_ind = 1
    group by
        stg_encounter_ed.visit_key,
        stg_encounter_ed.pat_key,
        hno_info.note_id
    having
        starting_smarttext_ind = 1
        or
        smarttext_ind = 1
)

select
  notes_raw.visit_key,
  notes_raw.pat_key,
  'BH_FIREARMS' as cohort,
  null as subcohort
from
  notes_raw
where
  notes_raw.deleted_note_ind != 1
group by
  notes_raw.visit_key,
  notes_raw.pat_key
