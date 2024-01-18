with note_all as (
     select
          note_edit_metadata_history.mrn,
          note_edit_metadata_history.encounter_date,
          note_edit_metadata_history.note_id,
          note_text.note_visit_key,
          note_text.seq_num,
          stg_bayley_notes.seq_num as group_seq_num,
          regexp_replace(note_text.note_text,
                         '[' || chr(128) || '-' || chr(255) || ',' || chr(63) || ']', '') as notetext
     from
          {{ref('stg_bayley_notes')}} as stg_bayley_notes
          inner join {{source('cdw', 'note_text')}} as note_text
               on stg_bayley_notes.note_visit_key = note_text.note_visit_key
          inner join {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
               on note_text.note_visit_key = note_edit_metadata_history.note_visit_key
     where
          note_edit_metadata_history.last_edit_ind = 1
          and note_text.seq_num between stg_bayley_notes.seq_num and stg_bayley_notes.seq_num + 1
          and note_edit_metadata_history.mrn != 'UNKNOWN'
     order by
          note_text.seq_num
),

notes_ordered as (
     select
          mrn,
          encounter_date,
          note_id,
          note_visit_key,
          group_seq_num,
          replace(
              regexp_replace(group_concat('~' || lpad(seq_num, 4, '0') || '~' || notetext, ' '), '~[0-9]{4}~', ''),
               '  ', ' ') as notetext
     from
          note_all
     group by
     mrn,
     encounter_date,
     note_id,
     group_seq_num,
     note_visit_key
),

bayley_note as (
     select
          mrn,
          encounter_date,
          note_id,
          note_visit_key,
          notetext,
          regexp_extract(lower(notetext), '(?<=bayley scales of infant development )\w+')
          as bayley_edition1,
          regexp_extract(lower(notetext), '(?<=bayley scales of infant and toddler development )\w+')
          as bayley_edition2,
          regexp_extract(lower(notetext), '(?<=bayley scales of infant and toddler development- )\w+')
          as bayley_edition3,
          regexp_extract(lower(notetext), '(?<=bayley scales of infant and toddler development  )\w+')
          as bayley_edition4,
          regexp_extract(lower(notetext), '(?<=bayley scales of infant and toddler development-)\w+')
          as bayley_edition5,
          regexp_replace(substring(notetext, instr(notetext, 'Composite Score Scaled Score')), ' {2,}', ' ')
          as bayley_note
     from
          notes_ordered
),

score_parse as (
     select
          note_visit_key,
          note_id,
          mrn,
          encounter_date,
          notetext,
          bayley_note,
          case
          when lower(coalesce(bayley_edition1, bayley_edition2, bayley_edition3, bayley_edition4, bayley_edition5))
               like '%thir%' then 'Third'
          when lower(coalesce(bayley_edition1, bayley_edition2, bayley_edition3, bayley_edition4, bayley_edition5))
               like '%3rd%' then 'Third'
          when lower(coalesce(bayley_edition1, bayley_edition2, bayley_edition3, bayley_edition4, bayley_edition5))
               = 'iii' then 'Third'
          when lower(coalesce(bayley_edition1, bayley_edition2, bayley_edition3, bayley_edition4, bayley_edition5))
               like '%four%' then 'Fourth'
          when lower(coalesce(bayley_edition1, bayley_edition2, bayley_edition3, bayley_edition4, bayley_edition5))
               like '%foruth%' then 'Fourth'
          else 'Not Available'
               end as bayley_edition,
          cast(regexp_extract(bayley_note, '(?<=Cognitive )\d+') as integer) as cognitive_score,
          cast(regexp_extract(bayley_note, '(?<=Language )\d+') as integer) as language_score,
          cast(regexp_extract(bayley_note, '(?<=Receptive Communication )\d+') as integer) as recept_comm_score,
          cast(regexp_extract(bayley_note, '(?<=Expressive Communication )\d+') as integer) as expr_comm_score,
          cast(regexp_extract(bayley_note, '(?<=Motor )\d+') as integer) as motor_score,
          cast(regexp_extract(bayley_note, '(?<=Fine Motor )\d+') as integer) as fine_motor_score,
          cast(regexp_extract(bayley_note, '(?<=Gross Motor )\d+') as integer) as gross_motor_score
     from
     bayley_note
     where
          lower(bayley_note) like '%composite score scaled score%'
          or lower(bayley_note) like '%standard%scaled score%'
)

select
     mrn,
     encounter_date,
     note_id,
     note_visit_key,
     bayley_edition,
     cognitive_score as cognitive_score,
     language_score as language_score,
     recept_comm_score as recept_comm_score,
     expr_comm_score as expr_comm_score,
     motor_score as motor_score,
     fine_motor_score as fine_motor_score,
     gross_motor_score as gross_motor_score
from
    score_parse
group by
     mrn,
     encounter_date,
     note_id,
     note_visit_key,
     bayley_edition,
     cognitive_score,
     language_score,
     recept_comm_score,
     expr_comm_score,
     motor_score,
     fine_motor_score,
     gross_motor_score
