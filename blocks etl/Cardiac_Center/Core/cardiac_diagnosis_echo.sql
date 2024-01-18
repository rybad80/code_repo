with
syngo_dx_cleanup as (
select
    studyid,
    replace(
        replace(replace(interpretdiagnosisdescription, '; ;', ';'), chr(10), ';'), chr(13), ';'
    ) as interpretdiagnosisdescription
from
    {{source('ccis_ods', 'syngo_echo_study')}}
),

syngo_dx_parse as (
select
    studyid,
    interpretdiagnosisdescription,
    length(
        interpretdiagnosisdescription
    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', '')) as number_paren_comma_check,
    length(
        interpretdiagnosisdescription
    ) - length(replace(interpretdiagnosisdescription, ';', '')) as semicolon_check,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 0 and (
                    length(interpretdiagnosisdescription) - length(replace(interpretdiagnosisdescription, ';', ''))
                ) = 0 then interpretdiagnosisdescription
        end as varchar(500)
    ) as dx0,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) > 0 then regexp_replace(
                    substring(interpretdiagnosisdescription, 1, instr(interpretdiagnosisdescription, '),', 1, 1)),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 0 then replace(
                      substring(
                          interpretdiagnosisdescription, 1, instr(interpretdiagnosisdescription, ';', 1, 1) - 1
                      ),
                      '\d\)\, ',
                      ''
                  )
              else null end as varchar(500)) as dx1,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 3 then regexp_replace(
                    substring(
                        interpretdiagnosisdescription,
                        instr(interpretdiagnosisdescription, '),', 1, 1),
                        length(interpretdiagnosisdescription)
                    ),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) = 1 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 1),
                          length(interpretdiagnosisdescription)
                      ),
                      ';',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                  ) > 3 then regexp_replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, '),', 1, 1),
                          instr(
                              interpretdiagnosisdescription, '),', 1, 2
                          ) - instr(interpretdiagnosisdescription, '),', 1, 1) + 1
                      ),
                      '\d\)\,',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 1 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 1),
                          instr(
                              interpretdiagnosisdescription, ';', 1, 2
                          ) - instr(interpretdiagnosisdescription, ';', 1, 1) + 1
                      ),
                      ';',
                      ''
                  )
              else null end as varchar(500)) as dx2,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 6 then regexp_replace(
                    substring(
                        interpretdiagnosisdescription,
                        instr(interpretdiagnosisdescription, '),', 1, 2),
                        length(interpretdiagnosisdescription)
                    ),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) = 2 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 2),
                          length(interpretdiagnosisdescription)
                      ),
                      ';',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                  ) > 6 then regexp_replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, '),', 1, 2),
                          instr(
                              interpretdiagnosisdescription, '),', 1, 3
                          ) - instr(interpretdiagnosisdescription, '),', 1, 2) + 1
                      ),
                      '\d\)\,',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 2 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 2),
                          instr(
                              interpretdiagnosisdescription, ';', 1, 3
                          ) - instr(interpretdiagnosisdescription, ';', 1, 2) + 1
                      ),
                      ';',
                      ''
                  )
              else null end as varchar(500)) as dx3,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 9 then regexp_replace(
                    substring(
                        interpretdiagnosisdescription,
                        instr(interpretdiagnosisdescription, '),', 1, 3),
                        length(interpretdiagnosisdescription)
                    ),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) = 3 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 3),
                          length(interpretdiagnosisdescription)
                      ),
                      ';',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                  ) > 9 then regexp_replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, '),', 1, 3),
                          instr(
                              interpretdiagnosisdescription, '),', 1, 4
                          ) - instr(interpretdiagnosisdescription, '),', 1, 3) + 1
                      ),
                      '\d\)\,',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 3 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 3),
                          instr(
                              interpretdiagnosisdescription, ';', 1, 4
                          ) - instr(interpretdiagnosisdescription, ';', 1, 3) + 1
                      ),
                      ';',
                      ''
                  )
              else null end as varchar(500)) as dx4,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 12 then regexp_replace(
                    substring(
                        interpretdiagnosisdescription,
                        instr(interpretdiagnosisdescription, '),', 1, 4),
                        length(interpretdiagnosisdescription)
                    ),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) = 4 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 4),
                          length(interpretdiagnosisdescription)
                      ),
                      ';',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                  ) > 12 then regexp_replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, '),', 1, 4),
                          instr(
                              interpretdiagnosisdescription, '),', 1, 5
                          ) - instr(interpretdiagnosisdescription, '),', 1, 4) + 1
                      ),
                      '\d\)\,',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 4 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 4),
                          instr(
                              interpretdiagnosisdescription, ';', 1, 5
                          ) - instr(interpretdiagnosisdescription, ';', 1, 4) + 1
                      ),
                      ';',
                      ''
                  )
              else null end as varchar(500)) as dx5,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 15 then regexp_replace(
                    substring(
                        interpretdiagnosisdescription,
                        instr(interpretdiagnosisdescription, '),', 1, 5),
                        length(interpretdiagnosisdescription)
                    ),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) = 5 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 5),
                          length(interpretdiagnosisdescription)
                      ),
                      ';',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                  ) > 15 then regexp_replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, '),', 1, 5),
                          instr(
                              interpretdiagnosisdescription, '),', 1, 6
                          ) - instr(interpretdiagnosisdescription, '),', 1, 5) + 1
                      ),
                      '\d\)\,',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 5 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 5),
                          instr(
                              interpretdiagnosisdescription, ';', 1, 6
                          ) - instr(interpretdiagnosisdescription, ';', 1, 5) + 1
                      ),
                      ';',
                      ''
                  )
              else null end as varchar(500)) as dx6,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 18 then regexp_replace(
                    substring(
                        interpretdiagnosisdescription,
                        instr(interpretdiagnosisdescription, '),', 1, 6),
                        length(interpretdiagnosisdescription)
                    ),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) = 6 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 6),
                          length(interpretdiagnosisdescription)
                      ),
                      ';',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                  ) > 18 then regexp_replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, '),', 1, 6),
                          instr(
                              interpretdiagnosisdescription, '),', 1, 7
                          ) - instr(interpretdiagnosisdescription, '),', 1, 6) + 1
                      ),
                      '\d\)\,',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 6  then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 6),
                          instr(
                              interpretdiagnosisdescription, ';', 1, 7
                          ) - instr(interpretdiagnosisdescription, ';, ', 1, 6) + 1
                      ),
                      ';',
                      ''
                  )
              else null end as varchar(500)) as dx7,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 21 then regexp_replace(
                    substring(
                        interpretdiagnosisdescription,
                        instr(interpretdiagnosisdescription, '),', 1, 7),
                        length(interpretdiagnosisdescription)
                    ),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) = 7 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 7),
                          length(interpretdiagnosisdescription)
                      ),
                      ';',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                  ) > 21 then regexp_replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, '),', 1, 7),
                          instr(
                              interpretdiagnosisdescription, '),', 1, 8
                          ) - instr(interpretdiagnosisdescription, '),', 1, 7) + 1
                      ),
                      '\d\)\,',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 7  then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 7),
                          instr(
                              interpretdiagnosisdescription, ';', 1, 8
                          ) - instr(interpretdiagnosisdescription, ';, ', 1, 7) + 1
                      ),
                      ';',
                      ''
                  )
              else null end as varchar(500)) as dx8,
    cast(
        case
            when
                (
                    length(
                        interpretdiagnosisdescription
                    ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                ) = 24 then regexp_replace(
                    substring(
                        interpretdiagnosisdescription,
                        instr(interpretdiagnosisdescription, '),', 1, 8),
                        length(interpretdiagnosisdescription)
                    ),
                    '\d\)\,',
                    ''
                )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) = 8 then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 8),
                          length(interpretdiagnosisdescription)
                      ),
                      ';',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(regexp_replace(interpretdiagnosisdescription, '\d\)\,', ''))
                  ) > 24 then regexp_replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, '),', 1, 8),
                          instr(
                              interpretdiagnosisdescription, '),', 1, 9
                          ) - instr(interpretdiagnosisdescription, '),', 1, 8) + 1
                      ),
                      '\d\)\,',
                      ''
                  )
              when
                  (
                      length(
                          interpretdiagnosisdescription
                      ) - length(replace(interpretdiagnosisdescription, ';', ''))
                  ) > 8  then replace(
                      substring(
                          interpretdiagnosisdescription,
                          instr(interpretdiagnosisdescription, ';', 1, 8),
                          instr(
                              interpretdiagnosisdescription, ';', 1, 9
                          ) - instr(interpretdiagnosisdescription, ';, ', 1, 8) + 1
                      ),
                      ';',
                      ''
                  )
              else null end as varchar(500)) as dx9
from
   syngo_dx_cleanup

),

syngo_diagnosis as (

select
      studyid,
      interpretdiagnosisdescription,
      dx0 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx0 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx1 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx1 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx2 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx2 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx3 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx3 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx4 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx4 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx5 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx5 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx6 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx6 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx7 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx7 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx8 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx8 is not null

union all

select
      studyid,
      interpretdiagnosisdescription,
      dx9 as syngo_diagnosis
from
     syngo_dx_parse
where
     dx9 is not null

),

final_dx as (
select
      studyid,
      study_date,
      interpretdiagnosisdescription,
      trim(both ' ' from trim(leading '? ' from trim(leading '), ' from syngo_diagnosis))) as echo_diagnosis
 from
      syngo_diagnosis
      inner join
          {{source('ccis_ods', 'syngo_echo_dosr_study')}} as syngo_echo_dosr_study on
              syngo_diagnosis.studyid = syngo_echo_dosr_study.study_ref

),

redcap_raw_data as (
   select distinct
         master_redcap_project.app_title,
         master_redcap_question.mstr_redcap_quest_key,
         master_redcap_question.field_order,
         master_redcap_question.field_nm,
         master_redcap_question.element_label,
         redcap_detail.record as rec,
         redcap_detail.value as rcd_value,
         substr(
             coalesce(master_redcap_element_answr.element_desc, redcap_detail.value),
             1,
             250
         ) as val,
         dense_rank() over (
             partition by
                 redcap_detail.record, redcap_detail.mstr_redcap_quest_key
             order by master_redcap_element_answr.element_id
         ) as row_num
   from
         {{source('cdw', 'redcap_detail')}} as redcap_detail
         inner join
             {{source('cdw', 'master_redcap_project')}} as master_redcap_project on
                 master_redcap_project.mstr_project_key = redcap_detail.mstr_project_key
         left join
             {{source('cdw', 'master_redcap_question')}} as master_redcap_question on
                 master_redcap_question.mstr_redcap_quest_key = redcap_detail.mstr_redcap_quest_key
                 and master_redcap_question.cur_rec_ind = 1
         left join
             {{source('cdw', 'master_redcap_element_answr')}} as master_redcap_element_answr on
                 master_redcap_element_answr.mstr_redcap_quest_key = redcap_detail.mstr_redcap_quest_key
                 and redcap_detail.value = master_redcap_element_answr.element_id
  where
        redcap_detail.cur_rec_ind = 1
        and master_redcap_project.project_id = 1138
),


redcap_dx as (
select
      rec,
      max(case when field_nm = 'echo_dx' then val else null end) as echo_diagnosis,
      max(case when field_nm = 'source_dx_name' then val else null end) as source_diagnosis,
      max(case when field_nm = 'mapping_source' then val else null end) as mapping_source,
      max(case when field_nm = 'seq_num' then val else null end) as seq_num,
      max(case when field_nm = 'source_dx_id' then val else null end) as source_dx_id,
      max(case when field_nm = 'chd_ind' then val else null end) as chd_ind
from
     redcap_raw_data
group by
     rec
)

select
       studyid || 'Syn' as cardiac_study_id,
       pat_key,
       mrn,
       patient_name,
       final_dx.echo_diagnosis as echo_diagnosis,
       source_dx_id,
       upper(upper(source_diagnosis)) as mapped_diagnosis,
       mapping_source,
       seq_num,
       chd_ind
from
     {{ref('cardiac_echo')}} as cardiac_echo
     left join final_dx on cardiac_echo.cardiac_study_id = final_dx.studyid || 'Syn'
     left join redcap_dx on lower(final_dx.echo_diagnosis) = lower(redcap_dx.echo_diagnosis)
where
     length(final_dx.echo_diagnosis) > 0
