with flowsheet_raw as (
    select
      flowsheet_all.visit_key,
      flowsheet_all.flowsheet_record_id,
      flowsheet_all.seq_num as fs_rec_seq_num,
      flowsheet_all.flowsheet_id,
      flowsheet_all.meas_val,
      flowsheet_all.meas_cmt,
      flowsheet_all.recorded_date,
      flowsheet_all.entry_date,
      row_number() over(partition by
                          fact_edqi.visit_key,
                          flowsheet_all.flowsheet_id
                        order by
                          case
                              when flowsheet_all.meas_val is not null
                                then 1
                              else -1
                          end desc,
                          flowsheet_all.recorded_date desc,
                          flowsheet_all.seq_num desc
      ) as row_value_nonnull_desc,
      row_number() over(partition by
                          fact_edqi.visit_key,
                          flowsheet_all.flowsheet_id
                        order by
                          case
                              when flowsheet_all.meas_cmt is not null
                                then 1
                              else -1
                          end desc, -- non-null requested if any are not null
                          flowsheet_all.recorded_date desc,
                          flowsheet_all.seq_num desc
      ) as row_cmt_nonnull_desc
    from
      {{source('cdw_analytics', 'fact_edqi')}} as fact_edqi
      inner join {{ref('flowsheet_all')}} as flowsheet_all
        on fact_edqi.visit_key = flowsheet_all.visit_key
    where
      flowsheet_all.flowsheet_id in (
        3008924 -- chop r ed language interpretation
      )
)

select
  fact_edqi.visit_key,
  max(
    case
        when flowsheet_raw.flowsheet_id = 3008924 -- chop r ed language interpretation 
             and flowsheet_raw.row_value_nonnull_desc = 1
          then cast(flowsheet_raw.meas_val as varchar(50))
    end
  ) as ed_lang_value_raw,
  max(
    case
        when flowsheet_raw.flowsheet_id = 3008924 -- chop r ed language interpretation 
             and flowsheet_raw.row_cmt_nonnull_desc = 1
          then cast(flowsheet_raw.meas_cmt as varchar(255))
    end
  ) as ed_visit_language_comment,
  case
      when ed_lang_value_raw is not null
        then ed_lang_value_raw
      when ed_visit_language_comment is not null
        then 'Unknown'
  end as ed_visit_language
from
  {{source('cdw_analytics', 'fact_edqi')}} as fact_edqi
  left join flowsheet_raw as flowsheet_raw on fact_edqi.visit_key = flowsheet_raw.visit_key
group by
  fact_edqi.visit_key
