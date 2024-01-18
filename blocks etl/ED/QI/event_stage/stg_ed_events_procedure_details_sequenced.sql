select
  stg_ed_events_procedure_details_raw.visit_key,
  stg_ed_events_procedure_details_raw.event_category,
  stg_ed_events_procedure_details_raw.event_name,
  stg_ed_events_procedure_details_raw.placed_date,
  stg_ed_events_procedure_details_raw.specimen_taken_date,
  stg_ed_events_procedure_details_raw.result_date,
  coalesce(clarity_organism.external_name, clarity_organism.name) as result_organism,
  stg_ed_events_procedure_details_raw.specimen_source,
  dense_rank() over (
      partition by
          stg_ed_events_procedure_details_raw.visit_key,
          stg_ed_events_procedure_details_raw.event_name
      order by
          stg_ed_events_procedure_details_raw.placed_date asc,
          case
              when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                   or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                then current_date
              else coalesce(stg_ed_events_procedure_details_raw.specimen_taken_date, '99991231'::date)
          end asc,
          -- Regardless of first/last, still prefer finals over preliminary/wip
          case
              when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                   or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                then 0
              when lower(stg_ed_events_procedure_details_raw.result_status) = 'preliminary'
                  then 1
              when lower(stg_ed_events_procedure_details_raw.result_status) = 'incomplete'
                  then 2
              else 0
          end asc,
          case
              when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                   or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                then 0
              when lower(stg_ed_events_procedure_details_raw.result_lab_status) like '%final%'
                  then 0
              else 1
          end asc,
          -- end preferring finals
          case
              when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                   or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                then current_date
              else coalesce(stg_ed_events_procedure_details_raw.proc_order_first_result_ts, '99991231'::date)
          end asc,
          case
              when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                   or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                then 0
              else stg_ed_events_procedure_details_raw.procedure_order_id
          end asc
  ) as event_ts_repeat_asc,
  case
      when lower(stg_ed_events_procedure_details_raw.event_selection_type) = 'all'
          then 1
      when lower(stg_ed_events_procedure_details_raw.event_selection_type) = 'first'
          then event_ts_repeat_asc
      when lower(stg_ed_events_procedure_details_raw.event_selection_type) = 'last'
          then dense_rank() over (
          partition by
              stg_ed_events_procedure_details_raw.visit_key,
              stg_ed_events_procedure_details_raw.event_name
          order by
              stg_ed_events_procedure_details_raw.placed_date desc,
              case
                  when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                       or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                    then current_date
                  else coalesce(stg_ed_events_procedure_details_raw.specimen_taken_date, '19000101'::date)
              end desc,
              -- Regardless of first/last, still prefer finals over preliminary/wip
              case
                  when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                       or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                    then 0
                  when lower(stg_ed_events_procedure_details_raw.result_status) = 'preliminary'
                      then 1
                  when lower(stg_ed_events_procedure_details_raw.result_status) = 'incomplete'
                      then 2
                  else 0
              end asc,
              case
                when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                       or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                    then 0
                  when lower(stg_ed_events_procedure_details_raw.result_lab_status) like '%final%'
                      then 0
                  else 1
              end asc,
              -- end preferring finals
              case
                  when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                       or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                    then current_date
                  else coalesce(stg_ed_events_procedure_details_raw.proc_order_first_result_ts, '19000101'::date)
              end desc,
              case
                  when lower(stg_ed_events_procedure_details_raw.event_name) like '%culture%'
                       or lower(stg_ed_events_procedure_details_raw.event_name) like '%gram_stain%'
                    then 0
                  else stg_ed_events_procedure_details_raw.procedure_order_id
              end desc
          )
      else 1
  end as event_repeat_number,
  -- Trick group_concat with an order by (Proc ID never has remainder/max 9 digits as of 2023)
  (
      '@'
      || lpad(event_ts_repeat_asc, 4, '0')
      || '@'
      || lpad(stg_ed_events_procedure_details_raw.procedure_order_id::int, 20, '0')
      || '@'
      || lpad(stg_ed_events_procedure_details_raw.result_seq_num, 4, '0')
      || '@'
      || case
          when lower(stg_ed_events_procedure_details_raw.result_value) like
                  -- already contains organism
                  '%' || lower(result_organism) || '%'
              then stg_ed_events_procedure_details_raw.result_value
          else nullif(
              btrim(
              coalesce(stg_ed_events_procedure_details_raw.result_value, '')
              || coalesce(' (' || result_organism || ')', '')
              ),
              ''
          )
      end
  ) as ordered_by_result_value
from
  {{ref('stg_ed_events_procedure_details_raw')}} as stg_ed_events_procedure_details_raw
  left join {{ source('clarity_ods', 'order_results') }} as order_results
      on stg_ed_events_procedure_details_raw.procedure_order_id = order_results.order_proc_id
          and stg_ed_events_procedure_details_raw.result_seq_num = order_results.line
          and stg_ed_events_procedure_details_raw.result_component_id = order_results.component_id
  left join {{ source('clarity_ods', 'clarity_organism') }}  as clarity_organism
      on order_results.lrr_based_organ_id = clarity_organism.organism_id
