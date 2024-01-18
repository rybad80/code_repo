with raw_flowsheets as (
    select
      lookup.event_category,
      lookup.event_name,
      flowsheet_all.visit_key,
      flowsheet_all.flowsheet_id,
      flowsheet_all.recorded_date,
      flowsheet_all.flowsheet_record_id,
      lookup.selection_type,
      case
          when lower(lookup.selection_type) in (
            'all',
            '0'     -- Also accept 0 as "all" (see ED QI README)
          )
            then 'text' -- Could explore array/collections, but not until needed/Snowflake
          else lookup.output_type
      end as output_type,
      lookup.care_settings,
      row_number()
        over(
          partition by
            flowsheet_all.visit_key,
            lookup.event_name
          order by
            flowsheet_all.recorded_date asc,
            flowsheet_all.flowsheet_record_id asc,
            flowsheet_all.entry_date desc
      ) as flow_rn_asc,
      row_number()
        over(
          partition by
            flowsheet_all.visit_key,
            lookup.event_name
          order by
            flowsheet_all.recorded_date desc,
            flowsheet_all.flowsheet_record_id desc,
            flowsheet_all.entry_date desc
      ) as flow_rn_desc,
      case
          when lower(lookup.selection_type) in (
            'all',
            '0'     -- Also accept 0 as "all" (see ED QI README)
          )
            then 1
          when lower(lookup.selection_type) = 'first'
               and flow_rn_asc = 1
            then 1
          when lower(lookup.selection_type) = 'last'
               and flow_rn_desc = 1
            then 1
          when cast(regexp_extract(btrim(lookup.selection_type), '^\d+$') as int) = flow_rn_asc  -- + Asc Counting
            then 1
          when abs(cast(regexp_extract(btrim(lookup.selection_type), '^-\d+$') as int)) = flow_rn_desc -- Neg Desc
            then 1
          else 0
      end as selected_row_ind,
      flowsheet_all.meas_val as reporting_value_text_raw,
      -- Text Responses: Preparing to trick NZSQL into group_concat with an order by
      (
        '@'
          || lpad(flow_rn_asc, 5, '0')
          || '@'
          || reporting_value_text_raw
      ) as reporting_value_text_padded,
      flowsheet_all.meas_val_num as reporting_value_numeric
    from
      {{ ref('stg_ed_encounter_cohort_all') }} as cohort
      inner join {{ ref('flowsheet_all') }} as flowsheet_all
        on cohort.visit_key = flowsheet_all.visit_key
      inner join {{ ref('lookup_ed_events_flowsheets') }} as lookup
        on flowsheet_all.flowsheet_id = lookup.flowsheet_id
    where
      flowsheet_all.meas_val is not null
      and (
        (
          '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/ed/%'
          and flowsheet_all.recorded_date <= coalesce(
            cohort.disch_ed_dt,
            cohort.depart_ed_dt,
            current_date
          )
        )
        or (
          '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/edecu/%'
          and flowsheet_all.recorded_date between cohort.admit_edecu_dt and cohort.disch_edecu_dt
        )
        or (
          '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/ip/%'
          and flowsheet_all.recorded_date  >= coalesce(
            cohort.disch_edecu_dt,
            cohort.disch_ed_dt,
            cohort.depart_ed_dt
          )
        )
      )
)

select
  raw_flowsheets.visit_key,
  raw_flowsheets.event_name,
  raw_flowsheets.output_type,
  max(raw_flowsheets.recorded_date) as recorded_date,
  max(raw_flowsheets.reporting_value_numeric) as reporting_value_numeric,
  cast(
    regexp_replace(
      group_concat(
          raw_flowsheets.reporting_value_text_padded,
          '|'
      ),
      '@[0-9]{5}@',
      ''
    ) as varchar(512)
  ) as reporting_value_text
from
  raw_flowsheets as raw_flowsheets
where
  raw_flowsheets.selected_row_ind = 1
group by
  raw_flowsheets.visit_key,
  raw_flowsheets.event_name,
  raw_flowsheets.output_type
