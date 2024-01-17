with raw_smart_data_elements as (
    select
      lookup.event_category,
      lookup.event_name,
      smart_data_element_all.visit_key,
      smart_data_element_all.concept_id,
      smart_data_element_all.entered_date,
      smart_data_element_all.sde_key,
      smart_data_element_all.seq_num,
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
      lookup.note_type,
      row_number()
        over(
          partition by
            smart_data_element_all.visit_key,
            lookup.event_name
          order by
            smart_data_element_all.entered_date asc,
            smart_data_element_all.sde_key asc,
            smart_data_element_all.seq_num asc
      ) as rn_asc,
      dense_rank()
        over(
          partition by
            smart_data_element_all.visit_key,
            lookup.event_name
          order by
            smart_data_element_all.entered_date asc,
            smart_data_element_all.sde_key asc
      ) as rank_asc,
      dense_rank()
        over(
          partition by
            smart_data_element_all.visit_key,
            lookup.event_name
          order by
            smart_data_element_all.entered_date desc,
            smart_data_element_all.sde_key desc
      ) as rank_desc,
      case
          when lower(lookup.selection_type) in (
            'all',
            '0'     -- Also accept 0 as "all" (see ED QI README)
          )
            then 1
          when lower(lookup.selection_type) = 'first'
               and rank_asc = 1
            then 1
          when lower(lookup.selection_type) = 'last'
               and rank_desc = 1
            then 1
          when cast(regexp_extract(btrim(lookup.selection_type), '^\d+$') as int) = rank_asc  -- Pos Asc Counting
            then 1
          when abs(cast(regexp_extract(btrim(lookup.selection_type), '^-\d+$') as int)) = rank_desc -- Neg Desc
            then 1
          else 0
      end as selected_row_ind,
      smart_data_element_all.element_value,
      -- Logic to prepare raw text responses
      case
          -- Converting boolean to Yes/No
          when lower(zc_data_type.name) = 'boolean'
            then decode(
              smart_data_element_all.element_value,
              '1',
              'Yes',
              '0',
              'No',
              null
            )
          -- Converting element response to next-step in tree's abbreviation/name
          when lower(zc_data_type.name) = 'element id'
            then coalesce(
              response_concept.abbreviation,
              response_concept.name
            )
          -- String representation for date (extracts, etc.)
          when lower(zc_data_type.name) = 'date'
            then date_dimension.calendar_dt_str
          -- Converting SER database lookup to name
          when lower(zc_data_type.name) = 'database'
               and lower(clarity_concept.master_file_link) = 'ser'
               and clarity_concept.master_file_item = .1
            then clarity_ser.prov_name
          -- Passing varchar value (max 512 extracted per Clarity data dictionary)
          else cast(smart_data_element_all.element_value as varchar(512))
      end as reporting_value_text_raw,
      -- Text Responses: Preparing to trick NZSQL into group_concat with an order by
      (
        '@'
          || lpad(rn_asc, 5, '0')
          || '@'
          || reporting_value_text_raw
      ) as reporting_value_text_padded,
      -- Logic to prepare numeric responses
      case
          -- Providing a numeric boolean to ease future logic/indicators
          when lower(zc_data_type.name) = 'boolean'
            then cast(smart_data_element_all.element_value as numeric(18, 2))
          -- Providing a numeric represenation of date in year/month/day format (common for date dim keys)
          when lower(zc_data_type.name) = 'date'
            then cast(to_char(date_dimension.calendar_dt, 'yyyymmdd') as numeric(18, 2))
          -- Passing anything Epic can convert to numeric as is
          else smart_data_element_all.element_value_numeric
      end as reporting_value_numeric,
      -- Logic to prepare date responses
      case
          when lower(zc_data_type.name) = 'date'
            then cast(date_dimension.calendar_dt as timestamp)
      end as reporting_value_timestamp
    from
      {{ ref('stg_ed_encounter_cohort_all') }} as cohort
      inner join {{ ref('smart_data_element_all') }} as smart_data_element_all
        on cohort.visit_key = smart_data_element_all.visit_key
      left join {{ref('stg_ed_events_smart_data_element_notes')}} as stg_ed_events_smart_data_element_notes
        on smart_data_element_all.linked_field = 'note_info.note_id'
           and smart_data_element_all.rec_id_char = stg_ed_events_smart_data_element_notes.note_id
      inner join {{ ref('lookup_ed_events_smart_data_element_all') }} as lookup
        on smart_data_element_all.concept_id = lookup.concept_id
           and (
             lower(smart_data_element_all.element_value) = lower(lookup.element_value)
             or lookup.element_value is null
           )
           and (
            stg_ed_events_smart_data_element_notes.ip_note_type_c = lookup.note_type
            or lookup.note_type is null
           )
      left join {{source('clarity_ods', 'clarity_concept')}} as clarity_concept
        on smart_data_element_all.concept_id = clarity_concept.concept_id
      left join {{source('clarity_ods', 'zc_data_type')}} as zc_data_type
        on clarity_concept.data_type_c = zc_data_type.data_type_c
      left join {{source('clarity_ods', 'clarity_concept')}} as response_concept
        on lower(zc_data_type.name) = 'element id'
           and smart_data_element_all.element_value = response_concept.concept_id
      left join {{source('clarity_ods', 'date_dimension')}} as date_dimension
        on case
               when lower(zc_data_type.name) = 'date'
                 then smart_data_element_all.element_value
           end = date_dimension.epic_dte
      left join {{source('clarity_ods', 'clarity_ser')}} as clarity_ser
        on case
               when lower(zc_data_type.name) = 'database'
                    and lower(clarity_concept.master_file_link) = 'ser'
                    and clarity_concept.master_file_item = .1
                 then smart_data_element_all.element_value
           end = clarity_ser.prov_id
    where
      coalesce(stg_ed_events_smart_data_element_notes.note_deleted_ind, 0) = 0
      and (
        (
          '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/ed/%'
          and smart_data_element_all.entered_date <= coalesce(
            cohort.disch_ed_dt,
            cohort.depart_ed_dt,
            current_date
          )
        )
        or (
          '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/edecu/%'
          and smart_data_element_all.entered_date between cohort.admit_edecu_dt and cohort.disch_edecu_dt
        )
        or (
          '/' || lower(coalesce(lookup.care_settings, 'ed/edecu/ip')) || '/' like '%/ip/%'
          and smart_data_element_all.entered_date  >= coalesce(
            cohort.disch_edecu_dt,
            cohort.disch_ed_dt,
            cohort.depart_ed_dt
          )
        )
      )
)

select
  raw_smart_data_elements.visit_key,
  raw_smart_data_elements.event_name,
  raw_smart_data_elements.output_type,
  max(raw_smart_data_elements.entered_date) as entered_date,
  max(raw_smart_data_elements.reporting_value_numeric) as reporting_value_numeric,
  max(raw_smart_data_elements.reporting_value_timestamp) as reporting_value_timestamp,
  cast(
    regexp_replace(
      group_concat(
          raw_smart_data_elements.reporting_value_text_padded,
          '|'
      ),
      '@[0-9]{5}@',
      ''
    ) as varchar(512)
  ) as reporting_value_text
from
  raw_smart_data_elements as raw_smart_data_elements
where
  raw_smart_data_elements.selected_row_ind = 1
group by
  raw_smart_data_elements.visit_key,
  raw_smart_data_elements.event_name,
  raw_smart_data_elements.output_type
