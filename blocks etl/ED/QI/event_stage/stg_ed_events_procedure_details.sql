with unique_organisms as (
    select
      stg_ed_events_procedure_details_sequenced.visit_key,
      stg_ed_events_procedure_details_sequenced.event_category,
      stg_ed_events_procedure_details_sequenced.event_name,
      stg_ed_events_procedure_details_sequenced.result_organism,
      stg_ed_events_procedure_details_sequenced.event_repeat_number,
      min(stg_ed_events_procedure_details_sequenced.result_date) as result_date
    from
      {{ref('stg_ed_events_procedure_details_sequenced')}} as stg_ed_events_procedure_details_sequenced
    where
      stg_ed_events_procedure_details_sequenced.result_organism is not null
    group by
      stg_ed_events_procedure_details_sequenced.visit_key,
      stg_ed_events_procedure_details_sequenced.event_category,
      stg_ed_events_procedure_details_sequenced.event_name,
      stg_ed_events_procedure_details_sequenced.result_organism,
      stg_ed_events_procedure_details_sequenced.event_repeat_number
),

unique_specimen_sources as (
    select
      stg_ed_events_procedure_details_sequenced.visit_key,
      stg_ed_events_procedure_details_sequenced.event_category,
      stg_ed_events_procedure_details_sequenced.event_name,
      stg_ed_events_procedure_details_sequenced.specimen_source,
      stg_ed_events_procedure_details_sequenced.event_repeat_number,
      min(stg_ed_events_procedure_details_sequenced.specimen_taken_date) as specimen_taken_date
    from
      {{ref('stg_ed_events_procedure_details_sequenced')}} as stg_ed_events_procedure_details_sequenced
    where
      stg_ed_events_procedure_details_sequenced.specimen_source is not null
    group by
      stg_ed_events_procedure_details_sequenced.visit_key,
      stg_ed_events_procedure_details_sequenced.event_category,
      stg_ed_events_procedure_details_sequenced.event_name,
      stg_ed_events_procedure_details_sequenced.specimen_source,
      stg_ed_events_procedure_details_sequenced.event_repeat_number
)

-- Ordering of resultables indicator/time
select
  stg_ed_events_procedure_details_sequenced.visit_key,
  stg_ed_events_procedure_details_sequenced.event_category,
  stg_ed_events_procedure_details_sequenced.event_name || '_order' as event_name,
  'procedure_order_result_clinical' as event_source,
  min(stg_ed_events_procedure_details_sequenced.placed_date) as event_timestamp,
  '1' as meas_val,
  stg_ed_events_procedure_details_sequenced.event_repeat_number,
  1 as event_varchar_length
from
  {{ref('stg_ed_events_procedure_details_sequenced')}} as stg_ed_events_procedure_details_sequenced
where
  stg_ed_events_procedure_details_sequenced.placed_date is not null
group by
  stg_ed_events_procedure_details_sequenced.visit_key,
  stg_ed_events_procedure_details_sequenced.event_category,
  stg_ed_events_procedure_details_sequenced.event_name,
  stg_ed_events_procedure_details_sequenced.event_repeat_number

union all

-- Ordered Group Concat of Results within Event Repeat
select
  stg_ed_events_procedure_details_sequenced.visit_key,
  stg_ed_events_procedure_details_sequenced.event_category,
  stg_ed_events_procedure_details_sequenced.event_name || '_result' as event_name,
  'procedure_order_result_clinical' as event_source,
  min(stg_ed_events_procedure_details_sequenced.result_date) as event_timestamp,
  replace(
    regexp_replace(
      group_concat(
        coalesce(stg_ed_events_procedure_details_sequenced.ordered_by_result_value, ''),
        '|'
      ),
      '@[0-9]{4}@[0-9]{20}@[0-9]{4}@',
      ''
    ),
    '|',
    '; '
  ) as meas_val,
  stg_ed_events_procedure_details_sequenced.event_repeat_number,
  -- Temporary patch for Netezza Limitation (to be replaced in Snowflake)
  case
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'blood_culture_all'
        then 1000 -- Note: This will still truncate extreme cases, but limited by spring field
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'csf_culture_all'
        then 1001 -- Note: This will still truncate extreme cases, but limited by spring field
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'mrsa_culture'
        then 750
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'urine_culture_first'
        then 500
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'blood_culture_first'
        then 500
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'csf_culture_first_ip'
        then 500
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'csf_rbc_all_ip'
        then 200
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'csf_culture_first_ed'
        then 200
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'csf_wbc_all_ip'
        then 200
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'urine_gram_stain_first'
        then 150
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'flu_first'
        then 150
      when lower(stg_ed_events_procedure_details_sequenced.event_name) = 'csf_gram_stain_first_ed'
        then 100
    else 50
  end as event_varchar_length
from
  {{ref('stg_ed_events_procedure_details_sequenced')}} as stg_ed_events_procedure_details_sequenced
where
  stg_ed_events_procedure_details_sequenced.ordered_by_result_value is not null
group by
  stg_ed_events_procedure_details_sequenced.visit_key,
  stg_ed_events_procedure_details_sequenced.event_category,
  stg_ed_events_procedure_details_sequenced.event_name,
  stg_ed_events_procedure_details_sequenced.event_repeat_number

union all

-- Ordered Group Concat of Organisms within Event Repeat
select
  unique_organisms.visit_key,
  unique_organisms.event_category,
  unique_organisms.event_name || '_organism' as event_name,
  'procedure_order_result_clinical' as event_source,
  min(unique_organisms.result_date) as event_timestamp,
  replace(
    group_concat(
      unique_organisms.result_organism,
      '|'
    ),
    '|',
    '; '
  ) as meas_val,
  unique_organisms.event_repeat_number,
  -- Temporary patch for Netezza Limitation (to be replaced in Snowflake)
  case
      when lower(unique_organisms.event_name) = 'blood_culture_all'
        then 175
    else 150
  end as event_varchar_length
from
  unique_organisms as unique_organisms
group by
  unique_organisms.visit_key,
  unique_organisms.event_category,
  unique_organisms.event_name,
  unique_organisms.event_repeat_number

union all

-- Ordered Group Concat of Distinct Specimen Sources within Event Repeat
select
  unique_specimen_sources.visit_key,
  unique_specimen_sources.event_category,
  unique_specimen_sources.event_name || '_specimen' as event_name,
  'procedure_order_result_clinical' as event_source,
  min(unique_specimen_sources.specimen_taken_date) as event_timestamp,
  replace(
    group_concat(
      unique_specimen_sources.specimen_source,
      '|'
    ),
    '|',
    '; '
  ) as meas_val,
  unique_specimen_sources.event_repeat_number,
  -- Temporary patch for Netezza Limitation (to be replaced in Snowflake)
  case
      when lower(unique_specimen_sources.event_name) = 'blood_culture_all'
        then 150
      when lower(unique_specimen_sources.event_name) = 'mrsa_culture'
        then 100
      when lower(unique_specimen_sources.event_name) = 'urine_culture_first'
        then 100
      when lower(unique_specimen_sources.event_name) = 'blood_culture_first'
        then 100
    else 50
  end as event_varchar_length
from
  unique_specimen_sources as unique_specimen_sources
group by
  unique_specimen_sources.visit_key,
  unique_specimen_sources.event_category,
  unique_specimen_sources.event_name,
  unique_specimen_sources.event_repeat_number
