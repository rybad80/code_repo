with fyi_result_indicators as (
    select
      visit_key,
      {{ ed_qi_concatenated_value_compare(
           table_relation = ref('ed_encounter_metric_procedure_details'),
           attribute_name = 'csf_wbc_all_ed_result',
           comparator = '>=',
           value_threshold = '9.0'
      ) }} as csf_wbc_all_ed_result_gtet_9_ind,
      {{ ed_qi_concatenated_value_compare(
           table_relation = ref('ed_encounter_metric_procedure_details'),
           attribute_name = 'csf_wbc_all_ed_result',
           comparator = '>=',
           value_threshold = '15.0'
      ) }} as csf_wbc_all_ed_result_gtet_15_ind,
      case
          when lower(csf_gram_stain_first_ed_result) like '% rod%'
            then 1
          when lower(csf_gram_stain_first_ed_result) like '% cocci%'
            then 1
          else 0
      end as csf_gram_stain_positive_org_ind,
      case
          when lower(leukocyte_esterase_first_result) like '%large%'
            then 1
          when lower(leukocyte_esterase_first_result) like '%moderate%'
            then 1
          when lower(leukocyte_esterase_first_result) like '%small%'
            then 1
          when lower(leukocyte_esterase_first_result) like '%trace%'
            then 1
          else 0
      end as leukocyte_esterase_pos_ind,
      case
          when lower(urine_gram_stain_first_result) like '% rod%'
            then 1
          when lower(urine_gram_stain_first_result) like '% cocci%'
            then 1
          when lower(urine_gram_stain_first_result) like '% coccobacilli%'
            then 1
          when lower(urine_gram_stain_first_result) like '% baccilus%'
            then 1
          else 0
      end as urine_gram_stain_positive_org_ind,
      case
          when lower(urine_nitrite_first_result) like '%positive%'
            then 1
          when lower(urine_nitrite_first_result) like '%1+%'
            then 1
          when lower(urine_nitrite_first_result) like '%2+%'
            then 1
          else 0
      end as urine_nitrite_pos_ind,
      {{ ed_qi_concatenated_value_compare(
        table_relation = ref('ed_encounter_metric_procedure_details'),
        attribute_name = 'urine_wbc_first_result',
        comparator = '>',
        value_threshold = '5',
        text_pattern_triggers = [
          "'5-10'",
          "'10-15'",
          "'10-25'",
          "'15-20'",
          "'20-30'",
          "'25-40'",
          "'25-50'",
          "'30-45'",
          "'45-62'",
          "'50-75'",
          "'75-100'",
          "'TNTC'"
        ]
      ) }} as urine_wbc_first_result_gt_5_ind,
      {{ ed_qi_concatenated_value_compare(
        table_relation = ref('ed_encounter_metric_procedure_details'),
        attribute_name = 'urine_wbc_first_result',
        comparator = '>=',
        value_threshold = '10',
        text_pattern_triggers = [
          "'10-15'",
          "'10-25'",
          "'15-20'",
          "'20-30'",
          "'25-40'",
          "'25-50'",
          "'30-45'",
          "'45-62'",
          "'50-75'",
          "'75-100'",
          "'TNTC'"
        ]
      ) }} as urine_wbc_first_result_gtet_10_ind,
      csf_culture_first_ed_specimen_date,
      csf_culture_first_ip_order,
      case
          when cbc_bands_last_ed_result like '%;%' -- Multiple responses (never seen, but just in case)
            then null
          when regexp_like(cbc_bands_last_ed_result,'[^0-9\.]')  -- Non-expected number
            then null
          else cast(coalesce(cbc_bands_last_ed_result, '0') as float)
      end as clean_cbc_bands_for_ratio,
      case
          when cbc_neutrophils_last_ed_result like '%;%' -- Multiple responses (never seen, but just in case)
            then null
          when regexp_like(cbc_neutrophils_last_ed_result,'[^0-9\.]')  -- Non-expected number
            then null
          else cast(cbc_neutrophils_last_ed_result as float)
      end as clean_cbc_neutrophils_for_ratio,
      case
          when lower(blood_wbc_last_ed_result) in (
            'not measured',
            'j12.7'
          )
            then 1
          else 0
      end as blood_wbc_last_ed_result_invalid_ind,
      {{ ed_qi_concatenated_value_compare(
           table_relation = ref('ed_encounter_metric_procedure_details'),
           attribute_name = 'blood_wbc_last_ed_result',
           comparator = '<',
           value_threshold = '5.0'
      ) }} as blood_wbc_last_ed_result_lt_5_ind,
      {{ ed_qi_concatenated_value_compare(
           table_relation = ref('ed_encounter_metric_procedure_details'),
           attribute_name = 'blood_wbc_last_ed_result',
           comparator = '>',
           value_threshold = '15.0'
      ) }} as blood_wbc_last_ed_result_gt_15_ind,
      case
          when lower(c_reactive_protein_first_result) in (
            'test not performed',
            'not done',
            'see comment'
          )
            then 1
          else 0
      end as c_reactive_protein_first_result_invalid_ind,
      {{ ed_qi_concatenated_value_compare(
        table_relation = ref('ed_encounter_metric_procedure_details'),
        attribute_name = 'c_reactive_protein_first_result',
        comparator = '>',
        value_threshold = '2.0',
        text_pattern_triggers = [
          "'>27.0'",
          "'>300.0'",
          "'>9.0'"
        ]
      ) }} as c_reactive_protein_first_result_gt_2_ind,
      case
          when lower(cbc_abs_neutrophils_last_ed_result) in (
            'canceled',
            'test not performed'
          )
            then 1
          else 0
      end as cbc_abs_neutrophils_last_ed_result_invalid_ind,
      {{ ed_qi_concatenated_value_compare(
           table_relation = ref('ed_encounter_metric_procedure_details'),
           attribute_name = 'cbc_abs_neutrophils_last_ed_result',
           comparator = '<',
           value_threshold = '1000.0'
      ) }} as cbc_abs_neutrophils_last_ed_result_lt_1k_ind,
      {{ ed_qi_concatenated_value_compare(
           table_relation = ref('ed_encounter_metric_procedure_details'),
           attribute_name = 'cbc_abs_neutrophils_last_ed_result',
           comparator = '>',
           value_threshold = '4000.0'
      ) }} as cbc_abs_neutrophils_last_ed_result_gt_4k_ind,
      case
          when lower(procalcitonin_first_result) in (
            'qns'
          )
            then 1
          else 0
      end as procalcitonin_first_result_invalid_ind,
      {{ ed_qi_concatenated_value_compare(
        table_relation = ref('ed_encounter_metric_procedure_details'),
        attribute_name = 'procalcitonin_first_result',
        comparator = '>',
        value_threshold = '0.5',
        text_pattern_triggers = [
          "'>100.00'",
          "'>200.00'"
        ]
      ) }} as procalcitonin_first_result_gt_p5_ind
    from
      {{ref('ed_encounter_metric_procedure_details')}}
)

select
  cohort.visit_key,
  case
      when arrive_ed_dt::date >= '20220325'
           and days_between(
                 stg_encounter_ed.dob::date,
                 stg_encounter_ed.ed_arrival_date::date
              ) < 29.0
           and fyi_result_indicators.csf_wbc_all_ed_result_gtet_15_ind = 1
        then 1
      when arrive_ed_dt::date >= '20220325'
           and days_between(
                 stg_encounter_ed.dob::date,
                 stg_encounter_ed.ed_arrival_date::date
              ) between 29.0 and 56.0
           and fyi_result_indicators.csf_wbc_all_ed_result_gtet_9_ind = 1
        then 1
      when arrive_ed_dt::date < '20220325'
           and csf_wbc_all_ed_result_gtet_9_ind = 1
        then 1
      else 0
  end as fyi_csf_wbc_abn_ind,
  coalesce(csf_gram_stain_positive_org_ind,0) as fyi_csf_gram_stain_abn_ind,
  case
      when arrive_ed_dt::date >= '20220810'
           and leukocyte_esterase_pos_ind = 1
        then 1
      when arrive_ed_dt::date < '20220810'
        then null
      else 0
  end as fyi_leukocyte_esterase_abn_bool,
  case
      when arrive_ed_dt::date >= '20220810'
        then null
      when arrive_ed_dt::date < '20220810'
           and urine_gram_stain_positive_org_ind = 1
        then 1
      else 0
  end as fyi_urine_gram_stain_abn_bool,
  coalesce(urine_nitrite_pos_ind, 0) as fyi_urine_nitrite_abn_ind,
  case
      when arrive_ed_dt::date >= '20220810'
           and urine_wbc_first_result_gt_5_ind = 1
        then 1
      when arrive_ed_dt::date < '20220810'
           and urine_wbc_first_result_gtet_10_ind = 1
        then 1
      else 0
  end as fyi_urine_wbc_abn_ind,
  case
      when fyi_result_indicators.csf_culture_first_ed_specimen_date <= coalesce(
        cohort.disch_ed_dt,
        cohort.depart_ed_dt,
        current_date
      ) -- collected in ED
        then 1
      else 0
  end as fyi_ed_lp_successful_ind,
  case
      when ed_encounter_metric_smart_data_element.ed_lp_note is not null
           and fyi_ed_lp_successful_ind = 0  -- not successful
        then 1
      else 0
  end as fyi_ed_lp_attempted_ind,
  case
      when fyi_result_indicators.csf_culture_first_ed_specimen_date > coalesce(
        cohort.disch_ed_dt,
        cohort.depart_ed_dt,
        current_date
      ) -- NOT collected in ED
        then 1
      when fyi_result_indicators.csf_culture_first_ip_order is not null
        then 1
      else 0
  end as fyi_ip_lp_successful_ind,
  case
      when clean_cbc_neutrophils_for_ratio = 0
        then null
      else (clean_cbc_bands_for_ratio / clean_cbc_neutrophils_for_ratio)
  end as cbc_bands_to_neutrophils_ratio,
  case
      when cbc_bands_to_neutrophils_ratio > .2
        then 1
      else 0
  end as cbc_bands_to_neutrophils_ratio_abn_ind,
  case
      when blood_wbc_last_ed_result_invalid_ind = 1
        then null
      when blood_wbc_last_ed_result_lt_5_ind = 1
        then 1
      when blood_wbc_last_ed_result_gt_15_ind = 1
        then 1
      else 0
  end as fyi_blood_wbc_abn_bool,
  case
      when c_reactive_protein_first_result_invalid_ind = 1
        then null
      when c_reactive_protein_first_result_gt_2_ind = 1
        then 1
      else 0
  end as c_reactive_protein_abn_bool,
  case
      when cbc_abs_neutrophils_last_ed_result_invalid_ind = 1
        then null
      when cbc_abs_neutrophils_last_ed_result_lt_1k_ind = 1
        then 1
      when cbc_abs_neutrophils_last_ed_result_gt_4k_ind = 1
        then 1
      else 0
  end as fyi_cbc_abs_neutrophils_abn_bool,
  case
      when procalcitonin_first_result_invalid_ind = 1
        then null
      when procalcitonin_first_result_gt_p5_ind = 1
        then 1
      else 0
  end as fyi_procalcitonin_abn_bool,
  max(
    coalesce(fyi_procalcitonin_abn_bool,0),
    coalesce(fyi_cbc_abs_neutrophils_abn_bool,0)
  ) as fyi_inflam_markers_ind,
  max(
    coalesce(fyi_leukocyte_esterase_abn_bool,0),
    coalesce(fyi_urine_wbc_abn_ind,0)
  ) as fyi_urine_markers_ind
from
  {{ref('stg_ed_encounter_cohort_all')}} as cohort
  inner join {{ ref('stg_encounter_ed') }} as stg_encounter_ed on cohort.visit_key = stg_encounter_ed.visit_key
  left join fyi_result_indicators as fyi_result_indicators on cohort.visit_key = fyi_result_indicators.visit_key
  left join {{ref('ed_encounter_metric_smart_data_element')}} as ed_encounter_metric_smart_data_element
    on cohort.visit_key = ed_encounter_metric_smart_data_element.visit_key