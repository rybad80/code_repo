with firearms_indicators as (
    select
      ed_encounter_metric_smart_data_element.visit_key,
      ed_encounter_metric_smart_data_element.firearm_present_first as gun_present,
      ed_encounter_metric_smart_data_element.firearm_safely_stored_first as gun_safely_stored,
      replace(ed_encounter_metric_smart_data_element.firearm_interventions_first, '|', ', ') as gun_safety_provided,
      replace(ed_encounter_metric_smart_data_element.firearm_location_first, '|', ', ') as gun_location,
      ed_encounter_metric_smart_data_element.firearm_loc_home_first as gun_location_home,
      ed_encounter_metric_smart_data_element.firearm_loc_friend_relative_first as gun_location_friend_relative,
      ed_encounter_metric_smart_data_element.firearm_loc_car_first as gun_location_car,
      ed_encounter_metric_smart_data_element.firearm_loc_other_first as gun_location_other,
      ed_encounter_metric_smart_data_element.firearm_addl_info_first as gun_additional_information,
      -- Screening Indicator
      case
          when gun_present = 'No'
              and gun_safely_stored = 'No gun reported in the home'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = '_Already Safe'
            then 1
          when gun_present = '0'
              and gun_safely_stored is null
              and gun_safety_provided = 'Declined'
            then 1
          when gun_present = 'No'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = '0'
              and gun_safely_stored is null
              and gun_safety_provided = 'Not offered'
            then 1
          when gun_present = 'Not asked'
              and gun_safely_stored = 'Not asked'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Declined'
            then 1
          when gun_present = '1'
              and gun_safely_stored is null
              and gun_safety_provided = 'Already safe'
            then 1
          when gun_present = '0'
              and gun_safely_stored is null
              and gun_safety_provided = 'Declined, Not offered'
            then 1
          when gun_present = '0'
              and gun_safely_stored is null
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Not asked'
              and gun_safely_stored = 'No gun reported in the home'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education, No gun reported'
            then 1
          when gun_present = '1'
              and gun_safely_stored is null
              and gun_safety_provided = 'Declined'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Gun Lock, Lock Box'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'No'
              and gun_safely_stored = 'Not asked'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Gun Lock'
            then 1
          when gun_present = 'No'
              and gun_safely_stored = 'No gun reported in the home'
              and gun_safety_provided = 'Declined'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Gun Lock'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No gun reported in the home'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Declined'
            then 1
          when gun_present = 'Not asked'
              and gun_safely_stored = 'Not asked'
              and gun_safety_provided = 'Education, No gun reported'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'yes'
              and gun_safety_provided is null
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and gun_safety_provided is null
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and gun_safety_provided is null
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and lower(gun_safety_provided) != 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'not asked'
              and gun_safety_provided is null
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and lower(gun_safety_provided) != 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and lower(gun_safety_provided) = 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and lower(gun_safety_provided) = 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) in ('yes', 'no', 'declined')
              and gun_safely_stored is null
              and gun_safety_provided is null
            then 1
          else 0
      end as firearm_screen_completed_ind,
      --Gun Present Indicator 
      case
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = '_Already Safe'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Declined'
            then 1
          when gun_present = '1'
              and gun_safely_stored is null
              and gun_safety_provided = 'Already safe'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education, No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Gun Lock'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'yes'
              and gun_safety_provided is null
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and gun_safety_provided is null
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and gun_safety_provided is null
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and lower(gun_safety_provided) != 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'not asked'
              and gun_safety_provided is null
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and lower(gun_safety_provided) != 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and lower(gun_safety_provided) = 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and lower(gun_safety_provided) = 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and gun_safely_stored is null
              and gun_safety_provided is null
            then 1
          else 0
      end as gun_present_ind,
      --Gun Safely Stored Indicator
      case
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = '_Already Safe'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Declined'
            then 1
          when gun_present = '1'
              and gun_safely_stored is null
              and gun_safety_provided = 'Already safe'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education, No gun reported'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'yes'
              and gun_safety_provided is null
              then 1
          else 0
      end as gun_stored_safely_ind,
      --Offered Safey Education/Device Indicator
      case
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Declined'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Gun Lock'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and lower(gun_safety_provided) != 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and lower(gun_safety_provided) != 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and lower(gun_safety_provided) = 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and lower(gun_safety_provided) = 'declined'
            then 1
          when
              lower(gun_safety_provided) like ('%education%')
              or
              lower(gun_safety_provided) like ('%gun lock%')
              or
              lower(gun_safety_provided) like ('%lock box%')
            then 1
          else 0
      end as gun_offered_education_device_ind,
      --Education/Device Provided Indicator
      case
          when gun_present = '0'
              and gun_safely_stored is null
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Education, No gun reported'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Gun Lock, Lock Box'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Education'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'No'
              and gun_safety_provided = 'Gun Lock'
            then 1
          when gun_present = 'Yes'
              and gun_safely_stored = 'Yes'
              and gun_safety_provided = 'Gun Lock'
            then 1
          when gun_present = 'Not asked'
              and gun_safely_stored = 'Not asked'
              and gun_safety_provided = 'Education, No gun reported'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'no'
              and lower(gun_safety_provided) != 'declined'
            then 1
          when
              stg_ed_encounter_cohort_all.arrive_ed_dt >= '05-22-23'
              and lower(gun_present) = 'yes'
              and lower(gun_safely_stored) = 'unsure'
              and lower(gun_safety_provided) != 'declined'
            then 1
          when
              lower(gun_safety_provided) like ('%education%')
              or
              lower(gun_safety_provided) like ('%gun lock%')
              or
              lower(gun_safety_provided) like ('%lock box%')
            then 1
          else 0
      end as gun_education_device_provided_ind,
      -- Education Received Indicator
      case
          when
              lower(gun_safety_provided) like '%education%'
            then 1
          else 0
      end as gun_education_received_ind,
      -- Gun Lock Indicator
      case
          when
              lower(gun_safety_provided) like '%gun lock%'
            then 1
          else 0
      end as gun_lock_received_ind,
      -- Lock Box Indicator
      case
          when
              lower(gun_safety_provided) like '%lock box%'
            then 1
          else 0
      end as gun_lock_box_received_ind,
      -- Subsequent Gun Status
      case
          when gun_present_ind = 1
              and gun_stored_safely_ind = 0
            then 'unsafe'
          when gun_present_ind = 1
              and gun_stored_safely_ind = 1
            then 'safe'
          when  gun_present_ind = 0
                and gun_stored_safely_ind = 0
            then 'no gun'
        end as current_gun_status,
      lag(current_gun_status) over(
        partition by
            stg_ed_encounter_cohort_all.pat_key
        order by
            stg_ed_encounter_cohort_all.arrive_ed_dt,
            ed_encounter_metric_smart_data_element.visit_key
      ) as last_gun_status,
      case
          when current_gun_status = 'unsafe'
              and last_gun_status in ('safe', 'no gun')
            then 1
          else 0
      end as subsequent_safely_stored_gun_ind,
      case
          when current_gun_status in (
                'safe',
                'no gun'
              )
              and last_gun_status = 'unsafe'
            then 1
          else 0
      end as subsequent_unsafely_stored_gun_ind
    from
      {{ref('ed_encounter_metric_smart_data_element')}} as ed_encounter_metric_smart_data_element
      inner join {{ref('stg_ed_encounter_cohort_all')}} as stg_ed_encounter_cohort_all
        on ed_encounter_metric_smart_data_element.visit_key = stg_ed_encounter_cohort_all.visit_key
    group by
      ed_encounter_metric_smart_data_element.visit_key,
      stg_ed_encounter_cohort_all.pat_key,
      stg_ed_encounter_cohort_all.arrive_ed_dt,
      ed_encounter_metric_smart_data_element.firearm_present_first,
      ed_encounter_metric_smart_data_element.firearm_safely_stored_first,
      ed_encounter_metric_smart_data_element.firearm_interventions_first,
      ed_encounter_metric_smart_data_element.firearm_location_first,
      ed_encounter_metric_smart_data_element.firearm_loc_home_first,
      ed_encounter_metric_smart_data_element.firearm_loc_friend_relative_first,
      ed_encounter_metric_smart_data_element.firearm_loc_car_first,
      ed_encounter_metric_smart_data_element.firearm_loc_other_first,
      ed_encounter_metric_smart_data_element.firearm_addl_info_first
)

select
    firearms_indicators.visit_key,
    firearms_indicators.firearm_screen_completed_ind,
    firearms_indicators.gun_present_ind,
    firearms_indicators.gun_stored_safely_ind,
    firearms_indicators.gun_offered_education_device_ind,
    firearms_indicators.gun_education_device_provided_ind,
    firearms_indicators.gun_education_received_ind,
    firearms_indicators.gun_lock_received_ind,
    firearms_indicators.gun_lock_box_received_ind,
    firearms_indicators.current_gun_status,
    firearms_indicators.last_gun_status,
    firearms_indicators.subsequent_safely_stored_gun_ind,
    firearms_indicators.subsequent_unsafely_stored_gun_ind
from
  firearms_indicators
