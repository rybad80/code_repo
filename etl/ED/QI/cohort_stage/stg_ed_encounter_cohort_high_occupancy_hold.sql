with raw_cohort as (
    select
      stg_encounter_ed.visit_key,
      stg_encounter_ed.pat_key,
      case
          when ed_encounter_metric_procedure_order.high_occupancy_hold is not null
            then 1
          else 0
      end as hoh_orders_ind,
      case
        when fact_edqi.bed_request_to_pt_left_min >= 240.0
          then 1
        else 0
      end as boarder_ind,
      case
          when hoh_orders_ind = 1
            then 1
          when boarder_ind = 1
            then 1
          else 0
      end as cohort_ind,
      case
          when (fact_edqi.bed_request_to_pt_left_min / 60.0) >= 6.0
            then '6+ Hours'
          when (fact_edqi.bed_request_to_pt_left_min / 60.0) >= 5.0
            then '>=5 and <6 Hours'
          when (fact_edqi.bed_request_to_pt_left_min / 60.0) >= 4.0
            then '>=4 and <5 Hours'
          when hoh_orders_ind = 1
            then 'HOH Orders Only'
      end as subcohort
    from
      {{ref('stg_encounter_ed')}} as stg_encounter_ed
      inner join {{ source('cdw_analytics', 'fact_edqi') }} as fact_edqi
        on stg_encounter_ed.visit_key = fact_edqi.visit_key
      left join {{ref('ed_encounter_metric_procedure_order')}} as ed_encounter_metric_procedure_order
        on stg_encounter_ed.visit_key = ed_encounter_metric_procedure_order.visit_key
    where
      cast(stg_encounter_ed.ed_arrival_date as date) >= '20210701'
      and stg_encounter_ed.ed_patients_seen_ind = 1
)

select
  raw_cohort.visit_key,
  raw_cohort.pat_key,
  'HIGH_OCCUPANCY_HOLD' as cohort,
  raw_cohort.subcohort
from
  raw_cohort as raw_cohort
where
  raw_cohort.cohort_ind = 1
