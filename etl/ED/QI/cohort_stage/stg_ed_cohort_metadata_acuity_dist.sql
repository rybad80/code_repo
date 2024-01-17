with lookback_period as (
    select
      dim_date.date_key,
      dim_date.full_date
    from
      {{ref('dim_date')}} as dim_date
    where
      dim_date.fiscal_year between
        (year(current_date + interval '6 months') - 2)
        and (year(current_date + interval '6 months') - 1)
),

transformed_data as (
    select
      ed_encounter_cohort_long.visit_key,
      ed_encounter_cohort_long.cohort,
      stg_encounter_ed.ed_arrival_date,
      stg_encounter_ed.acuity_esi,
      case
          when lower(btrim(stg_encounter_ed.acuity_esi)) in (
            '1 critical',
            'sort 1',
            'emergency level 1'
          )
            then 'acuity_one'
          when lower(btrim(stg_encounter_ed.acuity_esi)) in (
            '2 acute',
            'sort 2',
            'emergency level 2'
          )
            then 'acuity_two'
          when lower(btrim(stg_encounter_ed.acuity_esi)) in (
            '3 urgent',
            'sort 3',
            'emergency level 3'
          )
            then 'acuity_three'
          when lower(btrim(stg_encounter_ed.acuity_esi)) in (
            '4 urgent',
            'sort 4',
            'emergency level 4'
          )
            then 'acuity_four'
          when lower(btrim(stg_encounter_ed.acuity_esi)) in (
            '5 non-urgent',
            'sort 5',
            'emergency level 5'
          )
            then 'acuity_five'
      end as pivot_acuity
    from
      {{ref('ed_encounter_cohort_long')}} as ed_encounter_cohort_long
      inner join {{ref('stg_encounter_ed')}} as stg_encounter_ed
        on ed_encounter_cohort_long.visit_key = stg_encounter_ed.visit_key
      inner join lookback_period as lookback_period
        on cast(stg_encounter_ed.ed_arrival_date as date) = lookback_period.full_date
    where
      pivot_acuity is not null
)

select
  transformed_data.cohort,
  count(distinct transformed_data.visit_key) as total_count,
  count(
    distinct
    case
      when lower(transformed_data.pivot_acuity) = 'acuity_one'
        then transformed_data.visit_key
    end
   ) as acuity_one_count,
  count(
    distinct
    case
      when lower(transformed_data.pivot_acuity) = 'acuity_two'
        then transformed_data.visit_key
    end
   ) as acuity_two_count,
  count(
    distinct
    case
      when lower(transformed_data.pivot_acuity) = 'acuity_three'
        then transformed_data.visit_key
    end
   ) as acuity_three_count,
  count(
    distinct
    case
      when lower(transformed_data.pivot_acuity) = 'acuity_four'
        then transformed_data.visit_key
    end
   ) as acuity_four_count,
  count(
    distinct
    case
      when lower(transformed_data.pivot_acuity) = 'acuity_five'
        then transformed_data.visit_key
    end
   ) as acuity_five_count,
   ( cast(acuity_one_count as float) / cast(total_count as float) ) as acuity_one_dist,
   ( cast(acuity_two_count as float) / cast(total_count as float) ) as acuity_two_dist,
   ( cast(acuity_three_count as float) / cast(total_count as float) ) as acuity_three_dist,
   ( cast(acuity_four_count as float) / cast(total_count as float) ) as acuity_four_dist,
   ( cast(acuity_five_count as float) / cast(total_count as float) ) as acuity_five_dist
from
  transformed_data as transformed_data
group by
    transformed_data.cohort
