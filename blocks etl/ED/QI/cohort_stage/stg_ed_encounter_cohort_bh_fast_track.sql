-- Pulling ED BH Fast Track Cohort basic info/requirements
with base_ed_visits as (
    select
      stg_encounter_ed.visit_key,
      stg_encounter_ed.pat_key
    from
      {{ ref('stg_encounter_ed') }} as stg_encounter_ed
      inner join {{source('cdw', 'visit_reason')}} as visit_reason
        on stg_encounter_ed.visit_key = visit_reason.visit_key
      inner join {{source('cdw', 'master_reason_for_visit')}} as master_reason_for_visit
        on visit_reason.rsn_key = master_reason_for_visit.rsn_key
      inner join {{source('cdw', 'visit_ed_area')}} as visit_ed_area
        on stg_encounter_ed.visit_key = visit_ed_area.visit_key
      inner join {{source('cdw', 'ed_area')}} as ed_area
        on visit_ed_area.ed_area_key = ed_area.ed_area_key
    where
      year(stg_encounter_ed.encounter_date) >= year(current_date) - 5
      and stg_encounter_ed.ed_patients_seen_ind = 1
      and stg_encounter_ed.edecu_ind = 0
      and stg_encounter_ed.inpatient_ind = 0
      and stg_encounter_ed.ed_los_hrs <= 6.0
      and master_reason_for_visit.rsn_id in (
        3042, -- Psychiatric Emergencies
        745,  -- Suicide Concerns
        90,   -- Psychiatric Problem
        8601  -- Psychiatric Evaluation
      )
      and ed_area.ed_area_id in (
        24,     -- CHOP ED FLEX TEAM 7
        97,     -- EDECU
        3007001 -- EDECU
      )
    group by
      stg_encounter_ed.visit_key,
      stg_encounter_ed.pat_key
),

fishman_center as (
  select
    base_ed_visits.visit_key,
    max(
      case
          when lower(stg_adt_all.bed_name) = 'edf-hold'
            then 1
          when lower(stg_adt_all.bed_name) = 'edt7-hold'
               and stg_adt_all.encounter_date between '10/28/2022' and '12/05/2022'
            then 1
          else 0
      end
    ) as fishman_ind
  from
    base_ed_visits as base_ed_visits
    inner join {{ ref('stg_adt_all') }} as stg_adt_all on base_ed_visits.visit_key = stg_adt_all.visit_key
  where
    stg_adt_all.bed_ind = 1
  group by
    base_ed_visits.visit_key
)

select
  base_ed_visits.visit_key,
  base_ed_visits.pat_key,
  'BH_FAST_TRACK' as cohort,
  case
    when fishman_center.fishman_ind = 1
      then 'Fishman Center'
    else 'Non-Fishman Center'
  end as subcohort
from
  base_ed_visits as base_ed_visits
  left join fishman_center as fishman_center on base_ed_visits.visit_key = fishman_center.visit_key
