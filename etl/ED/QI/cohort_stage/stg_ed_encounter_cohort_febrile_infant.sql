with base_ed_visits as (
-- region Applying encounter-level criteria and pulling basic information about the ED visits 
    select
      stg_encounter_ed.visit_key,
      stg_encounter_ed.pat_key,
      days_between(
        stg_encounter_ed.dob::date,
        stg_encounter_ed.ed_arrival_date::date
      ) as calc_ed_age_days,
      stg_encounter_ed.ed_discharge_date
    from
      {{ ref('stg_encounter_ed') }} as stg_encounter_ed
    where
      year(stg_encounter_ed.encounter_date) >= 2017
      and calc_ed_age_days <= 56.0
      and stg_encounter_ed.ed_patients_seen_ind = 1
-- end region
),


ed_fever_vitals as (
-- region Pulling when a fever of 100.4+ was recorded within the ED
    select
        base_ed_visits.visit_key,
        1 as fever_recorded_in_ed_ind
    from
        base_ed_visits
        inner join {{ ref('flowsheet_all') }} as flowsheet_all
          on base_ed_visits.visit_key = flowsheet_all.visit_key
    where
        flowsheet_id = 6 -- Temp
        and flowsheet_all.meas_val_num >= 100.4
        and flowsheet_all.recorded_date <= base_ed_visits.ed_discharge_date
    group by
      base_ed_visits.visit_key
-- end region
),

fever_complaint as (
-- region Pulling indicators based on Visit Reasons/Complaints that include fever
    select
        base_ed_visits.visit_key,
        max(case
              when visit_reason.seq_num = 1 -- Is Chief/Primary
                then 1
              else 0
          end) as fever_chief_complaint_ind,
        1 as fever_complaint_ind
    from
        base_ed_visits
      inner join {{ source('cdw', 'visit_reason') }} as visit_reason on
          base_ed_visits.visit_key = visit_reason.visit_key
      inner join {{ source('cdw', 'master_reason_for_visit') }} as master_reason_for_visit
          on visit_reason.rsn_key = master_reason_for_visit.rsn_key
    where
      lower(master_reason_for_visit.rsn_nm) like '%fever%'
    group by
        base_ed_visits.visit_key
-- end region
),

inclusion_labs as (
-- region Pulling labs/indicators for labs required for cohort inclusion
    select
      base_ed_visits.visit_key,
      1 as inclusion_lab_ind,
      max(case
              when procedure_order_clinical.procedure_id = 6466 -- blood culture
                then 1
              else 0
          end) as blood_culture_ind,
      max(case
              when procedure_order_clinical.procedure_id in (
                    127657, -- enhanced ua profile
                    6478    -- urine culture
                  )
                then 1
              else 0
          end) as urine_culture_ind,
      max(case
              when procedure_order_clinical.procedure_id = 15598
                then 1
              else 0
          end) as csf_culture_ind
    from
      base_ed_visits
      inner join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on base_ed_visits.visit_key = procedure_order_clinical.visit_key
    where
      (
        procedure_order_clinical.procedure_id in (
           6466,   -- blood culture
           127657, -- enhanced ua profile
           6478    -- urine culture
        )
        or (
          procedure_order_clinical.procedure_id = 15598   -- sterile body fluid culture and gram stain
          and lower(procedure_order_clinical.order_specimen_source) in (
            'cerebrospinal fluid'
          )
        )
      )
      and procedure_order_clinical.placed_date <= base_ed_visits.ed_discharge_date
      and lower(procedure_order_clinical.order_status) != 'canceled'
    group by
      base_ed_visits.visit_key
-- end region
)

select
  base_ed_visits.visit_key,
  base_ed_visits.pat_key,
  'FEBRILE_INFANT' as cohort,
  case
      when base_ed_visits.calc_ed_age_days <= 21.0
        then '0-21 Days Old'
      when base_ed_visits.calc_ed_age_days < 29.0
        then '22-28 Days Old'
      when base_ed_visits.calc_ed_age_days <= 56.0
        then '29-56 Days Old'
      else 'Unknown'
  end as subcohort
from
  base_ed_visits
  inner join inclusion_labs on base_ed_visits.visit_key = inclusion_labs.visit_key
  left join ed_fever_vitals on base_ed_visits.visit_key = ed_fever_vitals.visit_key
  left join fever_complaint on base_ed_visits.visit_key = fever_complaint.visit_key
where
  coalesce(ed_fever_vitals.fever_recorded_in_ed_ind, fever_complaint.fever_complaint_ind) is not null
