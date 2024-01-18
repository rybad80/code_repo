with meds_cohort as (
    select distinct
       stg_medication_orders.pat_key,
       stg_medication_orders.visit_key
    from {{ ref('stg_vasoactive_inotropic_score_medication_orders') }}
    as stg_medication_orders
)
      select
        meds_cohort.pat_key,
        meds_cohort.visit_key,
        flowsheet_all.recorded_date as weight_recorded_date,
        flowsheet_all.meas_val / 35.274 as weight_kg
    from meds_cohort
    inner join {{ ref('flowsheet_all') }}
        as flowsheet_all on meds_cohort.visit_key = flowsheet_all.visit_key
            --weight and dosing weight
            and flowsheet_all.flowsheet_id in (14, 40022107)
            and flowsheet_all.meas_val is not null
