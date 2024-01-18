with base_pre as (
    select
        stg_medication_orders.pat_key,
        stg_medication_orders.visit_key,
        stg_medication_orders.csn,
        stg_medication_orders.patient_name,
        stg_medication_orders.mrn,
        stg_medication_orders.hospital_admit_date,
        stg_medication_orders.hospital_discharge_date,
        stg_medication_orders.order_dose_unit,
        stg_medication_orders.med,
        medication_administration.action_dt,
        max(cast(medication_administration.dose as numeric)) as ma_dose
    from {{ ref('stg_vasoactive_inotropic_score_medication_orders') }}
    as stg_medication_orders
    inner join
        {{ source ('cdw','medication_administration') }}
        as medication_administration on
            stg_medication_orders.med_ord_key
            = medication_administration.med_ord_key
    left join
        {{ source('cdw','dim_medication_administration_result') }}
        as dim_medication_administration_result on
            medication_administration.dim_med_admin_rslt_key
            = dim_medication_administration_result.dim_med_admin_rslt_key
    --check.. (to get rid of mar hold ype ones)
    where medication_administration.dose is not null
        and dim_medication_administration_result.med_admin_rslt_nm
        != 'MISSED'
        and dim_medication_administration_result.med_admin_rslt_nm
        != 'CANCELED ENTRY'
        and stg_medication_orders.medication_frequency = 'CONTINUOUS'
    group by
        stg_medication_orders.pat_key,
        stg_medication_orders.visit_key,
        stg_medication_orders.csn,
        stg_medication_orders.patient_name,
        stg_medication_orders.mrn,
        stg_medication_orders.hospital_admit_date,
        stg_medication_orders.hospital_discharge_date,
        stg_medication_orders.order_dose_unit,
        stg_medication_orders.med,
        medication_administration.action_dt
),

base as (
    select
        base_pre.*,
        case when base_pre.med = 'DOPAMINE' then base_pre.ma_dose
            when base_pre.med = 'DOBUTAMINE' then base_pre.ma_dose
            when base_pre.med in ('EPINEPHRINE', 'NOREPINEPHRINE')
                then 100 * base_pre.ma_dose
            when base_pre.med = 'MILRINONE' then 10 * base_pre.ma_dose
            when base_pre.med = 'VASOPRESSIN'
                 and base_pre.order_dose_unit = 'milli-units/kg/hr'
                 then base_pre.ma_dose / 6
            when base_pre.med = 'VASOPRESSIN'
                 and base_pre.order_dose_unit != 'milli-units/kg/hr'
                 then base_pre.ma_dose * 10000  -- units/kg/min or units/min
            else -2 end as vis_points,

        case
            when base_pre.med = 'DOPAMINE' then vis_points --noqa
        end as dopamine_vis_points,
        case
            when base_pre.med = 'DOBUTAMINE' then vis_points --noqa
        end as dobutamine_vis_points,
        case when base_pre.med = 'VASOPRESSIN' then vis_points --noqa
        end as epinephrine_vis_points,
        case
            when base_pre.med = 'MILRINONE' then vis_points --noqa
        end as milrinone_vis_points,
        case
            when base_pre.med = 'VASOPRESSIN' then vis_points --noqa
        end as vasopressin_vis_points,
        case
            when base_pre.med = 'NOREPINE' then vis_points --noqa
        end as norepinephrine_vis_points

    from base_pre
),

med_weight_join as (
    select
        base.pat_key,
        base.visit_key,
        base.med,
        base.action_dt,
        base.ma_dose,
        base.order_dose_unit,
        base.vis_points,
        stg_weights.weight_recorded_date,
        stg_weights.weight_kg,
        row_number()
        over (partition by base.pat_key, base.med, base.action_dt
            order by stg_weights.weight_recorded_date desc) as weight_recency_order
    from base
    left join  {{ ref('stg_vasoactive_inotropic_score_weights') }}
    as stg_weights on base.pat_key = stg_weights.pat_key
        and stg_weights.weight_recorded_date < base.action_dt
    where base.order_dose_unit in ('mcg/min', 'Units/min')
),

adjusted_vis_points as (
    select
        med_weight_join.pat_key,
        med_weight_join.visit_key,
        med_weight_join.med,
        med_weight_join.action_dt,
        med_weight_join.order_dose_unit,
        med_weight_join.ma_dose,
        med_weight_join.weight_kg,
        med_weight_join.vis_points / med_weight_join.weight_kg as vis_points_adj
    from med_weight_join
    where med_weight_join.weight_recency_order = 1
)

    select
        base.patient_name,
        base.visit_key,
        base.csn,
        base.mrn,
        base.hospital_admit_date,
        base.hospital_discharge_date,
        base.med,
        base.order_dose_unit,
        base.action_dt as vis_session_start,
        base.ma_dose,
        adjusted_vis_points.vis_points_adj,
        coalesce(
            adjusted_vis_points.vis_points_adj, base.vis_points
        ) as vis_points_final,
        lag(
            vis_points_final
        ) over (
            partition by base.visit_key, base.med order by base.action_dt
        ) as prev_med_vis_points,
        case when prev_med_vis_points is null then vis_points_final
            else vis_points_final - prev_med_vis_points
        end as vis_points_difference,

        case
            when
                vis_points_final != prev_med_vis_points
                or prev_med_vis_points is null then 1
            else 0
        end as rate_change_flag

    from base
    left join adjusted_vis_points on base.visit_key
        = adjusted_vis_points.visit_key
        and base.med = adjusted_vis_points.med
        and base.action_dt = adjusted_vis_points.action_dt
        and base.order_dose_unit = adjusted_vis_points.order_dose_unit
