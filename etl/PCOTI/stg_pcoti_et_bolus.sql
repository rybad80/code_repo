with icu_transfers as (
    select
        stg_pcoti_event_adt_grouped.*,
        stg_pcoti_event_adt_grouped.event_start_date - interval '1 hour' as icu_enter_date_minus_1hr,
        stg_pcoti_event_adt_grouped.event_start_date + interval '1 hour' as icu_enter_date_plus_1hr
    from
        {{ ref('stg_pcoti_event_adt_grouped') }} as stg_pcoti_event_adt_grouped
    where
        stg_pcoti_event_adt_grouped.event_type_abbrev in (
            'LOC_ICU_PICU',
            'LOC_ICU_NICU',
            'LOC_ICU_CICU'
        )
),

xfers_w_historical_weights as (
    select
        icu_transfers.pat_key,
        icu_transfers.visit_key,
        icu_transfers.event_start_date as icu_enter_date,
        icu_transfers.icu_enter_date_minus_1hr,
        icu_transfers.icu_enter_date_plus_1hr,
        icu_transfers.event_type_abbrev,
        flowsheet_vitals.recorded_date as vitals_recorded_date,
        flowsheet_vitals.weight_kg,
        row_number() over (
            partition by icu_transfers.pat_key
            order by flowsheet_vitals.recorded_date desc
        ) as seq
    from
        icu_transfers
        inner join {{ ref('flowsheet_vitals') }} as flowsheet_vitals
            on icu_transfers.pat_key = flowsheet_vitals.pat_key
    where
        flowsheet_vitals.recorded_date <= icu_transfers.icu_enter_date_minus_1hr
        and flowsheet_vitals.weight_kg is not null
),

xfer_with_weight as (
    select
        xfers_w_historical_weights.*
    from
        xfers_w_historical_weights
    where
        xfers_w_historical_weights.seq = 1
),

all_bolus as (
    select
        flowsheet_all.pat_key,
        flowsheet_all.recorded_date as bolus_admin_date,
        sum(flowsheet_all.meas_val_num) as bolus_ml
    from
        {{ ref('flowsheet_all') }} as flowsheet_all
    where
        flowsheet_all.flowsheet_id in (
            40003208,
            45035707,
            45035807,
            40001432,
            40001440,
            40001448,
            40001452,
            40001428,
            400114481
        )
        and flowsheet_all.meas_val_num > 0
        and flowsheet_all.recorded_date >= '2017-01-01'
    group by
        flowsheet_all.pat_key,
        flowsheet_all.recorded_date
),

xfer_with_bolus as (
    select
        xfer_with_weight.pat_key,
        xfer_with_weight.visit_key,
        xfer_with_weight.icu_enter_date,
        xfer_with_weight.event_type_abbrev,
        xfer_with_weight.weight_kg,
        all_bolus.bolus_admin_date,
        all_bolus.bolus_ml,
        all_bolus.bolus_ml / xfer_with_weight.weight_kg as bolus_ml_per_kg
    from
        xfer_with_weight
        inner join all_bolus
            on xfer_with_weight.pat_key = all_bolus.pat_key
    where
        all_bolus.bolus_admin_date >= xfer_with_weight.icu_enter_date_minus_1hr
        and all_bolus.bolus_admin_date <= xfer_with_weight.icu_enter_date_plus_1hr
),

cumulative_bolus as (
    select
        xfer_with_bolus.pat_key,
        xfer_with_bolus.visit_key,
        xfer_with_bolus.icu_enter_date,
        xfer_with_bolus.event_type_abbrev,
        xfer_with_bolus.weight_kg,
        xfer_with_bolus.bolus_admin_date,
        xfer_with_bolus.bolus_ml,
        xfer_with_bolus.bolus_ml_per_kg,
        sum(xfer_with_bolus.bolus_ml_per_kg) over (
            partition by xfer_with_bolus.pat_key, xfer_with_bolus.visit_key, xfer_with_bolus.icu_enter_date
            order by xfer_with_bolus.bolus_admin_date asc rows unbounded preceding
        ) as cumul_bolus_ml_per_kg
    from
        xfer_with_bolus
    group by
        xfer_with_bolus.pat_key,
        xfer_with_bolus.visit_key,
        xfer_with_bolus.icu_enter_date,
        xfer_with_bolus.event_type_abbrev,
        xfer_with_bolus.weight_kg,
        xfer_with_bolus.bolus_admin_date,
        xfer_with_bolus.bolus_ml,
        xfer_with_bolus.bolus_ml_per_kg
),

bolus_over_60 as (
    select
        cumulative_bolus.*,
        row_number() over (
            partition by cumulative_bolus.pat_key, cumulative_bolus.visit_key, cumulative_bolus.icu_enter_date
            order by cumulative_bolus.bolus_admin_date
        ) as record_order
    from
        cumulative_bolus
    where
        cumulative_bolus.cumul_bolus_ml_per_kg >= 60
)

select
    bolus_over_60.*
from
    bolus_over_60
where
    bolus_over_60.record_order = 1
