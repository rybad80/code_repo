--find prescriber's intended medication duration
select
    stg_asp_outpatient_sig.visit_key,
    stg_asp_outpatient_sig.med_ord_key,
    stg_asp_outpatient_sig.medication_order_id,
    stg_asp_outpatient_sig.abx_name,
    stg_asp_outpatient_sig.drug_category,
    stg_asp_outpatient_sig.drug_class,
    stg_asp_outpatient_sig.drug_subclass,
    stg_asp_outpatient_sig.sig_text_lower,
    days_between(
        order_med_3.end_dt_bef_fill_dt, --original end date by provider
        stg_asp_outpatient_sig.medication_start_date
    ) as num_days_prescribed,
    --how many doses were prescribed before overwrite?
    coalesce(
        stg_asp_outpatient_sig.days_through_end, --last day intended
        --Use sig dose first if known
        stg_asp_outpatient_sig.sig_num
            + coalesce(
                stg_asp_outpatient_sig.sig_then_num,
                0
            ),
        --if sig_num is null but sig_then_num is not, it's because provider uses natural language
        --assume today, first day, etc.; so only 1
        case when sig_then_num is not null
            then 1 + stg_asp_outpatient_sig.sig_then_num end,
        --Then calculate using dispense quantity and dose amount 
        --Cannot have partial doses, so rounded down
        floor(
            regexp_extract(quantity, '\b\d+(\.\d+)?\b')::numeric --convert varchar number to integer
                / coalesce(
                    nullif(stg_asp_outpatient_sig.sig_dose_num, 0),
                    nullif(order_medinfo.admin_min_dose, 0),
                    1
                )
        )
    ) as quantity_prescribed,
    --How often is the medication taken?
    case
        --Use duration data if known
        when stg_asp_outpatient_sig.sig_unit = 'day'
            or stg_asp_outpatient_sig.sig_then_unit = 'day' --newest
            or days_through_end is not null
            then 1
        --ignore incorrect quantities
        --dose unit does not match quantity unit; ex: 1 bottle/inhaler
        when quantity_prescribed = 0
            then null
        --Then use frequency data
        when lookup_medication_frequency.days_elapsed_between is not null
            then lookup_medication_frequency.days_elapsed_between
        --Then use doses and prescribed duration
        --End date is EOD at 2359, so we round up to calculate the frequency
        --Read: If a*(1/b) < c, then a/c < b
        --a is doses, c is days prescribed
        --(1/b) is dose frequency, so b is time between doses
        else 1.0 / ceil(quantity_prescribed / nullif(num_days_prescribed, 0))
        end as days_between_doses,
    --How many days elapse between all doses?
    round(quantity_prescribed * days_between_doses, 2) as prescribed_duration_days,
    --How many days of medication were provided?
    round(
        minutes_between(
            stg_asp_outpatient_sig.medication_start_date,
            stg_asp_outpatient_sig.medication_end_date
        ) / 24.0 / 60.0, 2
    ) as pharmacy_duration_days,
    coalesce(
        prescribed_duration_days,
        num_days_prescribed,
        pharmacy_duration_days,
        0
    ) as outpatient_duration_days
from
    {{ ref('stg_asp_outpatient_sig') }} as stg_asp_outpatient_sig
    left join {{ source('clarity_ods', 'order_med') }} as order_med
        on stg_asp_outpatient_sig.medication_order_id = order_med.order_med_id
    left join {{ source('clarity_ods', 'order_medinfo')}} as order_medinfo
        on stg_asp_outpatient_sig.medication_order_id = order_medinfo.order_med_id
    left join {{ source('clarity_ods', 'order_med_3') }} as order_med_3
        on stg_asp_outpatient_sig.medication_order_id = order_med_3.order_id
    left join {{ ref('lookup_medication_frequency') }} as lookup_medication_frequency
        on order_med.hv_discr_freq_id = lookup_medication_frequency.freq_id
