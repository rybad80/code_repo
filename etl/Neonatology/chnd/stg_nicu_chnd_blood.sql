with blood_raw as (
    select
        stg_nicu_chnd_timestamps.log_key,
        case
            when procedure_order_clinical.procedure_name like '%PRBC%' then 'PRBC'
            when procedure_order_clinical.procedure_name like '%CRYO%' then 'CRYO'
            when procedure_order_clinical.procedure_name like '%FFP%' then 'FFP'
            when procedure_order_clinical.procedure_name like '%PLTS%' then 'PLATELETS'
        end as blood_product_category,
        case
            when flowsheet_all.recorded_date between stg_nicu_chnd_timestamps.in_room_date
                and stg_nicu_chnd_timestamps.out_room_date
            then 'intraop'
            when flowsheet_all.recorded_date between stg_nicu_chnd_timestamps.out_room_date
                and (stg_nicu_chnd_timestamps.time_return + interval '6 hours')
            then 'postop'
        end as timing_category,
        flowsheet_all.meas_val_num
    from
        {{ ref('stg_nicu_chnd_timestamps') }} as stg_nicu_chnd_timestamps
        inner join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
            on stg_nicu_chnd_timestamps.pat_key = procedure_order_clinical.pat_key
        inner join {{ source('cdw', 'visit_stay_info') }} as visit_stay_info
            on procedure_order_clinical.visit_key = visit_stay_info.visit_key
        inner join {{ source('cdw', 'visit_stay_info_rows_order') }} as visit_stay_info_rows_order
            on visit_stay_info.vsi_key = visit_stay_info_rows_order.vsi_key
            and procedure_order_clinical.proc_ord_key = visit_stay_info_rows_order.ord_key
        inner join {{ ref('flowsheet_all') }} as flowsheet_all
            on visit_stay_info_rows_order.vsi_key = flowsheet_all.vsi_key
            and visit_stay_info_rows_order.seq_num = flowsheet_all.occurance
        inner join {{ source('clarity_ods', 'ord_blood_admin')}} as ord_blood_admin
            on procedure_order_clinical.procedure_order_id = ord_blood_admin.order_id
    where
        procedure_order_clinical.procedure_order_type = 'Child Order'
        and flowsheet_all.flowsheet_id = 500025331 --Volume given (mL)
        and blood_product_category is not null
        and timing_category is not null

    union all

    --historical method for blood product documentation
    select
        stg_nicu_chnd_timestamps.log_key,
        case
            when flowsheet_all.flowsheet_id = 40001428 then 'PLATELETS'
            when flowsheet_all.flowsheet_id = 40001432 then 'CRYO'
            when flowsheet_all.flowsheet_id = 40001452 then 'PRBC'
            when flowsheet_all.flowsheet_id = 40001440 then 'FFP'
        end as blood_product_category,
        case
            when flowsheet_all.recorded_date between stg_nicu_chnd_timestamps.in_room_date
                and stg_nicu_chnd_timestamps.out_room_date
            then 'intraop'
            when flowsheet_all.recorded_date between stg_nicu_chnd_timestamps.out_room_date
                and (stg_nicu_chnd_timestamps.time_return + interval '6 hours')
            then 'postop'
        end as timing_category,
        flowsheet_all.meas_val_num
    from
        {{ ref('stg_nicu_chnd_timestamps') }} as stg_nicu_chnd_timestamps
        inner join {{ ref('flowsheet_all') }} as flowsheet_all
            on stg_nicu_chnd_timestamps.pat_key = flowsheet_all.pat_key
    where
        blood_product_category is not null
        and timing_category is not null
)

select
    log_key,
    sum(case when timing_category = 'intraop' and blood_product_category = 'PLATELETS'
        then meas_val_num end) as intraop_platelets_volume,
    sum(case when timing_category = 'intraop' and blood_product_category = 'CRYO'
        then meas_val_num end) as intraop_cryo_volume,
    sum(case when timing_category = 'intraop' and blood_product_category = 'PRBC'
        then meas_val_num end) as intraop_prbc_volume,
    sum(case when timing_category = 'intraop' and blood_product_category = 'FFP'
        then meas_val_num end) as intraop_ffp_volume,
    sum(case when timing_category = 'postop' and blood_product_category = 'PLATELETS'
        then meas_val_num end) as postop_platelets_volume,
    sum(case when timing_category = 'postop' and blood_product_category = 'CRYO'
        then meas_val_num end) as postop_cryo_volume,
    sum(case when timing_category = 'postop' and blood_product_category = 'PRBC'
        then meas_val_num end) as postop_prbc_volume,
    sum(case when timing_category = 'postop' and blood_product_category = 'FFP'
        then meas_val_num end) as postop_ffp_volume
from
    blood_raw
group by
    log_key
