with procedure_raw as (
    --get pre-procedure resulted data from procedure_order_result_clinical for pc02, ph and glucose
    select
        stg_nicu_chnd_timestamps.log_key,
        'pre-procedure' as proc_type,
        case
            when procedure_order_result_clinical.result_component_id in (
                28, 123030071, 123130007) then 'pco2'
            when procedure_order_result_clinical.result_component_id in (
                27, 123030070, 123130006) then 'ph'
            when procedure_order_result_clinical.result_component_id in (
                500272, 123130001, 6, 123030216) then 'glucose'
        end as result_component,
        procedure_order_result_clinical.result_value_numeric as result_value,
        row_number() over (
            partition by stg_nicu_chnd_timestamps.log_key, result_component
            order by procedure_order_result_clinical.specimen_taken_date desc,
                procedure_order_result_clinical.result_date desc
        ) as seq
    from
        {{ ref('stg_nicu_chnd_timestamps') }} as stg_nicu_chnd_timestamps
        inner join {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical
            on stg_nicu_chnd_timestamps.visit_key = procedure_order_result_clinical.visit_key
    where
        stg_nicu_chnd_timestamps.admitted_after_surgery_ind != 1
        and procedure_order_result_clinical.result_component_id in (
            27, 123030070, 123130006, 28, 123030071, 123130007, 500272, 123130001, 6, 123030216
            )
        and procedure_order_result_clinical.specimen_taken_date
        between stg_nicu_chnd_timestamps.pre_lab_window
        and stg_nicu_chnd_timestamps.time_leave
        and procedure_order_result_clinical.result_value_numeric is not null

    union all

    --get post-procedure resulted data from procedure_order_result_clinical for pc02, ph and glucose
    select
        stg_nicu_chnd_timestamps.log_key,
        'post-procedure' as proc_type,
        case
            when procedure_order_result_clinical.result_component_id in (
                28, 123030071, 123130007) then 'pco2'
            when procedure_order_result_clinical.result_component_id in (
                27, 123030070, 123130006) then 'ph'
            when procedure_order_result_clinical.result_component_id in (
                500272, 123130001, 6, 123030216) then 'glucose'
        end as result_component,
        procedure_order_result_clinical.result_value_numeric as result_value,
        row_number() over (
            partition by stg_nicu_chnd_timestamps.log_key, result_component
            order by procedure_order_result_clinical.specimen_taken_date,
                procedure_order_result_clinical.result_date
        ) as seq
    from
        {{ ref('stg_nicu_chnd_timestamps') }} as stg_nicu_chnd_timestamps
        inner join {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical
            on stg_nicu_chnd_timestamps.visit_key = procedure_order_result_clinical.visit_key
    where
        procedure_order_result_clinical.result_component_id in (
            27, 123030070, 123130006, 28, 123030071, 123130007, 500272, 123130001, 6, 123030216
            )
        and procedure_order_result_clinical.specimen_taken_date
        between (stg_nicu_chnd_timestamps.time_return - interval '5 minutes')
        and stg_nicu_chnd_timestamps.post_lab_window
        and procedure_order_result_clinical.result_value_numeric is not null
)

select
    log_key,
    max(case when proc_type = 'pre-procedure' and result_component = 'pco2'
        then result_value end) as pre_pco2_value,
    max(case when proc_type = 'pre-procedure' and result_component = 'ph'
        then result_value end) as pre_ph_value,
    max(case when proc_type = 'pre-procedure' and result_component = 'glucose'
        then result_value end) as pre_glucose_value,
    max(case when proc_type = 'post-procedure' and result_component = 'pco2'
        then result_value end) as post_pco2_value,
    max(case when proc_type = 'post-procedure' and result_component = 'ph'
        then result_value end) as post_ph_value,
    max(case when proc_type = 'post-procedure' and result_component = 'glucose'
        then result_value end) as post_glucose_value
from
    procedure_raw
where
    seq = 1
group by
    log_key
