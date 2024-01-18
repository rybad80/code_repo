with
    med_cleared_dates_old_sde as (
 select
    encounter_all.visit_key,
    max(date(date('1840-12-31')
        + cast(smart_data_element_all.element_value as integer) * interval '1 day'))
        as med_cleared_date_old_sde
from
    {{ ref('encounter_all') }} as encounter_all
    inner join {{ ref('smart_data_element_all') }} as smart_data_element_all
        on smart_data_element_all.visit_key = encounter_all.visit_key
where
    smart_data_element_all.concept_id = 'CHOP#6282'
    and (encounter_all.ed_ind = 1 or encounter_all.inpatient_ind = 1)
group by
    encounter_all.visit_key
),

med_cleared_combined as (
select
    case when
    med_cleared_dates_old_sde.med_cleared_date_old_sde is not null
    then med_cleared_dates_old_sde.visit_key
    when stg_sw_medically_cleared.sw_form_first_med_cleared_date is not null
    then stg_sw_medically_cleared.visit_key
    when bh_dispositions_orders.last_mc_status is not null
    then bh_dispositions_orders.visit_key end
    as visit_key,
    med_cleared_dates_old_sde.med_cleared_date_old_sde,
    stg_sw_medically_cleared.sw_form_first_med_cleared_date,
    stg_sw_medically_cleared.sw_form_first_discharge_complete,
    bh_dispositions_orders.first_mc_yes_order_date,
    bh_dispositions_orders.last_mc_yes_order_date,
    bh_dispositions_orders.last_mc_status,
    bh_dispositions_orders.last_mc_status_date,
    bh_dispositions_orders.mc_expected_time_frame
from
    med_cleared_dates_old_sde
    full join
        {{ref('stg_sw_medically_cleared')}} as stg_sw_medically_cleared
        on stg_sw_medically_cleared.visit_key = med_cleared_dates_old_sde.visit_key
    full join
        {{ref('bh_dispositions_orders')}} as bh_dispositions_orders
        on bh_dispositions_orders.visit_key = med_cleared_dates_old_sde.visit_key
),

    med_cleared_combined_summary as (
select
    visit_key,
    min(sw_form_first_discharge_complete) as sw_form_first_discharge_complete,
    min(med_cleared_date_old_sde) as old_sde_mc_date,
    min(sw_form_first_med_cleared_date) as sw_form_mc_date_first,
    min(first_mc_yes_order_date) as order_mc_yes_date_time_first,
    min(last_mc_yes_order_date) as order_mc_yes_date_time_last,
    min(last_mc_status) as order_mc_status_last,
    min(last_mc_status_date) as order_mc_status_last_date,
    min(mc_expected_time_frame) as order_mc_expected_time_frame
from
    med_cleared_combined
group by
    visit_key
),

earliest_med_cleared_dates as (
select
    visit_key,
    least(
    coalesce(
    old_sde_mc_date,
    sw_form_mc_date_first,
    date(order_mc_yes_date_time_first)),
    coalesce(
    sw_form_mc_date_first,
    old_sde_mc_date,
    date(order_mc_yes_date_time_first)),
    coalesce(
    date(order_mc_yes_date_time_first),
    sw_form_mc_date_first,
    old_sde_mc_date)
    ) as mc_date_earliest
from med_cleared_combined_summary
group by
    visit_key,
    old_sde_mc_date,
    sw_form_mc_date_first,
    order_mc_yes_date_time_first
)

select
    med_cleared_combined_summary.visit_key,
    med_cleared_combined_summary.old_sde_mc_date,
    med_cleared_combined_summary.sw_form_mc_date_first,
    med_cleared_combined_summary.order_mc_yes_date_time_first,
    med_cleared_combined_summary.order_mc_yes_date_time_last,
    med_cleared_combined_summary.order_mc_status_last,
    med_cleared_combined_summary.order_mc_status_last_date,
    med_cleared_combined_summary.order_mc_expected_time_frame,
    med_cleared_combined_summary.sw_form_first_discharge_complete,
    earliest_med_cleared_dates.mc_date_earliest
from
    med_cleared_combined_summary
    inner join earliest_med_cleared_dates on
    earliest_med_cleared_dates.visit_key = med_cleared_combined_summary.visit_key
