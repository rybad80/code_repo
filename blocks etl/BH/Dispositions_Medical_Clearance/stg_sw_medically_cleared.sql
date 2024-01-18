with
sw_dispo_placement_status as (
select
    smart_data_element_all.sde_key,
    smart_data_element_all.visit_key,
    smart_data_element_all.entered_date as current_value_entered_date,
    smart_data_element_all.element_value as current_value,
    smart_data_element_history.previous_value_update_date,
    smart_data_element_history.sde_previous_value
from
    {{ref('smart_data_element_all')}} as smart_data_element_all
left join {{ref('smart_data_element_history')}} as smart_data_element_history
on smart_data_element_all.sde_key = smart_data_element_history.sde_key
where
    smart_data_element_all.concept_id = 'CHOPBH#375'
)

select
    sw_dispo_placement_status.visit_key,
    date(min(case
            when sw_dispo_placement_status.sde_previous_value = 'Medically Cleared, Waiting Placement'
            then sw_dispo_placement_status.previous_value_update_date
            when sw_dispo_placement_status.current_value = 'Medically Cleared, Waiting Placement'
            then sw_dispo_placement_status.current_value_entered_date
            end)) as sw_form_first_med_cleared_date,
    min(case
            when sw_dispo_placement_status.sde_previous_value = 'Patient is Discharged/Complete'
            then sw_dispo_placement_status.previous_value_update_date
            when sw_dispo_placement_status.current_value = 'Patient is Discharged/Complete'
            then sw_dispo_placement_status.current_value_entered_date
            end) as sw_form_first_discharge_complete
from
    sw_dispo_placement_status
group by
    sw_dispo_placement_status.visit_key
