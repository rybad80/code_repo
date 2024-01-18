select
    visit_key,
    visit_event_key,
    department_group_name,
    department_name,
    department_center_abbr,
    unit_transfer_date,
    mrft_to_unit_transfer_mins,
    COALESCE(last_service, 'Other') as service_at_transfer
from
    {{ref('capacity_ip_transfer_intervals')}}
where
    mrft_date is not null
