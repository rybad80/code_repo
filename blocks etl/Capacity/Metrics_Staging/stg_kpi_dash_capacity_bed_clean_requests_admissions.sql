with beds as (
select
    stg_kpi_dash_capacity_bed_clean_requests.visit_key,
    adt_bed.visit_event_key as adt_bed_visit_event_key,
    lead(adt_bed.visit_event_key, 1, null) over(
            partition by adt_bed.visit_key order by enter_date
        ) as next_visit_event_key,
    lead(adt_bed.inpatient_bed_order, 1, null) over(
            partition by adt_bed.visit_key order by enter_date
        ) as next_visit_inpatient_bed_order,
    stg_kpi_dash_capacity_bed_clean_requests.bed_key,
    adt_bed.enter_date,
    adt_bed.exit_date,
    adt_bed.inpatient_bed_order,
    adt_bed.initial_service as admission_service,
    stg_kpi_dash_capacity_bed_clean_requests.department_group_name,
    stg_kpi_dash_capacity_bed_clean_requests.department_name,
    stg_kpi_dash_capacity_bed_clean_requests.clean_request_to_clean_target_ind,
    stg_kpi_dash_capacity_bed_clean_requests.department_center_abbr
from
    {{ref('stg_kpi_dash_capacity_bed_clean_requests')}} as stg_kpi_dash_capacity_bed_clean_requests
    left join {{ref('adt_bed')}} as adt_bed
        on adt_bed.visit_event_key = stg_kpi_dash_capacity_bed_clean_requests.visit_event_key
where
    adt_bed.visit_event_key is not null
)

select
    admission_bed.visit_key,
    admission_bed.bed_key,
    admission_bed.visit_event_key as adt_bed_visit_event_key,
    adt_bed.enter_date,
    adt_bed.initial_service as admission_service,
    admission_bed.department_name,
    admission_bed.department_group_name,
    admission_bed.department_center_abbr,
    admission_bed.clean_request_to_clean_target_ind
from
    beds
    inner join {{ref('stg_kpi_dash_capacity_bed_clean_requests')}} as admission_bed
        on admission_bed.visit_event_key = beds.next_visit_event_key
        and next_visit_inpatient_bed_order = 1
    inner join {{ref('adt_bed')}} as adt_bed
        on adt_bed.visit_event_key = admission_bed.visit_event_key
