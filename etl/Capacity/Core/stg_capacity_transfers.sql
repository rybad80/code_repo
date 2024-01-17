with adt_stage as (
    select
        visit_key,
        visit_event_key,
        dept_key,
        pat_key,
        lead(visit_event_key, 1, null) over(
            partition by visit_key order by enter_date
        ) as next_visit_event_key,
        csn,
        mrn,
        patient_name,
        dob,
        enter_date,
        exit_date,
        exit_date_or_current_date,
        initial_service,
        department_name,
        department_group_name,
        bed_care_group,
        intended_use_group,
        -- part 1 of transfer_type, simplify intended use
        case
            when lower(intended_use_group) in ('ed', 'edecu-obs')
            then 'ED'
            when lower(intended_use_group) in ('cardiac obs', 'periop')
            then 'OR'
            else 'IP'
        end as intended_use_group_simplified,
        department_center_id,
        department_center_abbr,
        -- use to determine if intercampus and part 2 of transfer_type
        -- department_center will not suffice
        case
            when department_center_id in (101, 105)
            then 'KOPH'
            when department_center_id in (66, 104)
            then 'PHL'
        end as facility
    from
        {{ref('adt_department')}}
    where
        -- remove Main Transport and KOPH Virtual Units/Patient Placement
        intended_use_group is not null
        -- KOPH and PHL Campus visits only
        and department_center_id in (66, 101, 104, 105)
        and exit_date_or_current_date >= date('2017-07-01')
        and enter_date >= date('2010-01-01') -- old visits missing exit_date
),

last_service as (
    select
        adt_stage.visit_event_key,
        adt_service.service,
        row_number() over (
            partition by adt_stage.visit_event_key
            order by adt_service.service_start_datetime desc
        ) as service_number_desc
    from
        adt_stage
        left join {{ref('adt_service')}} as adt_service
            on adt_service.visit_key = adt_stage.visit_key
            -- new service other than initial service during department stay
            and adt_service.service_start_datetime > adt_stage.enter_date
            and adt_service.service_start_datetime < adt_stage.exit_date_or_current_date
            -- small amount of documentation errors
            and adt_service.service_los_hrs >= 0.5
    where
        adt_service.visit_key is not null
)

select
    adt_stage.visit_key,
    adt_stage.visit_event_key,
    adt_stage.pat_key,
    adt_stage.csn,
    adt_stage.patient_name,
    adt_stage.dob,
    adt_stage.mrn,
    adt_stage.enter_date,
    adt_stage.exit_date,
    adt_stage.exit_date_or_current_date,
    adt_stage.initial_service,
    coalesce(
        last_service.service,
        adt_stage.initial_service
    ) as last_service,
    adt_stage.dept_key,
    adt_stage.department_name,
    adt_stage.department_group_name,
    adt_stage.bed_care_group,
    adt_stage.intended_use_group,
    adt_stage.intended_use_group_simplified,
    adt_stage.department_center_id,
    adt_stage.department_center_abbr,
    adt_stage.facility,
    next_department.visit_event_key as next_visit_event_key,
    next_department.initial_service as next_service,
    next_department.dept_key as next_dept_key,
    next_department.department_name as next_department,
    next_department.department_group_name as next_department_group,
    next_department.bed_care_group as next_bed_care_group,
    next_department.intended_use_group as next_intended_use_group,
    next_department.intended_use_group_simplified as next_intended_use_group_simplified,
    next_department.department_center_id as next_department_center_id,
    next_department.department_center_abbr as next_department_center_abbr,
    next_department.facility as next_facility
from
    adt_stage
    -- inner join removes Discharges 
    inner join adt_stage as next_department
        on next_department.visit_event_key = adt_stage.next_visit_event_key
    left join last_service
        on last_service.visit_event_key = adt_stage.visit_event_key
        and service_number_desc = 1
