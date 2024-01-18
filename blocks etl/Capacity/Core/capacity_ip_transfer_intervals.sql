with dept as (
    select
        stg_capacity_transfers.visit_key,
        stg_capacity_transfers.visit_event_key,
        stg_capacity_transfers.pat_key,
        stg_capacity_transfers.csn,
        stg_capacity_transfers.dob,
        stg_capacity_transfers.patient_name,
        stg_capacity_transfers.mrn,
        stg_capacity_transfers.enter_date,
        stg_capacity_transfers.exit_date,
        stg_capacity_transfers.exit_date_or_current_date,
        stg_capacity_transfers.initial_service,
        stg_capacity_transfers.last_service,
        stg_capacity_transfers.dept_key,
        stg_capacity_transfers.department_name,
        stg_capacity_transfers.department_group_name,
        stg_capacity_transfers.bed_care_group as bed_care_care_group_name,
        stg_capacity_transfers.intended_use_group,
        stg_capacity_transfers.department_center_id,
        stg_capacity_transfers.department_center_abbr,
        stg_capacity_transfers.next_visit_event_key,
        stg_capacity_transfers.next_service,
        stg_capacity_transfers.next_dept_key,
        stg_capacity_transfers.next_department as next_department_name,
        stg_capacity_transfers.next_department_group as next_department_group_name,
        stg_capacity_transfers.next_bed_care_group as next_bed_care_group,
        stg_capacity_transfers.next_intended_use_group,
        stg_capacity_transfers.next_department_center_id,
        stg_capacity_transfers.next_department_center_abbr
    from
        {{ref('capacity_ip_census_cohort')}} as capacity_ip_census_cohort
        inner join {{ref('stg_capacity_transfers')}} as stg_capacity_transfers
            on stg_capacity_transfers.visit_key = capacity_ip_census_cohort.visit_key
    where
         -- Remove Admissions
        stg_capacity_transfers.enter_date >= capacity_ip_census_cohort.inpatient_census_admit_date
),

mrft_raw as (
select
    adt_department.csn,
    adt_department.visit_event_key,
    stg_capacity_discharge_order.event_action_dt_tm,
    adt_department.enter_date,
    adt_department.exit_date,
    row_number() over (
        partition by adt_department.visit_event_key order by stg_capacity_discharge_order.event_action_dt_tm
    ) as rank_num
from
    {{ref('adt_department')}} as adt_department
    inner join {{ref('stg_capacity_discharge_order')}} as stg_capacity_discharge_order
        on stg_capacity_discharge_order.pat_enc_csn_id = adt_department.csn
where
    --xfer order
    stg_capacity_discharge_order.event_action_nm = 'Xfer Order'
    -- event happened after entering department
    and stg_capacity_discharge_order.event_action_dt_tm >= adt_department.enter_date
    -- event happened before leaving department
    and stg_capacity_discharge_order.event_action_dt_tm < adt_department.exit_date
    -- event happend within 24 hours prior to leaving to department
    -- removes transfer orders where patient was not truly MRFT
    and stg_capacity_discharge_order.event_action_dt_tm >= cast(adt_department.exit_date as date) - 1
),

mrft as (
select
    mrft_raw.csn,
    mrft_raw.visit_event_key,
    mrft_raw.enter_date,
    min(mrft_raw.event_action_dt_tm) as mrft_date,
    case
        when min(mrft_raw.event_action_dt_tm) = max(mrft_raw.event_action_dt_tm)
        then null
        else max(mrft_raw.event_action_dt_tm)
    end as transfer_order_date,
    mrft_raw.exit_date,
    count(distinct mrft_raw.rank_num)  as total_transfer_orders
from
    mrft_raw
group by
    mrft_raw.csn,
    mrft_raw.visit_event_key,
    mrft_raw.enter_date,
    mrft_raw.exit_date
),

bed_assignment as (
    select
        pat_enc_csn_id as csn,
        linked_event_id,
        max(event_action_dt_tm) as event_max,
        max(case when event_action_nm = 'Unit Assign' then event_action_dt_tm end) as unit_assigned_date,
        max(case when event_action_nm = 'Bed Assign' then event_action_dt_tm end) as bed_assigned_date
    from
        {{ref('stg_capacity_pending_action')}}
    where
        event_action_nm in ('Bed Assign', 'Unit Assign')
    group by
        csn,
        linked_event_id
),

bed_assignment_one_row as (
    select
        mrft.visit_event_key,
        max(bed_assignment.unit_assigned_date) as unit_assigned_date,
        max(bed_assignment.bed_assigned_date) as bed_assigned_date
    from
        bed_assignment
        inner join mrft on mrft.csn = bed_assignment.csn
            and bed_assignment.event_max >= mrft.mrft_date
            and bed_assignment.event_max <= mrft.exit_date
    group by
        mrft.visit_event_key
)

select
    dept.visit_key,
    dept.visit_event_key,
    dept.next_visit_event_key,
    dept.pat_key,
    dept.mrn,
    dept.csn,
    dept.patient_name,
    dept.dob,
    dept.dept_key,
    dept.initial_service,
    dept.last_service,
    dept.department_name,
    dept.department_group_name,
    dept.bed_care_care_group_name,
    dept.department_center_abbr,
    dept.next_dept_key,
    dept.next_service,
    dept.next_department_name,
    dept.next_department_group_name,
    dept.next_bed_care_group,
    dept.next_department_center_abbr,
    dept.enter_date,
    mrft.mrft_date,
    bed_assignment_one_row.unit_assigned_date,
    bed_assignment_one_row.bed_assigned_date,
    mrft.transfer_order_date,
    dept.exit_date as unit_transfer_date,
    mrft.total_transfer_orders,
    case
        when
            bed_assignment_one_row.unit_assigned_date >= mrft.mrft_date
        then
            extract( --noqa: PRS
                epoch from bed_assignment_one_row.unit_assigned_date - mrft.mrft_date
            ) / 60.0
    end as mrft_to_unit_assigned_mins,
    case
        when
            bed_assignment_one_row.bed_assigned_date >= bed_assignment_one_row.unit_assigned_date
        then
            extract( --noqa: PRS
                epoch from bed_assignment_one_row.bed_assigned_date - bed_assignment_one_row.unit_assigned_date
            ) / 60.0
    end as unit_assigned_to_bed_assigned_mins,
    case
        when
            mrft.transfer_order_date >= bed_assignment_one_row.bed_assigned_date
        then
            extract( --noqa: PRS
                epoch from mrft.transfer_order_date - bed_assignment_one_row.bed_assigned_date
            ) / 60.0
    end as bed_assigned_to_transfer_order_mins,
    case
        when
            mrft.exit_date >= mrft.transfer_order_date
        then
            extract( --noqa: PRS
                epoch from mrft.exit_date - mrft.transfer_order_date
            ) / 60.0
    end as transfer_order_to_unit_transfer_mins,
    case
        when
            mrft.exit_date >= mrft.mrft_date
        then
            extract( --noqa: PRS
                epoch from mrft.exit_date - mrft.mrft_date
            ) / 60.0
    end as mrft_to_unit_transfer_mins,
    case
        when mrft_to_unit_transfer_mins >= 0 and mrft_to_unit_transfer_mins < 240
        then 1
        when mrft_to_unit_transfer_mins >= 0 and mrft_to_unit_transfer_mins >= 240
        then 0
    end as mrft_to_unit_transfer_mins_target_ind

from
    dept
    left join mrft
        on mrft.visit_event_key = dept.visit_event_key
    left join bed_assignment_one_row
        on bed_assignment_one_row.visit_event_key = dept.visit_event_key
