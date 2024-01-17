with transfer_order as (
    -- Intercampus Transfers Order clinician fills out prior to transfer
    -- entering reasons transfer is required
    select
        procedure_order_clinical.visit_key,
        procedure_order_clinical.placed_date,
        procedure_order_clinical.procedure_order_id,
        procedure_order_clinical.department_name,
        max(
            case when ord_spec_quest.ord_quest_id = '500500762'
            then cast(ord_spec_quest.ord_quest_resp as varchar(254)) end
        ) as transfer_indication,
        max(
            case when ord_spec_quest.ord_quest_id = '500500766'
            then cast(ord_spec_quest.ord_quest_resp as varchar(254)) end
        ) as transfer_medsurg_reason,
        max(
            case when ord_spec_quest.ord_quest_id = '500500767'
            then cast(ord_spec_quest.ord_quest_resp as varchar(254)) end
        ) as transfer_nonmedical_reason,
        max(
            case when ord_spec_quest.ord_quest_id = '500500771'
            then cast(ord_spec_quest.ord_quest_resp as varchar(254)) end
        ) as transfer_medsurg_other,
        max(
            case when ord_spec_quest.ord_quest_id = '500500769'
            then cast(ord_spec_quest.ord_quest_resp as varchar(254)) end
        ) as ir_procedure,
        max(
            case when ord_spec_quest.ord_quest_id = '132328'
            then cast(ord_spec_quest.ord_quest_resp as varchar(254)) end
        ) as surgical_procedure,
        max(
            case when ord_spec_quest.ord_quest_id = '500500772'
            then zc_pat_service.name end
        ) as transfer_evaluation_service,
        max(
            case when procedure_order_clinical.cpt_code = '500ADT15'
            then 1
            else 0 end
        ) as ed_to_ed_private_vehicle_ind,
        lead(procedure_order_clinical.department_name, 1, null) over(
            partition by procedure_order_clinical.visit_key order by procedure_order_clinical.placed_date
        ) as next_department_name
    from
        {{ref('procedure_order_clinical')}} as procedure_order_clinical
        inner join {{source('clarity_ods','ord_spec_quest')}} as ord_spec_quest
            on ord_spec_quest.order_id = procedure_order_clinical.procedure_order_id
        left join {{ source('clarity_ods', 'zc_pat_service') }} as zc_pat_service
            on zc_pat_service.hosp_serv_c = ord_spec_quest.ord_quest_resp
    where
        ord_spec_quest.ord_quest_id in
            ('500500762', '500500766', '500500767',
             '500500771', '500500769', '132328', '500500772')
        and procedure_order_clinical.proc_ord_parent_key > 0
    group by
        procedure_order_clinical.visit_key,
        procedure_order_clinical.procedure_order_id,
        procedure_order_clinical.placed_date,
        procedure_order_clinical.department_name
)

select
    stg_capacity_transfers.visit_key,
    stg_capacity_transfers.pat_key,
    stg_capacity_transfers.visit_event_key as sending_visit_event_key,
    stg_capacity_transfers.next_visit_event_key as receiving_visit_event_key,
    stg_capacity_transfers.csn,
    stg_capacity_transfers.mrn,
    stg_capacity_transfers.patient_name,
    stg_capacity_transfers.enter_date as sending_department_enter_date,
    stg_capacity_transfers.exit_date as transfer_date,
    stg_capacity_transfers.department_name as sending_department,
    stg_capacity_transfers.department_group_name as sending_department_group,
    stg_capacity_transfers.bed_care_group as sending_care_level,
    stg_capacity_transfers.department_center_id as sending_department_center_id,
    stg_capacity_transfers.department_center_abbr as sending_department_center_abbr,
    stg_capacity_transfers.facility as sending_facility,
    case
        when
            lower(stg_capacity_transfers.last_service) = 'not applicable'
            and stg_capacity_transfers.department_center_id in (66, 101)
        then
            'Emergency'
        else
            stg_capacity_transfers.last_service
    end as sending_service,
    stg_capacity_transfers.next_department as receiving_department,
    stg_capacity_transfers.next_department_group as receiving_department_group,
    stg_capacity_transfers.next_bed_care_group as receiving_care_level,
    stg_capacity_transfers.next_department_center_id as receiving_department_center_id,
    stg_capacity_transfers.next_department_center_abbr as receiving_department_center_abbr,
    stg_capacity_transfers.next_facility as receiving_facility,
        case
        when
            lower(stg_capacity_transfers.next_service) = 'not applicable'
            and stg_capacity_transfers.next_department_center_id in (66, 101)
        then
            'Emergency'
        else
            stg_capacity_transfers.next_service
    end as receiving_service,
    stg_capacity_transfers.facility || ' '
        || stg_capacity_transfers.intended_use_group_simplified || ' to '
        || stg_capacity_transfers.next_facility || ' '
        || stg_capacity_transfers.next_intended_use_group_simplified as transfer_type,
    stg_transport_all_encounters.transport_key,
    stg_transport_all_encounters.intake_date,
    stg_transport_all_encounters.enroute_date,
    stg_transport_all_encounters.destination_arrival_date as arrival_date,
    stg_transport_all_encounters.transport_complete_canceled_date,
    stg_transport_all_encounters.delay_reason,
    transfer_order.procedure_order_id as transfer_order_id,
    transfer_order.placed_date as transfer_order_date,
    -- region
    -- Primary source: Transfer Order
    -- Backup sourcs: Transport Intake Encounter
    cast(coalesce(
        transfer_order.transfer_indication,
        stg_transport_all_encounters.transfer_indication
    ) as varchar(50)) as transfer_indication,
    cast(coalesce(
        transfer_order.transfer_medsurg_reason,
        transfer_order.transfer_nonmedical_reason,
        stg_transport_all_encounters.transfer_medsurg_reason,
        stg_transport_all_encounters.transfer_nonmedical_reason
    ) as varchar(100)) as transfer_reason,
    -- Combine mutually exclusive columns into one.
    cast(case
        when lower(transfer_reason) = 'ir procedure'
        then coalesce(transfer_order.ir_procedure, stg_transport_all_encounters.transfer_ir_procedure_type)
        when lower(transfer_reason) = 'surgical procedure'
        then coalesce(transfer_order.surgical_procedure, stg_transport_all_encounters.transfer_surgical_procedure)
        when lower(transfer_reason) = 'other'
        then coalesce(transfer_order.transfer_medsurg_other,
                stg_transport_all_encounters.transfer_medsurg_reason_other)
    end as varchar(500)) as transfer_reason_text,
    coalesce(
        transfer_order.transfer_evaluation_service,
        stg_transport_all_encounters.transfer_evaluation_service
    ) as transfer_evaluation_service,
    -- Time from Transfer Order placed to Transport Team arriving to receiving facility
    extract( --noqa: PRS
            epoch from stg_transport_all_encounters.destination_arrival_date - transfer_order.placed_date
        ) / 60.0 as transfer_order_to_transport_arrival_destination_mins,
    stg_transport_all_encounters.intake_to_arrival_mins,
    -- endregion
    -- Was able to link ADT event to Transport Order
    case
        when stg_transport_all_encounters.transport_key is null
        then 0
        else 1
    end as transport_order_ind,
    -- Was able to link ADT event to Transfer Order
    case
        when transfer_order.procedure_order_id is null
        then 0
        else 1
    end as transfer_order_ind,
    transfer_order.ed_to_ed_private_vehicle_ind,
    extract(hour from stg_capacity_transfers.exit_date) as transfer_hour,
    dim_date.weekday_name as transfer_weekday_name,
    cast(date_trunc('week', cast(dim_date.full_date as date) + 1) as date) - 1 as transfer_week_start,
    date_trunc('month', dim_date.full_date) as transfer_month,
    dim_date.fiscal_quarter,
    dim_date.fiscal_year
from
    {{ref('stg_capacity_transfers')}} as stg_capacity_transfers
    inner join {{ref('dim_date')}} as dim_date
       on dim_date.full_date = date_trunc('day', stg_capacity_transfers.exit_date)
    -- visit_keys do not match due to transport intake process
    -- look for non-canceled intercampus order between ADT event
    -- prior to Intercampus transfer
    left join {{ref('stg_transport_all_encounters')}} as stg_transport_all_encounters
        on stg_transport_all_encounters.pat_key = stg_capacity_transfers.pat_key
        and stg_capacity_transfers.exit_date >= stg_transport_all_encounters.intake_date
        and stg_capacity_transfers.exit_date <= coalesce(
                stg_transport_all_encounters.transport_complete_canceled_date,
                current_date)
        and lower(stg_transport_all_encounters.transport_type_raw) = 'intercampus'
        and lower(stg_transport_all_encounters.final_status) in ('completed', 'pending')
    left join transfer_order
        on transfer_order.visit_key = stg_capacity_transfers.visit_key
        and transfer_order.department_name = stg_capacity_transfers.department_name
        and transfer_order.placed_date >= stg_capacity_transfers.enter_date
        and transfer_order.placed_date <= stg_capacity_transfers.exit_date
where
    -- Change in Facility (PHL/KOPH, ED Inclusive)
    stg_capacity_transfers.facility != stg_capacity_transfers.next_facility
    -- Handle duplicate transfer orders
    and (
        -- For visits that go back and forth, should have more than one order
        transfer_order.department_name != transfer_order.next_department_name
        -- Only 1 transfer order
        or transfer_order.next_department_name is null
    )
