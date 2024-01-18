with
    admit_link as (--find admission visit to link to intake visit
        select
			cust_service.comm_id,
			visit.visit_key,
            visit.hosp_admit_dt,
            visit.hosp_dischrg_dt
		from
            {{ source('clarity_ods', 'cust_service') }} as cust_service
			left join
                {{ source('clarity_ods', 'cust_serv_atchment') }} as cust_serv_atchment on
                    cust_service.comm_id = cust_serv_atchment.comm_id
                    and cust_serv_atchment.atchment_pt_csn_id is not null
			left join
                {{ source('cdw', 'visit') }} as visit                                   on
                    cust_serv_atchment.atchment_pt_csn_id = visit.enc_id
		where
            --communication initiated at transfer center or transfer destination
            cust_service.rec_comm_origin_c in (16, 17) --Transfer Center, Transfer Destination
),

    admit_csn as (--link admission visit with intake comm ID
        select distinct
            cust_service.comm_id,
            admit_link.visit_key,
            admit_link.hosp_admit_dt,
            admit_link.hosp_dischrg_dt,
            rank() over(
                partition by cust_service.comm_id order by admit_link.hosp_admit_dt desc, admit_link.visit_key desc
            ) as rnk
        from
            {{ source('clarity_ods', 'cust_service') }} as cust_service
            left  join
                {{ source('clarity_ods', 'cust_serv_atchment') }} as cust_serv_atchment on
                    cust_service.comm_id = cust_serv_atchment.comm_id
                    and cust_serv_atchment.atchment_ncs_id is not null
            inner join
                admit_link                                                            on
                    cust_serv_atchment.atchment_ncs_id = admit_link.comm_id
        where
            --communication initiated at transfer center or transfer destination
            cust_service.rec_comm_origin_c in (16, 17) --Transfer Center, Transfer Destination
),

    complete_date as (--completed/canceled date
        select
            cust_service.comm_id,
            timezone(
                tc_request_status_hx.status_update_utc_dttm, 'utc', 'America/New_York'
            ) as transport_complete_canceled_date,
            rank() over(
                partition by cust_service.comm_id order by  tc_request_status_hx.status_update_utc_dttm desc
            ) as rnk
        from
            {{ source('clarity_ods', 'cust_service') }} as cust_service
            left join
                {{ source('clarity_ods', 'tc_request_status_hx') }} as tc_request_status_hx on
                    tc_request_status_hx.comm_id = cust_service.comm_id
            left join
                {{ source('clarity_ods', 'zc_tc_request_status') }} as zc_tc_request_status on
                    zc_tc_request_status.tc_request_status_c = tc_request_status_hx.request_status_c
        where
            cust_service.rec_comm_origin_c = 16 --Transfer Center
            and zc_tc_request_status.internal_id in (4, 6) --Status is completed or canceled
),

attendant as (--attendants 1 through 4
    select
        visit_form.visit_key,
        max(
            case
                when master_question.quest_id in ('30040000', '30040001')
                then provider.full_nm
                else null
            end
        ) as team_member_a,
        max(
            case
                when master_question.quest_id in ('30042000', '30042001')
                then provider.full_nm
                else null
            end
        ) as team_member_b,
        max(
            case
                when master_question.quest_id in ('30043000', '30043001')
                then provider.full_nm
                else null
            end
        ) as team_member_c,
        max(
            case
                when master_question.quest_id in ('30044000', '30044001')
                then provider.full_nm
                else null
            end
        ) as team_member_d
    from
        {{ source('cdw', 'visit_form') }} as visit_form
        inner join {{ source('cdw', 'master_form') }} as master_form
            on master_form.form_key = visit_form.form_key
        inner join {{ source('cdw', 'master_question') }} as master_question
            on master_question.quest_key = visit_form.quest_key
        inner join {{ source('cdw', 'provider') }} as provider
            on provider.prov_id = visit_form.quest_ansr
    where
        master_form.form_id = 30010000
        and master_question.quest_id in (
            '30040000', --CHOP ED TRANSPORT ATTENDANT #1 - INBOUND 
            '30040001', --CHOP ED TRANSPORT ATTENDANT #1 - OUTBOUND
            '30042000', --CHOP ED TRANSPORT ATTENDANT #2 - INBOUND 
            '30042001', --CHOP ED TRANSPORT ATTENDANT #2 - OUTBOUND
            '30043000', --CHOP ED TRANSPORT ATTENDANT #3 - INBOUND  
            '30043001', --CHOP ED TRANSPORT ATTENDANT #3 - OUTBOUND
            '30044000', --CHOP ED TRANSPORT ATTENDANT #4 - INBOUND
            '30044001'  --CHOP ED TRANSPORT ATTENDANT #4 - OUTBOUND
        )
    group by
        visit_form.visit_key
)

select --noqa: L021
    cust_service.comm_id,
    visit.visit_key as intake_visit_key,
    cast(cust_service.record_entry_time as datetime) as intake_date,
    cust_serv_atchment.atchment_pt_csn_id as intake_csn,
    admit_csn.visit_key as admit_visit_key,
    cast(complete_date.transport_complete_canceled_date as datetime) as transport_complete_canceled_date,
    admit_csn.hosp_admit_dt as hospital_admit_date,
    admit_csn.hosp_dischrg_dt as hospital_discharge_date,
    patient.pat_key,
    patient.pat_mrn_id as mrn,
    patient.full_nm as patient_full_name,
    attendant.team_member_a,
    attendant.team_member_b,
    attendant.team_member_c,
    attendant.team_member_d,
    extract(
        epoch from cust_service.record_entry_time - patient.dob
    ) / (86400.00 * 365.25) as patient_age_at_intake,
    entry_emp.full_nm as entry_user,
    comp_emp.full_nm as comp_user,
    zc_tc_request_status.name as final_status,
    max(
        case
            when zc_tc_cancel_rsn.name is not null
            then cast(zc_tc_cancel_rsn.name as nvarchar(100))
            else 'No cancellation'
        end
    ) as transport_cancel_reason,

    max(
        case
            when zc_tc_cancel_rsn.name is not null
            then 'canceled'
            else cast(zc_ncs_topic.name as nvarchar(75))
        end
    ) as transport_type_raw,

    max(
        case
            when cust_service_transfer.transfer_type_c is not null
            then cast(cust_service_transfer.transfer_type_c as nvarchar(100))
            else null
        end
    ) as transfer_type
from
    {{ source('clarity_ods', 'cust_service') }} as cust_service
    left  join {{ source('clarity_ods', 'cust_serv_atchment') }} as cust_serv_atchment       on
            cust_serv_atchment.comm_id = cust_service.comm_id and cust_serv_atchment.atchment_pt_csn_id is not null
    left  join {{ source('clarity_ods', 'zc_ncs_topic') }} as zc_ncs_topic                   on
            zc_ncs_topic.topic_c = cust_service.topic_c
    inner join {{ source('clarity_ods', 'cust_service_transfer') }} as cust_service_transfer on
            cust_service_transfer.comm_id = cust_service.comm_id
    inner join {{ source('clarity_ods', 'zc_tc_request_status') }} as zc_tc_request_status   on
            zc_tc_request_status.tc_request_status_c = cust_service_transfer.request_status_c
    left  join {{ source('clarity_ods', 'zc_tc_cancel_rsn') }} as zc_tc_cancel_rsn           on
            zc_tc_cancel_rsn.tc_cancel_rsn_c = cust_service_transfer.cancel_status_rsn_c
    left  join {{ source('cdw', 'visit') }} as visit        on visit.enc_id = cust_serv_atchment.atchment_pt_csn_id
    left  join admit_csn                                    on admit_csn.comm_id = cust_service.comm_id
            and admit_csn.rnk = 1
    left  join complete_date                                on complete_date.comm_id = cust_service.comm_id
            and complete_date.rnk = 1
    left  join {{ source('cdw', 'patient') }} as patient    on patient.pat_id = cust_service.subj_member_id
    inner join {{ source('cdw', 'employee') }} as entry_emp on entry_emp.emp_id = cust_service.entry_user_id
    left  join {{ source('cdw', 'employee') }} as comp_emp  on comp_emp.emp_id = cust_service.res_user_id
    left  join attendant                                    on attendant.visit_key = visit.visit_key
where
    cust_service.rec_comm_origin_c = 16 --Origin from the Transfer Center
    and (zc_tc_request_status.internal_id is null
        or zc_tc_request_status.internal_id != 5) --status is not voided

group by
    cust_service.comm_id,
    visit.visit_key,
    cust_service.record_entry_time,
    cust_serv_atchment.atchment_pt_csn_id,
    admit_csn.visit_key,
    complete_date.transport_complete_canceled_date,
    admit_csn.hosp_admit_dt,
    admit_csn.hosp_dischrg_dt,
    patient.pat_key,
    patient.pat_mrn_id,
    patient.full_nm,
    patient.dob,
    entry_emp.full_nm,
    comp_emp.full_nm,
    zc_tc_request_status.name,
    attendant.team_member_a,
    attendant.team_member_b,
    attendant.team_member_c,
    attendant.team_member_d
