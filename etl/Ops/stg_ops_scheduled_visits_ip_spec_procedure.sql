/*
Scheduled elective visits based on the elective admission request procedure order.  These are mutually
exclusive to elective visit based on visit department and do not include visits for surgeries in
the Periop, Cardiac, and KOPH 3 units.
*/

with workqueue_orders as (
    select
        procedure_order.pat_key,
        procedure_order.proc_ord_key,
        max(case when master_question.quest_id = 121345 then order_question.ansr end) as visit_reason,
        visit_appointment.appt_start_dt as scheduled_date,
        department.dept_nm as visit_department_name,
        max(case when master_question.quest_id = 121341 then order_question.ansr end) as service_name,
        procedure_order.proc_ord_nm as scheduled_procedure,
        max(case
            when master_visit_question.quest_id = 121591 then visit_procedure_question.ansr
        end) as scheduled_destination,
        max(case when master_question.quest_id = 121344 then order_question.ansr end) as patient_priority,
        max(case when master_question.quest_id = 121343 then order_question.ansr end) as expected_los_desc,
        max(case when master_question.quest_id = 121347 then order_question.ansr end) as icu_ind,
        visit.visit_key
    from
        {{source('cdw', 'procedure_order')}} as procedure_order
        inner join {{source('cdw', 'procedure_order_appointment')}} as procedure_order_appointment
            on procedure_order_appointment.proc_ord_key = procedure_order.proc_ord_key
        inner join {{source('cdw', 'visit_appointment')}} as visit_appointment
            on visit_appointment.visit_key = procedure_order_appointment.visit_key
        inner join {{source('cdw', 'visit')}} as visit
            on visit.visit_key = visit_appointment.visit_key
        inner join {{source('cdw', 'department')}} as department
            on department.dept_key = visit_appointment.dept_key
        inner join {{source('cdw', 'order_question')}} as order_question
            on order_question.ord_key = procedure_order.proc_ord_key
        inner join {{source('cdw', 'master_question')}} as master_question
            on master_question.quest_key = order_question.quest_key
        inner join {{source('cdw', 'visit_procedure_question')}} as visit_procedure_question
            on visit_procedure_question.visit_key = visit.visit_key
        inner join {{source('cdw', 'master_question')}} as master_visit_question
            on master_visit_question.quest_key = visit_procedure_question.quest_key
    where
        lower(procedure_order.proc_ord_nm) = 'elective admission request'
        and date(visit_appointment.appt_start_dt) >= current_date
    group by
        procedure_order.pat_key,
        procedure_order.proc_ord_key,
        visit_appointment.appt_start_dt,
        department.dept_nm,
        procedure_order.proc_ord_nm,
        visit.visit_key
),
procedure_diagnosis as (
    select
        workqueue_orders.proc_ord_key,
        diagnosis.dx_nm as diagnosis_name,
        diagnosis.icd10_cd as icd10_code,
        row_number() over(
            partition by procedure_order_diagnosis.proc_ord_key
            order by diagnosis.dx_nm
        ) as row_num
    from
        workqueue_orders
        inner join {{source('cdw', 'procedure_order_diagnosis')}} as procedure_order_diagnosis
            on procedure_order_diagnosis.proc_ord_key = workqueue_orders.proc_ord_key
        inner join {{source('cdw', 'diagnosis')}} as diagnosis
            on diagnosis.dx_key = procedure_order_diagnosis.dx_key
            and diagnosis.icd10_ind = 1
)
select
    procedure_diagnosis.diagnosis_name,
    procedure_diagnosis.icd10_code,
    workqueue_orders.visit_reason,
    workqueue_orders.scheduled_date,
    workqueue_orders.visit_department_name,
    workqueue_orders.service_name,
    workqueue_orders.scheduled_procedure,
    workqueue_orders.scheduled_destination,
    workqueue_orders.patient_priority,
    workqueue_orders.expected_los_desc,
    1 as inpatient_ind,
    case
        when workqueue_orders.icu_ind = 'No' then 0
        when workqueue_orders.icu_ind = 'Yes' then 1
        else null
    end as icu_ind,
    workqueue_orders.pat_key,
    workqueue_orders.visit_key
from
    workqueue_orders
    left join procedure_diagnosis
        on procedure_diagnosis.proc_ord_key = workqueue_orders.proc_ord_key
        and row_num = 1
