with booked as (
    select
        surgery_department_or_case_procedure.or_key,
        date(min(or_case_audit_history.audit_act_dt)) as first_booked_date
    from
        {{source('cdw', 'or_case')}} as or_case
        inner join {{source('cdw', 'or_case_audit_history')}} as or_case_audit_history
            on or_case_audit_history.or_case_key = or_case.or_case_key
        inner join {{source('cdw', 'dim_or_audit_action')}} as dim_or_audit_action
            on dim_or_audit_action.dim_or_audit_act_key = or_case_audit_history.dim_or_audit_act_key
        inner join {{ref('surgery_department_or_case_procedure')}} as surgery_department_or_case_procedure
            on surgery_department_or_case_procedure.case_key = or_case.or_case_key
    where
        lower(dim_or_audit_action.or_audit_act_nm) = 'scheduled'
    group by
        surgery_department_or_case_procedure.or_key
),
ordered as (
    select
        or_case.or_case_key,
        date(procedure_order_clinical.placed_date) as request_date,
        procedure_order_clinical.proc_ord_key,
        row_number() over(
            partition by
                or_case.or_case_key
            order by
                (case
                    when lower(procedure_order_clinical.procedure_name) like '%request%'
                    then 1
                    else 0
                end) desc, /*prioritizes case request orders*/
                procedure_order_clinical.placed_date,
                procedure_order_clinical.proc_ord_key
        ) as rnk
    from
        {{source('cdw', 'or_case')}} as or_case
        inner join {{source('cdw', 'or_case_order')}}  as or_case_order
            on or_case_order.or_case_key = or_case.or_case_key
        inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
            on procedure_order_clinical.proc_ord_key = or_case_order.ord_key
            and procedure_order_clinical.procedure_order_type != 'Parent Order'
),
first_surgeon as (
    select
        surgery_department_or_case_procedure.or_key,
        surgery_department_or_case_procedure.primary_surgeon,
        surgery_department_or_case_procedure.surgeon_prov_key,
        surgery_department_or_case_procedure.primary_surgeon_provider_id,
        surgery_department_or_case_procedure.service,
        surgery_department_or_case_procedure.surgery_division,
        row_number() over(
            partition by surgery_department_or_case_procedure.or_key
            order by
                surgery_department_or_case_procedure.panel_number,
                surgery_department_or_case_procedure.procedure_seq_num
        ) as surgeon_seq_num
    from
        {{ref('surgery_department_or_case_procedure')}} as surgery_department_or_case_procedure
),
distinct_surgeons as (
    select
        surgery_department_or_case_procedure.or_key,
        '{"surgeon_provider_id": "'
        || provider.prov_id
        || '", "surgeon": "'
        || surgery_department_or_case_procedure.primary_surgeon
        || '"}'
        as all_surgeons
    from
        {{ref('surgery_department_or_case_procedure')}} as surgery_department_or_case_procedure
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = surgery_department_or_case_procedure.surgeon_prov_key
    group by
        surgery_department_or_case_procedure.or_key,
        provider.prov_id,
        surgery_department_or_case_procedure.primary_surgeon
),
distinct_services as (
    select
        surgery_department_or_case_procedure.or_key,
        max(surgery_department_or_case_procedure.surgery_department_ind) as surgery_department_ind,
        '{"panel_number": "'
            || surgery_department_or_case_procedure.panel_number
            || '", "service": "'
            || surgery_department_or_case_procedure.service
            || '", "surgery_division": "'
            || coalesce(surgery_department_or_case_procedure.surgery_division, '')
            || '"}'
            as all_services
    from
        {{ref('surgery_department_or_case_procedure')}} as surgery_department_or_case_procedure
    group by
        surgery_department_or_case_procedure.or_key,
        surgery_department_or_case_procedure.panel_number,
        surgery_department_or_case_procedure.service,
        surgery_department_or_case_procedure.surgery_division
),
distinct_procedures as (
    select
        surgery_department_or_case_procedure.or_key,
        group_concat(
            '{"cpt": "'
            || coalesce(surgery_department_or_case_procedure.cpt_code, '')
            || '", "proc_id": "'
            || coalesce(surgery_department_or_case_procedure.or_proc_id, '')
            || '", "description": "'
            || coalesce(surgery_department_or_case_procedure.or_procedure_name, '')
            || '", "laterality": "'
            || coalesce(surgery_department_or_case_procedure.laterality, '')
            || '", "panel_number": "'
            || coalesce(cast(surgery_department_or_case_procedure.panel_number as varchar(10)), '')
            || '", "service": "'
            || coalesce(surgery_department_or_case_procedure.service, '')
            || '", "surgeon_provider_id": "'
            || coalesce(provider.prov_id, '')
            || '", "surgeon": "'
            || coalesce(surgery_department_or_case_procedure.primary_surgeon, '')
            || '"}'
        ) as all_procedures
    from
        {{ref('surgery_department_or_case_procedure')}} as surgery_department_or_case_procedure
        left join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = surgery_department_or_case_procedure.surgeon_prov_key
    group by
        surgery_department_or_case_procedure.or_key,
        surgery_department_or_case_procedure.cpt_code,
        surgery_department_or_case_procedure.or_procedure_name,
        surgery_department_or_case_procedure.or_proc_id,
        surgery_department_or_case_procedure.laterality
    having
        all_procedures is not null
),
rollup_procedures as (
    select
        surgery_encounter.or_key,
        '[' || group_concat(distinct_procedures.all_procedures, ',') || ']' as all_procedures
    from
        {{ref('surgery_encounter')}} as surgery_encounter
        inner join distinct_procedures
            on distinct_procedures.or_key = surgery_encounter.or_key
    group by
        surgery_encounter.or_key
),
rollup_surgeons as (
    select
        surgery_encounter.or_key,
        '[' || group_concat(distinct_surgeons.all_surgeons, ',') || ']' as all_surgeons
    from
        {{ref('surgery_encounter')}} as surgery_encounter
        inner join distinct_surgeons
            on distinct_surgeons.or_key = surgery_encounter.or_key
    group by
        surgery_encounter.or_key
),
rollup_services as (
    select
        surgery_encounter.or_key,
        max(distinct_services.surgery_department_ind) as surgery_department_ind,
        '[' || group_concat(distinct_services.all_services, ',') || ']' as all_services
    from
        {{ref('surgery_encounter')}} as surgery_encounter
        inner join distinct_services
            on distinct_services.or_key = surgery_encounter.or_key
    group by
        surgery_encounter.or_key
),
rollup_surgery as (
    select
        surgery_department_or_case_procedure.or_key,
        surgery_department_or_case_procedure.surgery_date,
        surgery_department_or_case_procedure.visit_key,
        surgery_department_or_case_procedure.case_key,
        surgery_department_or_case_procedure.pat_key,
        count(distinct surgery_department_or_case_procedure.panel_number) as n_panels,
        count(*) as n_procedures,
        case
            when count(distinct surgery_department_or_case_procedure.or_key) over(
                partition by surgery_department_or_case_procedure.visit_key
                ) > 1
            then 1
            else 0
        end as multiple_surgery_visit_ind
    from
        {{ref('surgery_department_or_case_procedure')}} as surgery_department_or_case_procedure
    group by
        surgery_department_or_case_procedure.or_key,
        surgery_department_or_case_procedure.surgery_date,
        surgery_department_or_case_procedure.visit_key,
        surgery_department_or_case_procedure.case_key,
        surgery_department_or_case_procedure.pat_key
)
select
    rollup_surgery.or_key,
    surgery_encounter.mrn,
    surgery_encounter.patient_name,
    stg_encounter.csn,
    surgery_encounter.dob,
    stg_encounter.sex,
    stg_patient.race_ethnicity,
    surgery_encounter.surgery_age_years,
    surgery_encounter.surgery_date,
    surgery_encounter.case_status,
    surgery_encounter.log_id,
    surgery_encounter.location,
    surgery_encounter.location_group,
    surgery_encounter.room,
    surgery_encounter.patient_class,
    rollup_surgery.n_panels,
    rollup_surgery.n_procedures,
    first_surgeon.primary_surgeon,
    first_surgeon.primary_surgeon_provider_id,
    rollup_surgeons.all_surgeons,
    first_surgeon.service as primary_service,
    first_surgeon.surgery_division as primary_surgery_division,
    rollup_services.all_services,
    rollup_services.surgery_department_ind,
    rollup_procedures.all_procedures,
    ordered.request_date,
    booked.first_booked_date,
    booked.first_booked_date - ordered.request_date as n_days_request_to_booked,
    surgery_encounter.surgery_date - booked.first_booked_date as n_days_booked_to_surgery,
    or_case.tot_tm_needed - or_case.setup_offset - or_case.clnup_offset as scheduled_duration_mins,
    rollup_surgery.multiple_surgery_visit_ind,
    stg_encounter.patient_address_seq_num,
    stg_encounter.patient_address_zip_code,
    year(add_months(rollup_surgery.surgery_date, 6)) as fiscal_year,
    year(rollup_surgery.surgery_date) as calendar_year,
    date_trunc('month', rollup_surgery.surgery_date) as calendar_month,
    rollup_surgery.visit_key,
    surgery_encounter.pat_key,
    rollup_surgery.case_key,
    surgery_encounter.log_key,
    first_surgeon.surgeon_prov_key as primary_surgeon_prov_key,
    ordered.proc_ord_key
from
    rollup_surgery
    inner join {{source('cdw', 'or_case')}} as or_case
        on or_case.or_case_key = rollup_surgery.case_key
    inner join rollup_procedures
        on rollup_procedures.or_key = rollup_surgery.or_key
    inner join rollup_services
        on rollup_services.or_key = rollup_surgery.or_key
    inner join rollup_surgeons
        on rollup_surgeons.or_key = rollup_surgery.or_key
    inner join first_surgeon
        on first_surgeon.or_key = rollup_surgery.or_key and first_surgeon.surgeon_seq_num = 1
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = rollup_surgery.pat_key
    inner join {{ref('surgery_encounter')}} as surgery_encounter
        on surgery_encounter.or_key = rollup_surgery.or_key
    left join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = rollup_surgery.visit_key
    left join booked
        on booked.or_key = rollup_surgery.or_key
    left join ordered
        on ordered.or_case_key = or_case.or_case_key
        and ordered.rnk = 1
