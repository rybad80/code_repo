select
    adt_service.visit_event_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    stg_encounter.hospital_admit_date,
    stg_encounter.hospital_discharge_date,
    adt_service.service as itcu_service,
    case 
        when
            adt_service.service_start_datetime < stg_adt_all.dept_grp_enter_date
            then stg_adt_all.dept_grp_enter_date
        else adt_service.service_start_datetime
    end as itcu_service_start_datetime,
    case 
        when adt_service.service_end_datetime > stg_adt_all.dept_grp_exit_date then stg_adt_all.dept_grp_exit_date
        else adt_service.service_end_datetime
    end as itcu_service_end_datetime,
    row_number()
        over (partition by adt_service.visit_key order by itcu_service_start_datetime)
        as itcu_service_number,
    {{
        dbt_chop_utils.datetime_diff(
            from_date='itcu_service_start_datetime',
            to_date='itcu_service_end_datetime',
            unit='day'
        )
    }} as itcu_service_los_days,
    stg_encounter.visit_key,
    stg_patient.pat_key,
    stg_patient.pat_id
from
    {{ ref('adt_service') }} as adt_service
    inner join {{ ref('stg_adt_all') }} as stg_adt_all
        on stg_adt_all.visit_key = adt_service.visit_key
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.visit_key = adt_service.visit_key
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = stg_encounter.pat_key
where
    lower(stg_adt_all.department_group_name) = 'itcu'
    and stg_adt_all.department_group_ind = 1
    /* was on that service at some point during itcu stay */
    and (not (
    adt_service.service_start_datetime >= stg_adt_all.dept_grp_exit_date
    or adt_service.service_end_datetime <= stg_adt_all.dept_grp_enter_date)
    or (adt_service.service_start_datetime is not null and adt_service.service_end_datetime is null))
