/*===========================================
All PCU encounters FY17-present based ADT events removing periop transfers
=============================================*/

with adt as (
    select
    /*Flag first and last visits of segment to group together*/
        case when
            department_group_name = lag(department_group_name)
            over (
            partition by visit_key
            order by enter_date
            ) then 0 else 1
        end as start_segment_ind,
        case when
            department_group_name = lead(department_group_name)
            over (
            partition by visit_key
            order by enter_date
            ) then 0 else 1
        end as end_segment_ind,
        visit_key,
        pat_key,
        department_group_name,
        all_department_group_order,
        /*need these to connect to department level in next step*/
        visit_event_key,
        exit_date
    from
        {{ref('adt_department_group')}}
    where
        department_group_name not in ('Periop', 'COIC')
        and hospital_admit_date >= '2012-7-1'
),

/*Bring start and end segment records up to 1 row*/
pcu_segment as (
    select
        adt.visit_event_key,
        adt.visit_key,
        adt.pat_key,
        adt.all_department_group_order as start_all_department_group_order,
        case when adt.end_segment_ind = 0
            then lead(adt.all_department_group_order) over (
                partition by adt.visit_key
                order by adt.all_department_group_order
            )
            else adt.all_department_group_order
        end as end_all_department_group_order,
        start_department.all_department_order as start_all_department_order,
        case when adt.end_segment_ind = 0
            then lead(end_department.all_department_order) over (
                partition by adt.visit_key
                order by adt.all_department_group_order
            )
            else end_department.all_department_order
        end as end_all_department_order,
        adt.start_segment_ind
    from
        adt
        /*get department level info to find previous and next depts*/
        inner join {{ref('adt_department')}} as start_department
            on start_department.visit_event_key = adt.visit_event_key
        /*need to join on date here to get the last department in the group*/
        left join {{ref('adt_department')}} as end_department
            on end_department.visit_key = adt.visit_key
            and end_department.exit_date = adt.exit_date
    where
        adt.department_group_name = 'PCU'
        and (adt.start_segment_ind = 1
            or adt.end_segment_ind = 1)
)

select
    start_segment_adt.visit_event_key,
    stg_patient.mrn,
    encounter_inpatient.csn,
    stg_patient.patient_name,
    stg_patient.sex,
    cast(
        extract(epoch from start_segment_adt.enter_date - stg_patient.dob) / 3600.0 / 24.0 / 365.0
        as numeric(12, 4)
    )
    as age_years,
    start_segment_adt.enter_date as pcu_admit_date,
    dim_date_admit.fiscal_year as pcu_admit_fy,
    case
        when hour(start_segment_adt.enter_date) between 15 and 22
        then 'Evening/night Shift'
        else 'Day Shift'
    end as pcu_admit_shift,
    adt_bed.bed_name as first_bed_name,
    start_segment_adt.initial_service as service,
    /*ADT based previous department*/
    prev_department.department_name as previous_adt_department_name,
    prev_department.department_group_name as previous_adt_department_group_name,
    prev_department.bed_care_group as previous_adt_bed_care_group,
    prev_department.department_center_abbr as previous_adt_campus,
    /*using bed care group to break departments out into large segments
     and adding extra detail for ICUs*/
    cast(
        case
            when pcu_segment.start_all_department_group_order = 1
                then 'DIRECT ADMIT'
            /*PHL ICU -> PICU/NICU/CICU*/
            when prev_department.bed_care_group like 'PHL ICU%'
                then prev_department.department_group_name
            when prev_department.bed_care_group = 'PERIOP'
                then 'OR'
            /*Use bed care group name for ED and KOPH ICU*/
            when prev_department.bed_care_group in ('ED', 'KOPH ED', 'KOPH ICU')
                then prev_department.bed_care_group
            /*Everything else is general floor*/
            when prev_department.bed_care_group is not null
                then 'GENERAL FLOOR'
        end
        as varchar(30)
    )
    as origin_desc,
    end_segment_adt.exit_date as pcu_discharge_date,
    dim_date_disch.fiscal_year as pcu_discharge_fy,
    case
        when end_segment_adt.exit_date is null then null
        when hour(end_segment_adt.exit_date) between 15 and 22
        then 'Evening/night Shift'
        else 'Day Shift'
    end as pcu_discharge_shift,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.hospital_discharge_date,
    /*ADT based next department*/
    next_department.department_name as next_adt_department_name,
    next_department.department_group_name as next_adt_department_group_name,
    next_department.bed_care_group as next_adt_bed_care_group,
    next_department.department_center_abbr as next_adt_campus,
    stg_pcu_ccceo_redcap.destination as destination_desc,
    cast(
        extract(epoch from start_segment_adt.enter_date
        - encounter_inpatient.hospital_admit_date) / 3600.0 / 24.0
        as numeric(24, 6)
    )
    as pre_pcu_los_days,
    cast(
        extract(epoch from end_segment_adt.exit_date - start_segment_adt.enter_date) / 3600.0 / 24.0
        as numeric(24, 6)
    )
    as pcu_los_days,
    encounter_inpatient.hospital_los_days,
    cast(
        pcu_los_days / encounter_inpatient.hospital_los_days * 100
        as numeric(12, 3)
    )
    as pcu_percent_of_hospital_los,
    stg_pcu_ccceo_redcap.hsp_adm_rsn as hospital_admission_reason,
    stg_pcu_ccceo_redcap.pcu_adm_rsn as pcu_admission_reason,
    stg_pcu_ccceo_redcap.med_train as training_status,
    stg_pcu_ccceo_redcap.gtube as admission_gtube_ind,
    stg_pcu_ccceo_redcap.trach as trach_ind,
    stg_pcu_ccceo_redcap.trach_placement as trach_placement_date,
    stg_pcu_ccceo_redcap.new_trach as new_trach_ind,
    stg_pcu_ccceo_redcap.feed_tube as discharge_feed_tube,
    stg_pcu_ccceo_redcap.central_line as discharge_central_line,
    stg_pcu_ccceo_redcap.tpn as discharge_tpn_ind,
    stg_pcu_ccceo_redcap.team_color as discharge_team_name,
    stg_pcu_ccceo_redcap.projected_disp as projected_disposition,
    stg_pcu_ccceo_redcap.transport_mode,
    stg_pcu_ccceo_redcap.record as ccceo_redcap_id,
    pcu_segment.visit_key,
    pcu_segment.pat_key
from
    pcu_segment
    /*department group level information*/
    inner join {{ref('adt_department_group')}} as start_segment_adt
        on start_segment_adt.visit_key = pcu_segment.visit_key
        and start_segment_adt.all_department_group_order = pcu_segment.start_all_department_group_order
    inner join {{ref('adt_department_group')}} as end_segment_adt
        on end_segment_adt.visit_key = pcu_segment.visit_key
        and end_segment_adt.all_department_group_order = pcu_segment.end_all_department_group_order
    /*previous department*/
    left join {{ref('adt_department')}} as prev_department
        on prev_department.visit_key = pcu_segment.visit_key
        and prev_department.all_department_order = pcu_segment.start_all_department_order - 1
    /*next department*/
    left join {{ref('adt_department')}} as next_department
        on next_department.visit_key = pcu_segment.visit_key
        and next_department.all_department_order = pcu_segment.end_all_department_order + 1
    inner join {{ref('encounter_inpatient')}} as encounter_inpatient
        on encounter_inpatient.visit_key = pcu_segment.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = pcu_segment.pat_key
    inner join {{ref('adt_bed')}} as adt_bed
        on adt_bed.visit_event_key = start_segment_adt.visit_event_key
    inner join {{ref('dim_date')}} as dim_date_admit
        on dim_date_admit.full_date = date(start_segment_adt.enter_date)
    left join {{ref('dim_date')}} as dim_date_disch
        on dim_date_disch.full_date = date(end_segment_adt.exit_date)
    /*manually collected fields*/
    left join {{ref('stg_pcu_ccceo_redcap')}} as stg_pcu_ccceo_redcap
        on stg_pcu_ccceo_redcap.csn = encounter_inpatient.csn
        and stg_pcu_ccceo_redcap.pcu_adm_dt = start_segment_adt.enter_date
where
    pcu_segment.start_segment_ind = 1
    and start_segment_adt.enter_date >= '2016-7-1'
