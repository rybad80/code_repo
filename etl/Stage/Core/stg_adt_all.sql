{{ config(meta = {
    'critical': true
}) }}

with bed_one_row as (
    select
        visit_event_key,
        adt_event_id,
        patient_name,
        mrn,
        dob,
        csn,
        encounter_date,
        hospital_admit_date,
        hospital_discharge_date,
        bed_name,
        bed_type,
        room_name,
        department_name,
        department_group_name,
        department_center_abbr,
        department_center_id,
        intended_use_group,
        bed_care_group,
        initial_service,
        ed_ind,
        icu_ind,
        enter_date as bed_enter_date,
        lead(enter_date, 1, hospital_discharge_date) over ( --noqa: PRS
            partition by visit_key order by enter_date, adt_event_id desc
        ) as bed_exit_date,
        extract(epoch from bed_exit_date - enter_date) / 3600.0 as bed_los_hrs, --noqa: PRS
        extract(epoch from bed_exit_date - enter_date) / 86400.0 as bed_los_days, --noqa: PRS
        case
            when lower(visit_source_system) = 'idx' then 0
            when hospital_discharge_date is null then 1
            else 0
        end as currently_admitted_ind,
        coalesce(bed_exit_date, current_date) as bed_exit_date_or_current_date,
        extract( --noqa: PRS
            epoch from bed_exit_date_or_current_date - enter_date
        ) / 3600.0 as bed_los_hrs_as_of_today,
        extract( --noqa: PRS
            epoch from bed_exit_date_or_current_date - enter_date
        ) / 86400.0 as bed_los_days_as_of_today,
        row_number() over (partition by visit_key order by enter_date, adt_event_id desc) as all_bed_order,
        case
            when row_number() over (partition by visit_key order by enter_date desc, adt_event_id) = 1
                then 1
            else 0
        end as last_bed_ind,
        ip_unit_ind,
        pat_key,
        patient_key,
        visit_key,
        encounter_key,
        bed_key,
        room_key,
        dept_key,
        department_id,
        department_group_key,
        1 as bed_ind
    from
        {{ref('stg_adt_all_raw')}}
    where
        new_bed_ind = 1
),

dept_one_row as (
    select
        visit_event_key,
        adt_event_id,
        enter_date as dept_enter_date,
        lead(enter_date, 1, hospital_discharge_date) over (
            partition by visit_key order by enter_date, adt_event_id desc
        ) as dept_exit_date,
        extract(epoch from dept_exit_date - enter_date) / 3600.0 as dept_los_hrs, --noqa: PRS
        extract(epoch from dept_exit_date - enter_date) / 86400.0 as dept_los_days, --noqa: PRS
        coalesce(dept_exit_date, current_date) as dept_exit_date_or_current_date,
        extract( --noqa: PRS
            epoch from dept_exit_date_or_current_date - enter_date
        ) / 3600.0 as dept_los_hrs_as_of_today,
        extract( --noqa: PRS
            epoch from dept_exit_date_or_current_date - enter_date
        ) / 86400.0 as dept_los_days_as_of_today,
        row_number() over (
            partition by visit_key order by enter_date, adt_event_id desc
        ) as all_department_order,
        case when extract( --noqa: PRS
            epoch from coalesce(hospital_discharge_date, current_timestamp) - hospital_admit_date
            ) > (24.0 * 60.0 * 60.0)
                    and (
                        department_group_name = 'CPRU'
                        and dept_los_hrs_as_of_today > 20
                        and enter_date < '2020-10-19'
                    )
        then 1 else 0
        end as cpru_20_hosp_24_ind,
        case when ip_unit_ind = 1 or cpru_20_hosp_24_ind = 1 then 1 else 0 end as considered_ip_unit,
        case
            when row_number() over (partition by visit_key order by enter_date desc, adt_event_id) = 1
                then 1
            else 0
        end as last_department_ind,
        case -- last physical department (no MAIN TRANSPORT/Virtual Units)
            when row_number() over (partition by visit_key
                                order by
                                case
                                    when bed_care_group is null
                                        and ip_unit_ind = 0
                                    then 1
                                    else 0
                                end,
                                dept_enter_date desc,
                                adt_event_id) = 1
                then 1
            else 0
        end as discharge_department_ind,
        1 as department_ind,
        visit_key
    from
        {{ref('stg_adt_all_raw')}}
    where
        new_dept_ind = 1
),

dept_inpatient_order as (
    select
        visit_event_key,
        case
            when
                considered_ip_unit = 1 then row_number() over (
                    partition by visit_key, considered_ip_unit order by dept_enter_date, adt_event_id desc
                )
        end as inpatient_department_order
    from
        dept_one_row
),

dept_grp_one_row as (
    select
        visit_event_key,
        adt_event_id,
        enter_date as dept_grp_enter_date,
        lead(enter_date, 1, hospital_discharge_date) over (
            partition by visit_key order by enter_date, adt_event_id desc
        ) as dept_grp_exit_date,
        extract(epoch from dept_grp_exit_date - enter_date) / 3600.0 as dept_grp_los_hrs, --noqa: PRS
        extract(epoch from dept_grp_exit_date - enter_date) / 86400.0 as dept_grp_los_days, --noqa: PRS
        coalesce(dept_grp_exit_date, current_date) as dept_grp_exit_date_or_current_date,
        extract( --noqa: PRS
            epoch from dept_grp_exit_date_or_current_date - enter_date
        ) / 3600.0 as dept_grp_los_hrs_as_of_today,
        extract( --noqa: PRS
            epoch from dept_grp_exit_date_or_current_date - enter_date
        ) / 86400.0 as dept_grp_los_days_as_of_today,
        row_number() over (
            partition by visit_key order by enter_date, adt_event_id desc
        ) as all_department_group_order,
        case when (
            extract( --noqa: PRS
                epoch from coalesce(hospital_discharge_date, current_timestamp) - hospital_admit_date
            )
            > (24.0 * 60.0 * 60.0)
        )
            and (
                department_group_name = 'CPRU'
                and dept_grp_los_hrs_as_of_today > 20
                and enter_date < '2020-10-19'
            )
        then 1 else 0
        end as cpru_20_hosp_24_ind,
        case when ip_unit_ind = 1 or cpru_20_hosp_24_ind = 1 then 1 else 0 end as considered_ip_unit,
        case
            when row_number() over (partition by visit_key order by enter_date desc, adt_event_id) = 1 then 1
            else 0
        end as last_department_group_ind,
        1 as department_group_ind,
        visit_key
    from
        {{ref('stg_adt_all_raw')}}
    where
        new_dept_grp_ind = 1
),

dept_grp_inpatient_order as (
    select
        visit_event_key,
        case
            when
                considered_ip_unit = 1 then row_number() over (
                    partition by visit_key, considered_ip_unit order by dept_grp_enter_date, adt_event_id desc
                )
        end as inpatient_department_group_order
    from
        dept_grp_one_row
),

bed_considered_ip as (
    select
        bed_one_row.visit_event_key,
        max(
            case when extract( --noqa: PRS
                epoch from coalesce(bed_one_row.hospital_discharge_date, current_timestamp)
                - bed_one_row.hospital_admit_date
                ) > (24.0 * 60.0 * 60.0)
                        and (
                            bed_one_row.department_group_name = 'CPRU'
                            and dept_los_hrs_as_of_today > 20
                            and bed_one_row.bed_enter_date < '2020-10-19'
                        )
            then 1 else 0
        end ) as cpru_20_hosp_24_ind,
        max(
            case
                when bed_one_row.ip_unit_ind = 1 or cpru_20_hosp_24_ind = 1
                then 1
                else 0
        end ) as considered_ip_unit
    from
        bed_one_row
        inner join dept_one_row
            on bed_one_row.visit_key = dept_one_row.visit_key
            and bed_one_row.bed_enter_date between dept_one_row.dept_enter_date and dept_one_row.dept_exit_date
    group by
        bed_one_row.visit_event_key
)

select
    bed_one_row.visit_event_key,
    bed_one_row.patient_name,
    bed_one_row.mrn,
    bed_one_row.dob,
    bed_one_row.csn,
    bed_one_row.encounter_date,
    bed_one_row.hospital_admit_date,
    bed_one_row.hospital_discharge_date,
    bed_one_row.intended_use_group,
    bed_one_row.bed_care_group,
    bed_one_row.initial_service,
    bed_one_row.currently_admitted_ind,
    bed_one_row.ed_ind,
    bed_one_row.icu_ind,
    sum(
        case when bed_one_row.icu_ind = 1 then bed_one_row.bed_los_hrs else 0 end
    ) over (partition by bed_one_row.visit_key) as icu_los_hrs,
    min(case when ip_unit_ind = 1 or dept_one_row.cpru_20_hosp_24_ind = 1 then bed_one_row.bed_enter_date end)
        over (partition by bed_one_row.visit_key) as ip_enter_date,
    bed_one_row.bed_name,
    bed_one_row.bed_type,
    bed_one_row.room_name,
    bed_one_row.bed_enter_date,
    bed_one_row.bed_exit_date,
    bed_one_row.bed_los_hrs,
    bed_one_row.bed_los_days,
    bed_one_row.bed_exit_date_or_current_date,
    bed_one_row.bed_los_hrs_as_of_today,
    bed_one_row.bed_los_days_as_of_today,
    bed_one_row.all_bed_order,
    case
        when bed_considered_ip.considered_ip_unit = 1 then row_number() over (
            partition by bed_one_row.visit_key, bed_considered_ip.considered_ip_unit
            order by bed_one_row.bed_enter_date, bed_one_row.adt_event_id desc)
    end as inpatient_bed_order,
    bed_one_row.last_bed_ind,
    bed_one_row.bed_ind,
    bed_one_row.department_name,
    case
        when
            bed_considered_ip.cpru_20_hosp_24_ind = 1
        then '104'
        else bed_one_row.department_center_id
    end as department_center_id,
    case
    when
        bed_considered_ip.cpru_20_hosp_24_ind = 1
    then 'PHL IP Cmps'
    else bed_one_row.department_center_abbr
    end as department_center_abbr,
    dept_one_row.dept_enter_date,
    dept_one_row.dept_exit_date,
    dept_one_row.dept_los_hrs,
    dept_one_row.dept_los_days,
    dept_one_row.dept_exit_date_or_current_date,
    dept_one_row.dept_los_hrs_as_of_today,
    dept_one_row.dept_los_days_as_of_today,
    dept_one_row.all_department_order,
    dept_inpatient_order.inpatient_department_order,
    dept_one_row.last_department_ind,
    case
        when bed_one_row.hospital_discharge_date is not null
        then dept_one_row.discharge_department_ind
        else 0
    end as discharge_department_ind,
    dept_one_row.department_ind,
    bed_one_row.department_group_name,
    dept_grp_one_row.dept_grp_enter_date,
    dept_grp_one_row.dept_grp_exit_date,
    dept_grp_one_row.dept_grp_los_hrs,
    dept_grp_one_row.dept_grp_los_days,
    dept_grp_one_row.dept_grp_exit_date_or_current_date,
    dept_grp_one_row.dept_grp_los_hrs_as_of_today,
    dept_grp_one_row.dept_grp_los_days_as_of_today,
    dept_grp_one_row.all_department_group_order,
    dept_grp_inpatient_order.inpatient_department_group_order,
    dept_grp_one_row.last_department_group_ind,
    dept_grp_one_row.department_group_ind,
    bed_one_row.ip_unit_ind,
    dept_one_row.cpru_20_hosp_24_ind,
    dept_one_row.considered_ip_unit,
    bed_one_row.pat_key,
    bed_one_row.patient_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    bed_one_row.visit_key,
    bed_one_row.encounter_key,
    bed_one_row.bed_key,
    bed_one_row.room_key,
    bed_one_row.dept_key,
    bed_one_row.department_id,
    bed_one_row.department_group_key
from
    bed_one_row
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = bed_one_row.visit_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.visit_key = bed_one_row.visit_key
    left join bed_considered_ip
        on bed_considered_ip.visit_event_key = bed_one_row.visit_event_key
    left join dept_one_row
        on dept_one_row.visit_event_key = bed_one_row.visit_event_key
    left join dept_grp_one_row
        on dept_grp_one_row.visit_event_key = bed_one_row.visit_event_key
    left join dept_grp_inpatient_order
        on dept_grp_inpatient_order.visit_event_key = bed_one_row.visit_event_key
    left join dept_inpatient_order
        on dept_inpatient_order.visit_event_key = bed_one_row.visit_event_key
