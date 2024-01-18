with transaction_raw as (
    select
        adt_department.visit_key,
        adt_department.enter_date as enter_date,
        adt_department.department_name as dept_nm,
        adt_department.hospital_discharge_date,
        adt_department.initial_service as adt_service,
        adt_department.department_group_name as dept_grp_nm,
        lag(adt_department.department_group_name) over(
            partition by adt_department.visit_key order by adt_department.enter_date
        ) as prev_dept_group_nm,
        case when adt_department.department_group_name != prev_dept_group_nm or prev_dept_group_nm is null
            then 1
            else 0 end as new_dept_group_ind
    from
        {{ref('adt_department')}} as adt_department
),

next_dept as (
    select
        visit_key,
        enter_date,
        dept_grp_nm,
        lead(enter_date, 1, hospital_discharge_date)
            over (partition by visit_key order by enter_date)
            as dept_exit_date,
        lead(dept_grp_nm, 1, null) over (partition by visit_key order by enter_date) as next_dept
    from
        transaction_raw
    where
        new_dept_group_ind = 1
),

adt_details as (
    select
        transaction_raw.visit_key,
        transaction_raw.dept_grp_nm,
        transaction_raw.enter_date as in_date,
        {{
            dbt_utils.surrogate_key([
                'transaction_raw.visit_key',
                'in_date'
                ])
        }} as cicu_enc_key,
        max(case when next_dept.dept_exit_date is not null
            and next_dept.dept_exit_date
            < coalesce(patient.death_dt, current_date)
            then next_dept.dept_exit_date
            when patient.death_dt is not null
                then patient.death_dt
            else current_date end) as out_date,
        extract(epoch from out_date - in_date) / 3600.0 as cicu_los_hrs
    from
        transaction_raw
        inner join next_dept
            on next_dept.visit_key = transaction_raw.visit_key
                and transaction_raw.enter_date = next_dept.enter_date
                and transaction_raw.dept_grp_nm = 'CICU'
                and transaction_raw.enter_date >= '01/01/2018'
        left join {{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter
            on cardiac_unit_encounter.visit_key = transaction_raw.visit_key
                and lower(cardiac_unit_encounter.department_name) = 'cicu'
        left join {{source('cdw', 'patient')}} as patient
            on cardiac_unit_encounter.pat_key = patient.pat_key
        left join {{ref('stg_cardiac_pc4_arrest')}} as stg_cardiac_pc4_arrest
            on stg_cardiac_pc4_arrest.r_enc_key = cardiac_unit_encounter.enc_key
    group by
        transaction_raw.visit_key,
        transaction_raw.dept_grp_nm,
        transaction_raw.enter_date
),

arrest_indicators as (
    select
        adt_details.visit_key,
        adt_details.in_date,
        max(case when stg_cardiac_pc4_arrest.r_card_arrest_strt_dt >= adt_details.in_date
            and stg_cardiac_pc4_arrest.r_card_arrest_strt_dt <= adt_details.out_date
            then 1
            else 0 end
        ) as arrest_ind,
        min(case when stg_cardiac_pc4_arrest.r_card_arrest_strt_dt >= adt_details.in_date
            and stg_cardiac_pc4_arrest.r_card_arrest_strt_dt <= adt_details.out_date
            then stg_cardiac_pc4_arrest.r_card_arrest_strt_dt
            else null end
        ) as first_arrest_date
    from
        adt_details
        left join {{ref('cardiac_unit_encounter')}} as cardiac_unit_encounter
            on cardiac_unit_encounter.visit_key = adt_details.visit_key
                and lower(cardiac_unit_encounter.department_name) = 'cicu'
        left join {{source('cdw', 'patient')}} as patient
            on cardiac_unit_encounter.pat_key = patient.pat_key
        left join {{ref('stg_cardiac_pc4_arrest')}} as stg_cardiac_pc4_arrest
            on stg_cardiac_pc4_arrest.r_enc_key = cardiac_unit_encounter.enc_key
    group by
        adt_details.visit_key,
        adt_details.in_date
),

--necessary because the initial approach pulls in multiple cardiac encounters
cardiac_encounter_one_row as (
    select 
        visit_key,
        pat_key,
        dob,
        mrn,
        hospital_admit_date,
        hospital_discharge_date,
        row_number() over(
            partition by visit_key
            order by registry, department_admit_date)
        as cardiac_encounter_seq_num
    from {{ref('cardiac_unit_encounter')}}
    where
        lower(department_name) = 'cicu'
)

select
    adt_details.visit_key,
    cardiac_encounter_one_row.pat_key,
    cardiac_encounter_one_row.dob,
    cardiac_encounter_one_row.mrn,
    cardiac_encounter_one_row.hospital_admit_date,
    coalesce(cardiac_encounter_one_row.hospital_discharge_date, '01-01-2100') as hospital_discharge_date,
    adt_details.dept_grp_nm,
    adt_details.in_date,
    adt_details.cicu_enc_key,
    adt_details.out_date,
    adt_details.cicu_los_hrs,
    arrest_indicators.arrest_ind,
    arrest_indicators.first_arrest_date,
    coalesce(arrest_indicators.first_arrest_date,
    adt_details.in_date + (adt_details.out_date - adt_details.in_date) / 2.0) as mid_end_date,
    coalesce(arrest_indicators.first_arrest_date,
    adt_details.in_date + random() * (adt_details.out_date - adt_details.in_date)) as rand_end_date
from
    adt_details
    left join cardiac_encounter_one_row as cardiac_encounter_one_row
        on cardiac_encounter_one_row.visit_key = adt_details.visit_key
        and cardiac_encounter_seq_num = 1
    left join arrest_indicators
        on arrest_indicators.visit_key = adt_details.visit_key
        and arrest_indicators.in_date = adt_details.in_date
