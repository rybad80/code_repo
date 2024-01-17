with surg_info as (
   select
        stg_surgery.or_key,
        initcap(dict_pat_class.dict_nm) as patient_class,
        dict_or_asa_rating.dict_nm as asa_rating,
        case when location.loc_id = 46 then 'NORA' else dict_or_svc.dict_nm end as service,
        upper(room.full_nm) as room,
        upper(location.loc_nm) as surgery_location,
        case
            when location.loc_id in (900100100, 1.003) and room.prov_id in ('461', '123R.003')
                then 'NICU'
            when location.loc_id in (900100100, 1.003) and room.prov_id in ('460', '124R.003')
                then 'PICU'
            when location.loc_id in (900100100, 1.003) and room.prov_id in ('938', '940', '26R.003')
                then 'PACU'
            when location.loc_id in (900100100, 1.003) and room.prov_id in ('21452', '46R.003')
                then 'ER'
            when room.prov_id in (
                '109R.003',
                '108R.003',
                '37R.003',
                '32R.003',
                '120R.003',
                '30R.003',
                '96R.003',
                '33R.003',
                '58R.003',
                '31R.003',
                '57R.003',
                '39R.003',
                '121R.003',
                '29R.003',
                '27R.003',
                '117R.003',
                '34R.003',
                '122R.003',
                '45R.003'
            )
                then 'Other'
            when (location.loc_id in (900100105.000, 5.00300) and room.prov_id not in ('107R.003', '103R.003'))
                or room.prov_id in ('59R.003', '91R.003', '35R.003', '36R.003')
                then 'Cardiac'
            when location.loc_id in (900100104.000)
                or room.prov_id in ('103R.003', '107R.003', '110R.003', '111R.003')
                then 'SDU'
            when (location.loc_id = 900100100.000 and room.prov_id in ('0', '106', '112', '111'))
                    or (location.loc_id = 1.003 and room.prov_id in ('28R.003', '85R.003'))
                    or (upper(room.full_nm) like 'CHOP OR #%')
                    then 'Main OR'
            when location.loc_id in (
                900100110,
                900100101,
                3.003,
                900100102,
                2.003,
                900100109,
                900100103,
                4.003
            )
                then 'ASC'
            when location.loc_id = 900100111
                then 'KOPH OR'
            else null
        end as location_group,
        dict_or_case_type.dict_nm as case_type,
        dict_or_case_class.dict_nm as case_class
    from
        {{ ref('stg_surgery') }} as stg_surgery
        left join {{ source('cdw', 'cdw_dictionary') }} as dict_or_case_type
            on dict_or_case_type.dict_key = stg_surgery.dict_or_case_type_key
        left join {{ source('cdw', 'cdw_dictionary') }} as dict_or_case_class
            on dict_or_case_class.dict_key = stg_surgery.dict_or_case_class_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dict_pat_class
            on dict_pat_class.dict_key = stg_surgery.dict_pat_class_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dict_or_svc
            on dict_or_svc.dict_key = stg_surgery.dict_or_svc_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as dict_or_asa_rating
            on dict_or_asa_rating.dict_key = stg_surgery.dict_or_asa_rating_key
        inner join {{ source('cdw', 'location') }} as location --noqa: L029
            on location.loc_key = stg_surgery.loc_key
        inner join {{ source('cdw', 'provider') }} as room
            on room.prov_key = stg_surgery.room_prov_key
),

num_surg as (
    select
        stg_surgery.visit_key,
        count(stg_surgery.case_key) as encounter_total_surgery_count
    from
        {{ ref('stg_surgery') }} as stg_surgery
    where
        stg_surgery.visit_key > 1
    group by
        stg_surgery.visit_key
),

count_log_proc as (
    select
        stg_surgery.log_key,
        min(
            case
                when or_log_surgeons.panel_num = 1 and or_log_all_procedures.all_proc_panel_num = 1
                    then or_log_all_procedures.seq_num
            end
        ) as first_panel_first_procedure_seq_num,
        max(or_log_all_procedures.seq_num) as number_of_procedures,
        max(or_log_all_procedures.all_proc_panel_num) as number_of_panels
    from
        {{ ref('stg_surgery') }} as stg_surgery
        inner join {{ source('cdw', 'or_log_all_procedures') }} as or_log_all_procedures
            on or_log_all_procedures.log_key = stg_surgery.log_key
        inner join {{ source('cdw', 'or_log_surgeons') }} as or_log_surgeons
            on or_log_surgeons.log_key = stg_surgery.log_key
    group by
        stg_surgery.log_key
),

first_panel_first_log_proc as (
    select
        stg_surgery.log_key,
        /*
        In _very_ rare and old cases, the first panel has multiple procedures with the same seq_num
        In those cases, we choose one of the procedures at random
        */
        max(or_procedure.or_proc_nm) as first_panel_first_procedure_name
    from
        {{ ref('stg_surgery') }} as stg_surgery
        inner join {{ source('cdw', 'or_log_all_procedures') }} as or_log_all_procedures
            on or_log_all_procedures.log_key = stg_surgery.log_key
        inner join {{ source('cdw', 'or_log_surgeons') }} as or_log_surgeons
            on or_log_surgeons.log_key = stg_surgery.log_key
        inner join {{ source('cdw', 'or_procedure') }} as or_procedure
            on or_procedure.or_proc_key = or_log_all_procedures.or_proc_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as or_role
            on or_role.dict_key = or_log_surgeons.dict_or_role_key
        inner join count_log_proc
            on count_log_proc.log_key = stg_surgery.log_key
    where
        or_log_surgeons.panel_num = 1
        and or_log_all_procedures.all_proc_panel_num = 1
        and or_log_all_procedures.seq_num = count_log_proc.first_panel_first_procedure_seq_num
        /* primary surgeon */
        and or_role.src_id in ('1.003', '1')
    group by
        stg_surgery.log_key
),

count_case_proc as (
    select
        stg_surgery.case_key,
        min(
            case
                when or_case_all_surgeons.panel_num = 1 and or_case_all_procedures.panel_num = 1
                    then or_case_all_procedures.seq_num
            end
        ) as first_panel_first_procedure_seq_num,
        min(
            case when or_case_all_surgeons.panel_num = 1 then or_case_all_procedures.or_proc_key end
        ) as first_panel_first_procedure_or_proc_key,
        max(or_case_all_procedures.seq_num) as number_of_procedures,
        max(or_case_all_procedures.panel_num) as number_of_panels
    from
        {{ ref('stg_surgery') }} as stg_surgery
        inner join {{ source('cdw', 'or_case_all_procedures') }} as or_case_all_procedures
            on or_case_all_procedures.or_case_key = stg_surgery.case_key
        inner join {{ source('cdw', 'or_case_all_surgeons') }} as or_case_all_surgeons
            on or_case_all_surgeons.or_case_key = stg_surgery.case_key
    group by
        stg_surgery.case_key
),

first_panel_first_case_proc as (
    select
        stg_surgery.case_key,
        or_procedure.or_proc_nm as first_panel_first_procedure_name
    from
        {{ ref('stg_surgery') }} as stg_surgery
        inner join {{ source('cdw', 'or_case_all_procedures') }} as or_case_all_procedures
            on or_case_all_procedures.or_case_key = stg_surgery.case_key
        inner join {{ source('cdw', 'or_case_all_surgeons') }} as or_case_all_surgeons
            on or_case_all_surgeons.or_case_key = stg_surgery.case_key
        inner join {{ source('cdw', 'or_procedure') }} as or_procedure
            on or_procedure.or_proc_key = or_case_all_procedures.or_proc_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as or_panel_role
            on or_panel_role.dict_key = or_case_all_surgeons.dict_or_panel_role_key
        inner join count_case_proc
            on count_case_proc.case_key = stg_surgery.case_key
    where
        or_case_all_surgeons.panel_num = 1
        and or_case_all_procedures.panel_num = 1
        and or_case_all_procedures.seq_num = count_case_proc.first_panel_first_procedure_seq_num
        /* primary panel surgeon */
        and or_panel_role.src_id in ('1.003', '1')
    group by
        stg_surgery.case_key,
        or_procedure.or_proc_nm
),

add_on_cases as (
    select
        case_key,
        case
            when add_on_case_ind = 1 or add_on_case_sch_ind = 1 then 1
            else 0
        end as add_on_case_ind
    from
        {{ ref('stg_surgery_case') }}
)

select
    stg_surgery.or_key,
    stg_surgery.case_status,
    stg_surgery.patient_name,
    stg_surgery.mrn,
    stg_surgery.dob,
    stg_surgery.csn,
    stg_surgery.surgery_csn,
    stg_surgery_anesthesia.anesthesia_csn,
    stg_surgery.encounter_date,
    num_surg.encounter_total_surgery_count,
    stg_surgery.surgery_date,
    (date(stg_surgery.surgery_date) - date(stg_surgery.dob)) / 365.25 as surgery_age_years,
    surg_info.service,
    surg_info.patient_class as patient_class,
    surg_info.room,
    surg_info.surgery_location as location, --noqa: L029, reinstate linting after column name change
    surg_info.location_group,
    surg_info.asa_rating,
    surg_info.case_type,
    surg_info.case_class,
    coalesce(
        first_panel_first_case_proc.first_panel_first_procedure_name,
        first_panel_first_log_proc.first_panel_first_procedure_name
    ) as first_panel_first_procedure_name,
    coalesce(count_log_proc.number_of_panels, count_case_proc.number_of_panels) as number_of_panels,
    coalesce(count_log_proc.number_of_procedures, count_case_proc.number_of_procedures) as number_of_procedures,
    stg_surgery.primary_surgeon,
    stg_surgery.source_system,
    stg_surgery.posted_ind,
    fact_or_log.first_case_ind,
    add_on_cases.add_on_case_ind,
    stg_surgery.case_id,
    stg_surgery.log_id,
    stg_surgery_anesthesia.anesthesia_id,
    stg_surgery.surgeon_prov_key,
    stg_surgery.case_key,
    stg_surgery.log_key,
    stg_surgery.pat_key,
    stg_surgery.hsp_acct_key,
    stg_surgery.visit_key,
    stg_surgery_anesthesia.anes_key,
    stg_surgery_anesthesia.anes_visit_key,
    stg_surgery.vsi_key
from
    {{ ref('stg_surgery') }} as stg_surgery
    left join {{ source('cdw', 'fact_or_log') }} as fact_or_log
        on fact_or_log.log_key = stg_surgery.log_key
        and fact_or_log.surg_prov_key = stg_surgery.surgeon_prov_key
    left join {{ ref('stg_surgery_anesthesia') }} as stg_surgery_anesthesia
        on stg_surgery_anesthesia.or_key = stg_surgery.or_key
    inner join surg_info
        on surg_info.or_key = stg_surgery.or_key
    left join num_surg
        on num_surg.visit_key = stg_surgery.visit_key
    left join count_log_proc
        on count_log_proc.log_key = stg_surgery.log_key
            and stg_surgery.case_status = 'Completed'
    left join count_case_proc
        on count_case_proc.case_key = stg_surgery.case_key
            and stg_surgery.case_status = 'Scheduled'
    left join first_panel_first_log_proc
        on first_panel_first_log_proc.log_key = stg_surgery.log_key
            and stg_surgery.case_status = 'Completed'
    left join first_panel_first_case_proc
        on first_panel_first_case_proc.case_key = stg_surgery.case_key
            and stg_surgery.case_status = 'Scheduled'
    left join add_on_cases
        on add_on_cases.case_key = stg_surgery.case_key
where
    /* Filter out a few, mostly older, edges cases (~35) where we don't have a record of a procedure */
    coalesce(
        first_panel_first_case_proc.first_panel_first_procedure_name,
        first_panel_first_log_proc.first_panel_first_procedure_name
    ) is not null
