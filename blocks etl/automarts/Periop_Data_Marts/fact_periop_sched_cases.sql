with cohort as (
--region
select
    or_case.or_case_key,
    or_case.or_case_id,
    or_case.admit_visit_key as visit_key,
    patient.pat_key,
    dict_sched_stat.dict_nm as scheduling_status,
    or_case.case_begin_dt as sched_start_dt,
    or_case.case_end_dt as sched_end_dt,

    dict_case_service.dict_nm as service,
    location.loc_nm as loc,
    room.full_nm as room,
    case when location.loc_id = 900100100
                    and room.prov_id not in ('0', -- unknown rooms (misdocumented procedures near optime go-live)
                                             '107', -- C-section OR
                                             -- Cardiac
                                             '144', '14611', '14612', '139', '134', '135', '143', '138', '142',
                                             '108', -- Fetal OR
                                             '461', -- NICU
                                             '460', -- PICU
                                             '938', '940') -- PACU
               then 1 else 0
               end as main_4th_fl_or_ind,
    dict_case_type.dict_nm as case_type,
    dict_case_class.dict_nm as case_class,
    dict_orc_pat_class.dict_nm as orc_pat_class,
    case when (lower(master_date.day_nm) != 'thursday' and time(or_case.case_begin_dt) = '07:20')
                or (lower(master_date.day_nm)  = 'thursday' and time(or_case.case_begin_dt) = '08:20') then 1
            else 0 end as first_case_ind,
    case when or_case.add_on_case_ind = 1 or or_case.add_on_case_sch_ind = 1 then 1 else 0 end as add_on_case_ind,
    case
        when location.loc_id in (900100110, 900100101, 900100102, 900100103, 900100109) then 1 else 0
    end as asc_ind,
    or_case.num_of_panels as cnt_panels

from {{ source('cdw', 'or_case') }} as or_case
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_sched_stat
        on dict_sched_stat.dict_key = or_case.dict_or_sched_stat_key
    inner join {{ source('cdw', 'patient') }} as patient
        on patient.pat_key = or_case.pat_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_case_service
            on dict_case_service.dict_key = or_case.dict_or_svc_key
    inner join {{ source('cdw', 'location') }} as location --noqa: L029
        on location.loc_key = or_case.loc_key
    inner join {{ source('cdw', 'provider') }} as room
        on room.prov_key = or_case.room_prov_key
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_date.dt_key = or_case.surg_dt_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_case_type     on
            dict_case_type.dict_key = or_case.dict_or_case_type_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_case_class
            on dict_case_class.dict_key = or_case.dict_or_case_class_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_case_loc
            on dict_case_loc.dict_key = or_case.dict_or_post_dest_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_orc_pat_class
            on dict_orc_pat_class.dict_key = or_case.dict_or_pat_class_key

where
    date(or_case.case_begin_dt) >= current_date
--endregion
), orc_surgeon as (
--region
select
    cohort.or_case_key,
    orc_surgeon_nm.full_nm as case_surgeon_primary

from cohort
    inner join {{ source('cdw', 'or_case') }} as or_case
        on or_case.or_case_key = cohort.or_case_key
    inner join {{ source('cdw', 'or_case_all_surgeons') }} as orc_surgeon
        on orc_surgeon.or_case_key = or_case.or_case_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_or_panel_role
            on dict_or_panel_role.dict_key = orc_surgeon.dict_or_panel_role_key
    inner join
        {{ source('cdw', 'provider') }} as orc_surgeon_nm
            on orc_surgeon_nm.prov_key = orc_surgeon.surg_prov_key

where
    dict_or_panel_role.src_id in (1.0000, 1.0030)
    and orc_surgeon.panel_num = 1

group by
    cohort.or_case_key,
    orc_surgeon_nm.full_nm
--endregion
), procs as (
--region
select
    orc_proc.or_case_key,
    max(orc_proc.seq_num) as cnt_procs,
    min(
        case when orc_surgeon.panel_num = 1 then orc_proc.seq_num end
    ) as min_proc_1st_panel

from cohort
    inner join {{ source('cdw', 'or_case_all_procedures') }} as orc_proc
        on orc_proc.or_case_key = cohort.or_case_key
    inner join {{ source('cdw', 'or_case_all_surgeons') }} as orc_surgeon
        on orc_surgeon.or_case_key = orc_proc.or_case_key

where orc_surgeon.panel_num = orc_proc.panel_num

group by orc_proc.or_case_key
--endregion
), proc_primary as (
--region
select
    orc_proc.or_case_key,
    or_procedure.or_proc_nm as proc_primary

from cohort
    inner join {{ source('cdw', 'or_case_all_procedures') }} as orc_proc
        on orc_proc.or_case_key = cohort.or_case_key
    inner join {{ source('cdw', 'or_procedure') }} as or_procedure
        on or_procedure.or_proc_key = orc_proc.or_proc_key
    inner join {{ source('cdw', 'or_case_all_surgeons') }} as orc_surgeon
        on orc_surgeon.or_case_key = orc_proc.or_case_key
    inner join procs on procs.or_case_key = cohort.or_case_key

where
    orc_surgeon.panel_num = 1
    and orc_proc.seq_num = procs.min_proc_1st_panel
    and orc_surgeon.panel_num = orc_proc.panel_num

group by
    orc_proc.or_case_key,
    or_procedure.or_proc_nm

--endregion
), arc_destination as (
--region
select
    cohort.or_case_key,
    group_concat(
        case when clinical_concept.concept_id = 'CHOP#1039' then smart_data_element_value.elem_val end, ';'
    ) as initial_pat_dest,
    group_concat(
        case when clinical_concept.concept_id = 'CHOPANES#008' then smart_data_element_value.elem_val end, ';'
    )  as final_pat_dest

from cohort
    inner join {{ source('cdw', 'anesthesia_encounter_link') }} as anesthesia_encounter_link
        on anesthesia_encounter_link.or_case_key = cohort.or_case_key
	inner join
        {{ source('cdw', 'smart_data_element_info') }} as smart_data_element_info   on
            smart_data_element_info.visit_key = anesthesia_encounter_link.anes_event_visit_key
	inner join
        {{ source('cdw', 'smart_data_element_value') }} as smart_data_element_value  on
            smart_data_element_info.sde_key = smart_data_element_value.sde_key
    inner join
        {{ source('cdw', 'clinical_concept') }} as clinical_concept          on
            clinical_concept.concept_key = smart_data_element_info.concept_key

where
    smart_data_element_info.src_sys_val = 'SmartForm 283' -- unique ID of the anes pre plan smart form
    -- INITIAL PATIENT DESTINATION, CHOP ANES FINAL PATIENT DESTINATION
    and clinical_concept.concept_id in ('CHOP#1039', 'CHOPANES#008')
    and smart_data_element_value.elem_val is not null

group by cohort.or_case_key

--endregion
)

select
    cohort.or_case_key,
    cohort.or_case_id,
    cohort.pat_key,
    date(cohort.sched_start_dt) as sched_surgery_dt,
    cohort.sched_start_dt,
    cohort.sched_end_dt,
    cohort.scheduling_status,
    cohort.service,
    cohort.loc,
    cohort.room,
    cohort.main_4th_fl_or_ind,
    cohort.case_type,
    cohort.case_class,
    cohort.orc_pat_class,
    proc_primary.proc_primary,
    orc_surgeon.case_surgeon_primary,
    cohort.cnt_panels,
    procs.cnt_procs,
    cohort.first_case_ind,
    cohort.add_on_case_ind,
    cohort.asc_ind,
    arc_destination.initial_pat_dest,
    arc_destination.final_pat_dest

from cohort
    left join orc_surgeon     on orc_surgeon.or_case_key = cohort.or_case_key
    inner join procs                on procs.or_case_key = cohort.or_case_key
    left join proc_primary    on proc_primary.or_case_key = cohort.or_case_key
    left join arc_destination on arc_destination.or_case_key = cohort.or_case_key
