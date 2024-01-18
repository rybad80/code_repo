with cohort as (
--region
select
    or_log.log_id,
    or_log.log_key,
    or_log.admit_visit_key as visit_key,
    or_log.vsi_key

from {{ source('cdw', 'or_case') }} as or_case
    inner join {{ source('cdw', 'or_log') }} as or_log
        on or_log.log_key = or_case.log_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_not_perf
        on dict_not_perf.dict_key = or_log.dict_not_perf_key
    inner join {{ source('cdw', 'master_date') }} as master_date
        on master_date.dt_key = or_log.surg_dt_key

where
    dict_not_perf.src_id = -2 -- not discontinued
    and or_log.log_id != 0 -- has log ID
    and master_date.full_dt >= '2013-05-04'

group by or_log.log_id, or_log.log_key, or_log.admit_visit_key, or_log.vsi_key
--endregion
), pat_class as (
--region
select
    cohort.log_key,
    dict_orc_pat_class.dict_nm as orc_pat_class,
    dict_orl_pat_class.dict_nm as orl_pat_class,
    -- take min to eliminate an issue where one patient had two hosp acct keys
    min(dict_har_pat_class.dict_nm) as har_pat_class

from cohort
    inner join {{ source('cdw', 'or_log') }} as or_log
        on or_log.log_key = cohort.log_key
    inner join {{ source('cdw', 'or_case') }} as or_case
        on or_case.log_key = cohort.log_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_orc_pat_class
        on dict_orc_pat_class.dict_key = or_case.dict_or_pat_class_key
    inner join
        {{source('cdw', 'cdw_dictionary') }} as dict_orl_pat_class      on
            dict_orl_pat_class.dict_key = or_log.dict_pat_class_key
    left join
        {{source('cdw', 'hospital_account_visit') }} as hospital_account_visit
            on hospital_account_visit.visit_key = or_log.admit_visit_key
    left join
        {{ source('cdw', 'hospital_account') }} as hospital_account
            on hospital_account.hsp_acct_key = hospital_account_visit.hsp_acct_key
    left join
        {{ source('cdw', 'cdw_dictionary') }} as dict_har_pat_class on
            dict_har_pat_class.dict_key = hospital_account.dict_acct_class_key

group by cohort.log_key, dict_orc_pat_class.dict_nm, dict_orl_pat_class.dict_nm
--endregion
), orc_surgeon as (
--region
select
    cohort.log_key,
    orc_surgeon_nm.full_nm as case_surgeon_primary

from cohort
    inner join {{ source('cdw', 'or_case') }} as or_case
        on or_case.log_key = cohort.log_key
    inner join {{ source('cdw', 'or_case_all_surgeons') }} as orc_surgeon
        on orc_surgeon.or_case_key = or_case.or_case_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_or_panel_role on
            dict_or_panel_role.dict_key = orc_surgeon.dict_or_panel_role_key
    inner join
        {{ source('cdw', 'provider') }} as orc_surgeon_nm           on
            orc_surgeon_nm.prov_key = orc_surgeon.surg_prov_key

where
    dict_or_panel_role.src_id in (1.0000, 1.0030)
    and orc_surgeon.panel_num = 1

group by cohort.log_key, orc_surgeon_nm.full_nm
--endregion
), orl_surgeon as (
--region
select
    cohort.log_key,
    -- taking arbitrary min because for some reason, a few patients in 2014 have two primray surgeons
    min(orl_surgeon_nm.full_nm) as log_surgeon_primary

from cohort
    inner join {{ source('cdw', 'or_log_surgeons') }} as orl_surgeon    on orl_surgeon.log_key = cohort.log_key
    inner join
        {{ source('cdw', 'provider') }} as orl_surgeon_nm        on
            orl_surgeon_nm.prov_key = orl_surgeon.surg_prov_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_or_role    on
            dict_or_role.dict_key = orl_surgeon.dict_or_role_key
    inner join {{ source('cdw', 'or_log_all_procedures') }} as orl_proc on orl_proc.log_key = cohort.log_key

where
    dict_or_role.src_id in (1.0000, 1.0030)
    and orl_surgeon.panel_num = 1
    and orl_proc.all_proc_panel_num = 1

group by cohort.log_key
--endregion
), procs as (
--region
select
    cohort.log_key,
    max(orl_proc.seq_num) as cnt_procs,
    min(case when orl_surgeon.panel_num = 1 then orl_proc.seq_num end) as min_proc_1st_panel

from cohort
    inner join {{ source('cdw', 'or_log_all_procedures') }} as orl_proc on orl_proc.log_key = cohort.log_key
    inner join {{ source('cdw', 'or_log_surgeons') }} as orl_surgeon    on orl_surgeon.log_key = cohort.log_key

where orl_surgeon.panel_num = orl_proc.all_proc_panel_num

group by cohort.log_key
--endregion
), proc_primary as (
--region
select
    cohort.log_key,
    or_proc.or_proc_nm as proc_primary

from cohort
    inner join {{ source('cdw', 'or_log_all_procedures') }} as orl_proc on orl_proc.log_key = cohort.log_key
    inner join
        {{ source('cdw', 'or_procedure') }} as or_proc           on or_proc.or_proc_key = orl_proc.or_proc_key
    inner join {{ source('cdw', 'or_log_surgeons') }} as orl_surgeon    on orl_surgeon.log_key = cohort.log_key
inner join procs on procs.log_key = cohort.log_key

where
    orl_surgeon.panel_num = 1
    and orl_proc.seq_num = procs.min_proc_1st_panel
    and orl_surgeon.panel_num = orl_proc.all_proc_panel_num

group by
    cohort.log_key,
    or_proc.or_proc_nm
--endregion
), case_info as (
--region
select
    cohort.log_key,
    date(master_date.full_dt) as surgery_dt,
    dict_case_service.dict_nm as service,
    location.loc_nm as loc,
    room.full_nm as room,
    max(case when location.loc_id = 900100100
                    and room.prov_id not in ('0', -- unknown rooms (misdocumented procedures near optime go-live)
                                             '107', -- C-section OR
                                             -- Cardiac
                                             '144', '14611', '14612', '139', '134', '135', '143', '138', '142',
                                             '108', -- Fetal OR
                                             '461', -- NICU
                                             '460', -- PICU
                                             '938', '940') -- PACU
               then 1 else 0
               end) as main_4th_fl_or_ind,
    dict_case_type.dict_nm as case_type,
    dict_case_class.dict_nm as case_class,
    fact_or_log.first_case_ind,
    max(
        case when or_case.add_on_case_ind = 1 or or_case.add_on_case_sch_ind = 1 then 1 else 0 end
    ) as add_on_case_ind,
    max(
        case when location.loc_id in (900100110, 900100101, 900100102, 900100109, 900100103) then 1 else 0 end
    ) as asc_ind,
    max(case when dict_or_stat.src_id = 2 then 1 else 0 end) as posted_ind,
    or_log.num_of_panels as cnt_panels

from cohort
    inner join {{ source('cdw', 'or_log') }} as or_log                           on or_log.log_key = cohort.log_key
    inner join
        {{ source('cdw', 'or_case') }} as or_case                         on or_case.log_key = cohort.log_key
    inner join
        {{ source('cdw', 'master_date') }} as master_date                      on
            master_date.dt_key = or_log.surg_dt_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_case_service on
            dict_case_service.dict_key = or_log.dict_or_svc_key
    inner join
        {{ source('cdw', 'location') }} as location --noqa:L029
            on location.loc_key = or_log.loc_key
    inner join {{ source('cdw', 'provider') }} as room                    on room.prov_key = or_log.room_prov_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_case_type    on
            dict_case_type.dict_key = or_log.dict_or_case_type_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_case_class   on
            dict_case_class.dict_key = or_log.dict_or_case_class_key
    inner join
        {{ source('cdw', 'fact_or_log') }} as fact_or_log                      on
            fact_or_log.log_key = or_log.log_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_or_stat      on
            dict_or_stat.dict_key = or_log.dict_or_stat_key

group by
    cohort.log_key,
    master_date.full_dt,
    dict_case_service.dict_nm,
    location.loc_nm,
    room.full_nm,
    fact_or_log.first_case_ind,
    dict_case_type.dict_nm,
    dict_case_class.dict_nm,
    or_log.num_of_panels
--end region
)

select
    cohort.log_id,
    cohort.log_key,
    cohort.visit_key,
    cohort.vsi_key,
    visit.pat_key,
    case_info.surgery_dt,
    case_info.service,
    case_info.loc,
    case_info.room,
    case_info.main_4th_fl_or_ind,
    case_info.case_type,
    case_info.case_class,
    pat_class.orc_pat_class,
    pat_class.orl_pat_class,
    pat_class.har_pat_class,
    proc_primary.proc_primary,
    orc_surgeon.case_surgeon_primary,
    orl_surgeon.log_surgeon_primary,
    case_info.cnt_panels,
    procs.cnt_procs,
    case_info.first_case_ind,
    case_info.add_on_case_ind,
    case_info.asc_ind,
    case_info.posted_ind

from cohort
    inner join {{ source('cdw', 'visit') }} as visit
        on visit.visit_key = cohort.visit_key
    inner join case_info         on case_info.log_key = cohort.log_key
    inner join pat_class         on pat_class.log_key = cohort.log_key
    left join orc_surgeon  on orc_surgeon.log_key = cohort.log_key
    left join orl_surgeon  on orl_surgeon.log_key = cohort.log_key
    left join proc_primary on proc_primary.log_key = cohort.log_key
    inner join procs             on procs.log_key = cohort.log_key
