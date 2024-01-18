with fixed_log_surgeon as (
    select distinct --distinct is the fix here
        or_log_surgeons.log_key,
        or_log_surgeons.surg_prov_key as surgeon_prov_key,
        provider.full_nm as primary_surgeon,
        dict_or_svc.dict_nm as service,
        or_log_surgeons.panel_num
    from
        {{source('cdw', 'or_log_surgeons')}} as or_log_surgeons
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = or_log_surgeons.surg_prov_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_role
            on dict_or_role.dict_key = or_log_surgeons.dict_or_role_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_svc
            on dict_or_svc.dict_key = or_log_surgeons.dict_or_svc_key
    where
        dict_or_role.src_id in (1.0000, 1.0030) --primary surgeon
),

fixed_case_surgeon as (
    select distinct--distinct is the fix here
        or_case_all_surgeons.or_case_key,
        or_case_all_surgeons.surg_prov_key as surgeon_prov_key,
        provider.full_nm as primary_surgeon,
        dict_or_svc.dict_nm as service,
        or_case_all_surgeons.panel_num
    from
        {{source('cdw', 'or_case_all_surgeons')}} as or_case_all_surgeons
        inner join {{source('cdw', 'provider')}} as provider
            on provider.prov_key = or_case_all_surgeons.surg_prov_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_panel_role
            on dict_or_panel_role.dict_key = or_case_all_surgeons.dict_or_panel_role_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_svc
            on dict_or_svc.dict_key = or_case_all_surgeons.dict_or_svc_key
    where
        dict_or_panel_role.src_id in (1.0000, 1.0030) --primary surgeon
),

log_procedures as (
    select
        stg_surgery.log_key,
        row_number() over (
                partition by
                    stg_surgery.log_key
                order by
                    or_log_all_procedures.all_proc_panel_num,
                    or_log_all_procedures.seq_num
        ) as procedure_seq_num,
        or_procedure.or_proc_key,
        or_procedure.or_proc_id,
        or_procedure.or_proc_nm as or_procedure_name,
        dict_rpt_grp2.dict_nm as nhsn_category,
        or_procedure_cpt.cpt_cd as cpt_code,
        or_log_all_procedures.all_proc_panel_num as panel_number,
        fixed_log_surgeon.primary_surgeon,
        fixed_log_surgeon.surgeon_prov_key,
        fixed_log_surgeon.service,
        dict_wound_class.dict_nm as wound_class,
        dict_or_lrb.dict_nm as laterality
    from
        {{ref('stg_surgery')}} as stg_surgery
        inner join {{source('cdw', 'or_log_all_procedures')}} as or_log_all_procedures
            on or_log_all_procedures.log_key = stg_surgery.log_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_wound_class
            on dict_wound_class.dict_key = or_log_all_procedures.dict_wound_class_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_lrb
            on dict_or_lrb.dict_key = or_log_all_procedures.dict_or_lrb_key
        inner join {{source('cdw', 'or_procedure')}} as or_procedure
            on or_procedure.or_proc_key = or_log_all_procedures.or_proc_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_rpt_grp2
            on dict_rpt_grp2.dict_key = or_procedure.dict_rpt_grp2_key
        left join {{source('cdw', 'or_procedure_cpt')}} as or_procedure_cpt
            on or_procedure_cpt.or_proc_key = or_procedure.or_proc_key
                and or_procedure_cpt.seq_num = 1
        left join fixed_log_surgeon
            on fixed_log_surgeon.log_key = or_log_all_procedures.log_key
                and fixed_log_surgeon.panel_num = or_log_all_procedures.all_proc_panel_num
),

case_procedures as (
    select
        stg_surgery.case_key,
        row_number() over (
            partition by
                stg_surgery.case_key
            order by
                or_case_all_procedures.panel_num,
                or_case_all_procedures.seq_num
        ) as procedure_seq_num,
        or_procedure.or_proc_key,
        or_procedure.or_proc_id,
        or_procedure.or_proc_nm as or_procedure_name,
        dict_rpt_grp2.dict_nm as nhsn_category,
        or_procedure_cpt.cpt_cd as cpt_code,
        or_case_all_procedures.panel_num as panel_number,
        fixed_case_surgeon.primary_surgeon,
        fixed_case_surgeon.surgeon_prov_key,
        fixed_case_surgeon.service,
        null as wound_class,
        dict_or_lrb.dict_nm as laterality
    from
        {{ref('stg_surgery')}} as stg_surgery
        inner join {{source('cdw', 'or_case_all_procedures')}} as or_case_all_procedures
            on or_case_all_procedures.or_case_key = stg_surgery.case_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_or_lrb
            on dict_or_lrb.dict_key = or_case_all_procedures.dict_or_lrb_key
        inner join {{source('cdw', 'or_procedure')}} as or_procedure
            on or_procedure.or_proc_key = or_case_all_procedures.or_proc_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_rpt_grp2
            on dict_rpt_grp2.dict_key = or_procedure.dict_rpt_grp2_key
        left join {{source('cdw', 'or_procedure_cpt')}} as or_procedure_cpt
            on or_procedure_cpt.or_proc_key = or_procedure.or_proc_key
                and or_procedure_cpt.seq_num = 1
        left join fixed_case_surgeon
            on fixed_case_surgeon.or_case_key = or_case_all_procedures.or_case_key
                and fixed_case_surgeon.panel_num = or_case_all_procedures.panel_num
)

select
    stg_surgery.or_key,
    coalesce(log_procedures.procedure_seq_num, case_procedures.procedure_seq_num) as procedure_seq_num,
    stg_surgery.case_status,
    stg_surgery.patient_name,
    stg_surgery.mrn,
    stg_surgery.dob,
    stg_surgery.sex,
    stg_surgery.csn,
    stg_surgery.surgery_csn,
    stg_surgery.encounter_date,
    stg_surgery.hospital_discharge_date,
    stg_surgery.surgery_date,
    (date(stg_surgery.surgery_date) - date(stg_surgery.dob)) / 365.25 as surgery_age_years,
    coalesce(log_procedures.or_procedure_name, case_procedures.or_procedure_name) as or_procedure_name,
    coalesce(log_procedures.or_proc_id, case_procedures.or_proc_id) as or_proc_id,
    coalesce(log_procedures.cpt_code, case_procedures.cpt_code) as cpt_code,
    coalesce(log_procedures.panel_number, case_procedures.panel_number) as panel_number,
    coalesce(log_procedures.primary_surgeon, case_procedures.primary_surgeon) as primary_surgeon,
    coalesce(log_procedures.service, case_procedures.service) as service,
    coalesce(log_procedures.wound_class, case_procedures.wound_class) as wound_class,
    coalesce(log_procedures.laterality, case_procedures.laterality) as laterality,
    coalesce(log_procedures.nhsn_category, case_procedures.nhsn_category) as nhsn_category,
    coalesce(log_procedures.or_proc_key, case_procedures.or_proc_key) as or_proc_key,
    coalesce(log_procedures.surgeon_prov_key, case_procedures.surgeon_prov_key) as surgeon_prov_key,
    stg_surgery.source_system,
    /* TODO: this column was added for #719, but should not exist here, and will be removed
    (along with the join to `surgery_encounter_timestamps`) at a later date */
    surgery_encounter_timestamps.post_op_los_days,
    stg_surgery.case_id,
    stg_surgery.log_id,
    stg_surgery.case_key,
    stg_surgery.log_key,
    stg_surgery.pat_key,
    stg_surgery.hsp_acct_key,
    stg_surgery.visit_key
from
    {{ref('stg_surgery')}} as stg_surgery
    left join log_procedures
        on log_procedures.log_key = stg_surgery.log_key
            and stg_surgery.case_status = 'Completed'
    left join case_procedures
        on case_procedures.case_key = stg_surgery.case_key
            and stg_surgery.case_status = 'Scheduled'
    left join {{ ref('surgery_encounter_timestamps') }} as surgery_encounter_timestamps
        on surgery_encounter_timestamps.log_key = stg_surgery.log_key
where
    coalesce(case_procedures.case_key, log_procedures.log_key) is not null
