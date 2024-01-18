with
relevant_procs as (
    select
        surgery_department_or_case_procedure.*,
		case
			when surgery_department_or_case_procedure.or_proc_id = '9400' then 'instrumentation - spine'
			when ortho_surgery_scoliosis_procedures.scoliosis_category is not null
            then ortho_surgery_scoliosis_procedures.scoliosis_category
			when lower(surgery_department_or_case_procedure.or_procedure_name) like '%halo%' then 'halo'
			else 'i and d'
            end as scoliosis_category
    from
        {{ ref('surgery_department_or_case_procedure') }} as surgery_department_or_case_procedure
        inner join {{ ref('ctis_registry') }} as ctis_registry
            on ctis_registry.pat_key = surgery_department_or_case_procedure.pat_key
        left join {{ ref('ortho_surgery_scoliosis_procedures') }} as ortho_surgery_scoliosis_procedures
            on ortho_surgery_scoliosis_procedures.or_key = surgery_department_or_case_procedure.or_key
            and ortho_surgery_scoliosis_procedures.or_proc_id = surgery_department_or_case_procedure.or_proc_id
        left join {{ source('cdw', 'or_log_all_procedures') }} as or_log_all_procedures
            on or_log_all_procedures.log_key = surgery_department_or_case_procedure.log_key
            and or_log_all_procedures.all_proc_panel_num = surgery_department_or_case_procedure.panel_number
            and or_log_all_procedures.seq_num = surgery_department_or_case_procedure.procedure_seq_num
            and or_log_all_procedures.or_proc_key = surgery_department_or_case_procedure.or_proc_key
        left join {{ source('cdw', 'cdw_dictionary')}}  as or_region
            on or_region.dict_key = or_log_all_procedures.dict_or_rgn_key
    where
        (
            lower(surgery_department_or_case_procedure.service) = 'orthopedics'
            and (
                ortho_surgery_scoliosis_procedures.or_key is not null
                or lower(surgery_department_or_case_procedure.or_procedure_name) like '%halo%'
            )
        )
        or (
            lower(surgery_department_or_case_procedure.service) in ('orthopedics', 'plastic surgery')
            and or_region.src_id in (
                541.0030, 917.0000,     -- 'chest'
                546.0030, 1005.0000,  -- 'spine'
                909.0000, 1495.0030,  -- 'back'
                997.0000             -- 'rib'
            )
            and (
                lower(surgery_department_or_case_procedure.or_procedure_name) like '%i&d%'
                or lower(surgery_department_or_case_procedure.or_procedure_name) like '%drainage%'
                or lower(surgery_department_or_case_procedure.or_procedure_name) like '%drainage%'
                or lower(surgery_department_or_case_procedure.or_procedure_name) like '%debrid%'
                or lower(surgery_department_or_case_procedure.or_procedure_name) like '%dehiscence%'
                or lower(surgery_department_or_case_procedure.or_procedure_name) like '%abscess%'
                or lower(surgery_department_or_case_procedure.or_procedure_name) like '%bursa%'
                or lower(surgery_department_or_case_procedure.or_procedure_name) like '%dressing appl%'
            )
        )
),

all_procs as (
        select
            *,
            case when laterality = 'bilateral' then 'left' else laterality end as final_laterality,
            case when laterality = 'bilateral' then 1 else 0 end as bilateral_ind
        from
            relevant_procs
        where
            laterality = 'bilateral'
	union
        select
            *,
            case when laterality = 'bilateral' then 'right' else laterality end as final_laterality,
            case when laterality = 'bilateral' then 1 else 0 end as bilateral_ind
        from
            relevant_procs
)

select
    {{
        dbt_utils.surrogate_key([
            'all_procs.or_key',
            'all_procs.panel_number',
            'all_procs.procedure_seq_num',
            'all_procs.final_laterality'
        ])
    }} as primary_key,
    all_procs.patient_name,
    all_procs.mrn,
    all_procs.dob,
    all_procs.csn,
    all_procs.surgery_age_years,
    all_procs.encounter_date,
    all_procs.case_status,
    all_procs.surgery_date,
    all_procs.panel_number,
    all_procs.primary_surgeon,
    all_procs.primary_surgeon_provider_id,
    all_procs.procedure_seq_num,
    all_procs.or_proc_id,
    all_procs.cpt_code,
    all_procs.or_procedure_name,
    all_procs.scoliosis_category,
    row_number() over(
        partition by all_procs.pat_key, all_procs.scoliosis_category, all_procs.final_laterality
        order by all_procs.surgery_date, all_procs.panel_number, all_procs.procedure_seq_num
      ) as category_side_order,
    all_procs.final_laterality as laterality,
    all_procs.bilateral_ind,
    all_procs.log_id,
    all_procs.patient_class,
    all_procs.panel_start_time,
    all_procs.panel_end_time,
    all_procs.panel_length_minutes,
    all_procs.surgery_placed_date,
    all_procs.fiscal_year,
    master_date.fy_yyyy_qtr as fiscal_quarter,
    all_procs.calendar_year,
    all_procs.calendar_month,
    all_procs.or_key,
    all_procs.case_key,
    all_procs.log_key,
    all_procs.pat_key,
    all_procs.hsp_acct_key,
    all_procs.visit_key,
    all_procs.or_proc_key,
    all_procs.surgeon_prov_key,
    all_procs.source_system
from
    all_procs
    left join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.visit_key = all_procs.visit_key
    left join {{ source('cdw', 'master_date') }} as master_date
        on master_date.full_dt = all_procs.surgery_date
