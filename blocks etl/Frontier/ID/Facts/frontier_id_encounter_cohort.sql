with
id_ip_data as (--region:
    select
        stg_encounter.visit_key,
        stg_encounter.mrn,
        note_edit_metadata_history.encounter_date as note_date,
        case
            when lower(note_edit_metadata_history.note_type) in (
                'consult note')
            then 1 else 0 end
        as id_ip_consult_ind,
        case
            when lower(note_edit_metadata_history.note_type) in (
                'progress notes')
            then 1 else 0 end
        as id_ip_progress_ind
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ ref('note_edit_metadata_history') }} as note_edit_metadata_history
        on stg_encounter.visit_key = note_edit_metadata_history.visit_key
    left join {{ ref('lookup_frontier_program_providers_all') }} as lookup_frontier_program_providers_all
        on lower(note_edit_metadata_history.version_author_provider_name)
            = cast(lookup_frontier_program_providers_all.provider_name as nvarchar(20))
            and lookup_frontier_program_providers_all.program = 'id'
    left join {{ source('cdw', 'note_text') }} as note_text
        on note_edit_metadata_history.note_visit_key = note_text.note_visit_key
    where
        (lookup_frontier_program_providers_all.end_date is null
            or (lookup_frontier_program_providers_all.end_date is not null
                and note_edit_metadata_history.encounter_date <= lookup_frontier_program_providers_all.end_date))
        and lower(lookup_frontier_program_providers_all.provider_type) like '%ip%'
        and note_edit_metadata_history.last_edit_ind = 1
        and lower(note_edit_metadata_history.note_type) in ('consult note', 'progress notes')
        and (lower(note_text.note_text) like '%dysregulated immunity%consult%'
            or lower(note_text.note_text) like '%immune dysregulation%'
            )
    group by
        stg_encounter.visit_key,
        stg_encounter.mrn,
        note_edit_metadata_history.encounter_date,
        note_type
    --end region
),
progress_note_count as (--region
    select
        mrn,
        sum(id_ip_progress_ind) as id_ip_progress_sum
    from id_ip_data
    group by
        mrn
    --end region
),
id_op_data as (--region
    select
        stg_encounter.visit_key,
        1 as id_multi_d_ind
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ ref('lookup_frontier_program_providers_all') }} as lookup_frontier_program_providers_all
        on provider.prov_id
            = cast(lookup_frontier_program_providers_all.provider_id as nvarchar(20))
            and lookup_frontier_program_providers_all.program = 'id'
            and lower(lookup_frontier_program_providers_all.provider_type) like '%op%'
    inner join {{ ref('lookup_frontier_program_departments')}} as lookup_fp_departments
        on stg_encounter.department_id = cast(lookup_fp_departments.department_id as nvarchar(20))
        and lookup_fp_departments.program = 'id'
        and lookup_fp_departments.active_ind = 1
    inner join {{ ref('lookup_frontier_program_visit')}} as lookup_fp_visit
        on stg_encounter.visit_type_id = cast(lookup_fp_visit.id as nvarchar(20))
        and lookup_fp_visit.program = 'id'
        and lookup_fp_visit.category = 'idfp visit'
        and lookup_fp_visit.active_ind = 1
    where
        (lookup_frontier_program_providers_all.end_date is null
            or (lookup_frontier_program_providers_all.end_date is not null
                and stg_encounter.encounter_date <= lookup_frontier_program_providers_all.end_date))
        and stg_encounter.appointment_status_id in (2, 6, 1)  --completed, arrived, scheduled
    --end region
),
cohort_build as (--region
    select
        stg_encounter.visit_key,
        stg_encounter.mrn,
        stg_encounter.csn,
        stg_encounter.patient_name,
        stg_encounter.encounter_date,
        id_ip_data.note_date,
        initcap(provider.full_nm) as provider_name,
        provider.prov_id as provider_id,
        stg_encounter.department_name,
        stg_encounter.department_id,
        stg_encounter.visit_type,
        stg_encounter.visit_type_id,
        stg_encounter.encounter_type,
        stg_encounter.encounter_type_id,
        coalesce(id_ip_data.id_ip_consult_ind, 0) as id_ip_consult_ind,
        progress_note_count.id_ip_progress_sum,
        coalesce(id_op_data.id_multi_d_ind, 0) as id_multi_d_ind,
        year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
        stg_encounter.hospital_admit_date,
        stg_encounter.hospital_discharge_date,
        stg_encounter.pat_key,
        coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join id_ip_data
        on stg_encounter.visit_key = id_ip_data.visit_key
    left join id_op_data
        on stg_encounter.visit_key = id_op_data.visit_key
    left join progress_note_count
        on stg_encounter.mrn = progress_note_count.mrn
    where
        id_ip_consult_ind = 1
        or id_multi_d_ind = 1
    order by
        patient_name,
        encounter_date
--end region
)
select * from cohort_build
