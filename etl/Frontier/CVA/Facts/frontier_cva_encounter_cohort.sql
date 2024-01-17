with
note_data as (--region
    select
        visit_key,
        encounter_date,
        note_type
    from
        {{ ref('frontier_cva_note_data') }}
    --end region
),
ip_encounter_data as (--region:
    select
        stg_encounter.visit_key,
        note_data.encounter_date as note_date,
        max(case
            when lower(note_data.note_type) = 'consult note'
            then 1 else 0 end)
        as cva_ip_consult_ind,
        max(case
            when lower(note_data.note_type) = 'progress notes'
            then 1 else 0 end)
        as cva_ip_progress_ind
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join note_data
        on stg_encounter.visit_key = note_data.visit_key
    where lower(note_data.note_type) in ('consult note', 'progress notes')
    group by
        stg_encounter.visit_key,
        note_data.encounter_date
    --end region
),
op_encounter_data as (--region:
    select
        stg_encounter.visit_key,
        max(case
            when stg_encounter.department_id = '101012176' --bgr cvap multi d pgm
            then 1 else 0 end)
        as cva_multi_d_ind,
        max(case
            when stg_encounter.department_id = '101001118' --bgr oncology day hosp
            then 1 else 0 end)
        as cva_onco_day_ind
    from {{ ref('stg_encounter') }} as stg_encounter
    left join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    where
        stg_encounter.department_id = '101012176'     -- bgr cvap multi d pgm
        or (stg_encounter.department_id = '101001118' -- bgr oncology day hosp
            and provider.prov_id in (
                            '660301',   -- 'adams, denise m'
                            '29133',    -- 'snyder, kristen'
                            '10666',    -- 'borst, alexandra'
                            '664492',   -- 'fox, michael d'
                            '45704'     -- 'cohen-cutler, sally'
                            )
            )
    group by stg_encounter.visit_key
    --end region
),
cohort_build as (--region:
    select
        stg_encounter.visit_key,
        stg_encounter.mrn,
        stg_encounter.csn,
        stg_encounter.patient_name,
        stg_encounter.encounter_date,
        initcap(provider.full_nm) as provider_name,
        provider.prov_id as provider_id,
        stg_encounter.department_name,
        stg_encounter.department_id,
        stg_encounter.visit_type,
        stg_encounter.visit_type_id,
        stg_encounter.encounter_type,
        stg_encounter.encounter_type_id,
        ip_encounter_data.note_date,
        case
            when stg_encounter_inpatient.visit_key is not null
            then 1
            else 0
        end as inpatient_ind,
        coalesce(ip_encounter_data.cva_ip_consult_ind, 0) as cva_ip_consult_ind,
        coalesce(ip_encounter_data.cva_ip_progress_ind, 0) as cva_ip_progress_ind,
        coalesce(op_encounter_data.cva_multi_d_ind, 0) as cva_multi_d_ind,
        coalesce(op_encounter_data.cva_onco_day_ind, 0) as cva_onco_day_ind,
        stg_encounter.hospital_admit_date,
        stg_encounter.hospital_discharge_date,
        year(add_months(stg_encounter.encounter_date, 6)) as fiscal_year,
        stg_encounter.pat_key,
        coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
        on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    left join {{ref('stg_encounter_inpatient')}} as stg_encounter_inpatient
        on stg_encounter_inpatient.visit_key = stg_encounter.visit_key
    left join ip_encounter_data
        on stg_encounter.visit_key = ip_encounter_data.visit_key
    left join op_encounter_data
        on stg_encounter.visit_key = op_encounter_data.visit_key
    where
        cva_ip_consult_ind + cva_ip_progress_ind > 0
        or cva_multi_d_ind + cva_onco_day_ind > 0
    order by
        patient_name,
        encounter_date
    --end region
)
select * from cohort_build
