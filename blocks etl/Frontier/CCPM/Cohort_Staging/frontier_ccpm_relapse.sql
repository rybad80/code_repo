with
neuro_group as (
    select
        mrn,
        'Neuro' as rr_group_label,
        'cancer_center_brain_tumor_diagnosis' as rr_data_source,
        date(diagnosis_relapse_date) as rr_date
    from
        {{ ref('cancer_center_brain_tumor_diagnosis') }}
    where
        lower(diagnosis_type) in ('relapse', 'refractory/progressive')
    group by
        mrn,
        date(diagnosis_relapse_date)
),
registry_group as (
    select
        stg_patient.mrn,
        lookup_frontier_program_definitions.group_label as rr_group_label,
        'registry_tumor_oncology' as rr_data_source,
        date(registry_tumor_oncology.date_recur) as rr_date
    from
        {{ source('cdw', 'registry_tumor_oncology') }} as registry_tumor_oncology
        inner join {{ ref('stg_patient') }} as stg_patient
            on registry_tumor_oncology.pat_key = stg_patient.pat_key
        inner join {{ ref('lookup_frontier_program_definitions') }} as lookup_frontier_program_definitions
            on registry_tumor_oncology.onco_general_dx_cd = lookup_frontier_program_definitions.code
    where
        registry_tumor_oncology.date_recur is not null
        and registry_tumor_oncology.icdo_histology_behavior_cd like '%/3' --indicates malignant disease
    group by
        stg_patient.mrn,
        lookup_frontier_program_definitions.group_label,
        date(registry_tumor_oncology.date_recur)
),
sde_group as (
    select
        smart_data_element_all.mrn,
        lookup_frontier_program_definitions.group_label as rr_group_label,
        'smart_data_element_all' as rr_data_source,
        date(smart_data_element_all.entered_date) as rr_date
    from
        {{ ref('smart_data_element_all') }} as smart_data_element_all
        inner join {{ ref('lookup_frontier_program_definitions') }} as lookup_frontier_program_definitions
            on lookup_frontier_program_definitions.epic_source_location
                = smart_data_element_all.epic_source_location
    where
        lower(smart_data_element_all.concept_id) like 'choponc%'
        and (
            lower(smart_data_element_all.concept_description) like '%refractory%'
            or lower(smart_data_element_all.concept_description) like '%relapse%'
            )
    group by
        smart_data_element_all.mrn,
        lookup_frontier_program_definitions.group_label,
        date(smart_data_element_all.entered_date)
),
union_all as (
    select *
    from neuro_group
    union all
    select *
    from registry_group
    union all
    select *
    from sde_group
),
dx_count as (
    select
        *,
        row_number() over(
            partition by mrn
            order by rr_date)
        as dx_count
    from union_all
)
select
    mrn,
    rr_group_label,
    min(rr_date) as min_rr_date,
    min(case
        when rr_date >= '2021-01-01' then rr_date end)
    as cy21_initial_date,
    min(case
        when rr_date >= '2022-07-01' then rr_date end)
    as fy23_initial_date,
    max(dx_count) as max_dx_count,
    1 as relapse_refractory_ind
from dx_count
group by
    mrn,
    rr_group_label
having cy21_initial_date is not null
