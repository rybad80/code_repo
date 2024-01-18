with registry as (
    select
        registry_data_membership.pat_key,
        dim_registry_status.registry_status_nm as registry_status,
        dim_registry_status.registry_status_id,
        registry_data_membership.first_include_dttm as registry_added_date,
        registry_data_membership.last_update_dttm as registry_updated_date
    from
        {{source('cdw', 'registry_data_membership')}} as registry_data_membership
        inner join {{source('cdw', 'registry_configuration')}} as registry_configuration
            on registry_data_membership.registry_config_key = registry_configuration.registry_config_key
        inner join {{source('cdw', 'dim_registry_status')}} as dim_registry_status
            on registry_data_membership.dim_registry_status_key = dim_registry_status.dim_registry_status_key
    where
        registry_configuration.registry_id = 100120 --'chop ortho ctis patient registry'
),
ctis_dx as (
    select
        diagnosis_encounter_all.pat_key,
        min(diagnosis_encounter_all.encounter_date)
            as first_noted_date,
        max(case when epic_grouper_item.epic_grouper_id = 108611 then 1 else 0 end)
            as ctis_grouper,
        max(case when epic_grouper_item.epic_grouper_id = 108595 then 1 else 0 end)
            as thoracic_insufficiency_syndrome_ind,
        max(case when epic_grouper_item.epic_grouper_id = 110778 then 1 else 0 end)
            as congenital_scoliosis_ind,
        max(case when epic_grouper_item.epic_grouper_id = 108600 then 1 else 0 end)
            as neuromuscular_scoliosis_ind,
        max(case when epic_grouper_item.epic_grouper_id = 108603 then 1 else 0 end)
            as syndromic_scoliosis_ind,
        max(case when epic_grouper_item.epic_grouper_id = 108598 then 1 else 0 end)
            as infantile_scoliosis_ind,
        max(case when epic_grouper_item.epic_grouper_id = 110774 then 1 else 0 end)
            as juvenile_scoliosis_ind,
        max(case when epic_grouper_item.epic_grouper_id = 108605 then 1 else 0 end)
            as congenital_rib_or_spine_ind,
        max(case when epic_grouper_item.epic_grouper_id = 110776 then 1 else 0 end)
            as growing_rod_adjustment_ind,
        case
            when congenital_scoliosis_ind = 1 then 'Congenital'
            when neuromuscular_scoliosis_ind = 1 then 'Neuromuscular'
            when syndromic_scoliosis_ind = 1 then 'Syndromic'
            when (infantile_scoliosis_ind + juvenile_scoliosis_ind) > 0 then 'Idiopathic'
        end as ctis_category
    from
        {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        left join {{source('cdw', 'epic_grouper_diagnosis')}} as epic_grouper_diagnosis
            on epic_grouper_diagnosis.dx_key = diagnosis_encounter_all.dx_key
        left join {{source('cdw', 'epic_grouper_item')}} as epic_grouper_item
            on epic_grouper_item.epic_grouper_key = epic_grouper_diagnosis.epic_grouper_key
    where
        epic_grouper_item.epic_grouper_id in (
                108595, --'chop edg con ortho thoracic insufficiency syndrome'
                108598, --'chop edg con ortho infantile scoliosis',
                108600, --'chop edg con ortho neuromuscular scoliosis',
                108603, --'chop edg con ortho syndromic scoliosis',
                108605, --'chop edg con ortho congenital rib or spine',
                108611, --'chop edg ortho ctis grouper',
                110774, --'chop edg icd10 ortho juvenile scoliosis',
                110776, --'chop edg icd10 ortho growing rod adjustment',
                110778  --'chop edg icd10 ortho congenital scoliosis',
        )
    group by
        diagnosis_encounter_all.pat_key
),
other_diagnoses as (
    select
        pat_key,
        max(case when lower(icd10_code) in ('g12.0', 'g12.1', 'g12.8', 'g12.9') then 1 else 0 end)
            as sma_ind,
        max(case when lower(icd10_code) not in ('g12.0', 'g12.1', 'g12.8', 'g12.9') then 1 else 0 end)
            as developmental_delay_ind
    from
        {{ ref('diagnosis_encounter_all') }}
    where
        lower(icd10_code) in (
                            'f80.1',   --expressive language disorder
                            'f80.2',   --mixed receptive-expressive language disorder,
                            'f80.4',   --speech and language development delay due to hearing loss,
                            'f80.89',  --other developmental disorders of speech and language,
                            'f80.9',   --developmental disorder of speech and language, unspecified,
                            'f81.89',  --other developmental disorders of scholastic skills,
                            'f81.9',   --developmental disorder of scholastic skills, unspecified,
                            'f82',     --specific developmental disorder of motor function,
                            'f84.0',   --autistic disorder,
                            'f84.2',   --rett's syndrome,
                            'f84.3',   --other childhood disintegrative disorder,
                            'f84.5',   --asperger's syndrome,
                            'f84.8',   --other pervasive developmental disorders,
                            'f84.9',   --pervasive developmental disorder, unspecified,
                            'f89',     --unspecified disorder of psychological development,
                            'g12.0',   -- spinal muscular atrophy
                            'g12.1',   -- spinal muscular atrophy
                            'g12.8',   -- spinal muscular atrophy
                            'g12.9'    -- spinal muscular atrophy
            )
    group by pat_key
),
patient_history as (
    select
        registry.pat_key,
        min(stg_encounter.encounter_date) as earliest_chop_date,
        min(
            case
                when lower(department.specialty) like '%orthop%'
                    or (lower(surgery_procedure.service) = 'orthopedics'
                        and surgery_procedure.case_status = 'Completed')
                then stg_encounter.encounter_date
            end
        ) as earliest_ortho_date,
        extract(epoch from earliest_ortho_date - earliest_chop_date) as date_dist,
        case when date_dist <= 30 then 1 else 0 end as ortho_credit_ind
    from
        {{ ref('stg_encounter') }} as stg_encounter
        left join {{source('cdw', 'department')}} as department
            on department.dept_key = stg_encounter.dept_key
        left join {{ ref('surgery_procedure') }} as surgery_procedure
            on surgery_procedure.pat_key = stg_encounter.pat_key
            and surgery_procedure.surgery_date
                between stg_encounter.hospital_admit_date
                    and stg_encounter.hospital_discharge_date
        inner join registry
            on registry.pat_key = stg_encounter.pat_key
    group by
        registry.pat_key
)
select
    stg_patient.patient_key,
    stg_patient.pat_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.sex,
    stg_patient.current_age,
    stg_patient.race_ethnicity,
    registry.registry_status,
    registry.registry_status_id,
    registry.registry_added_date,
    registry.registry_updated_date,
    ctis_dx.first_noted_date,
    ctis_dx.ctis_category,
    patient_history.earliest_chop_date,
    patient_history.earliest_ortho_date,
    patient_history.ortho_credit_ind,
    coalesce(other_diagnoses.developmental_delay_ind, 0) as developmental_delay_ind,
    ctis_dx.thoracic_insufficiency_syndrome_ind,
    ctis_dx.congenital_scoliosis_ind,
    ctis_dx.neuromuscular_scoliosis_ind,
    ctis_dx.syndromic_scoliosis_ind,
    ctis_dx.infantile_scoliosis_ind,
    ctis_dx.juvenile_scoliosis_ind,
    ctis_dx.congenital_rib_or_spine_ind,
    ctis_dx.growing_rod_adjustment_ind,
    coalesce(other_diagnoses.sma_ind, 0) as sma_ind
from
    registry
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = registry.pat_key
    inner join ctis_dx
        on ctis_dx.pat_key = registry.pat_key
    left join other_diagnoses
        on other_diagnoses.pat_key = registry.pat_key
    left join patient_history
        on patient_history.pat_key = registry.pat_key
