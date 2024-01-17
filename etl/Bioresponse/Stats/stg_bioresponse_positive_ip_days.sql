with positive_ip as ( -- noqa: PRS
    select
        bioresponse_encounters_with_positives.diagnosis_hierarchy_1,
        capacity_ip_midnight_census.patient_key,
        capacity_ip_midnight_census.encounter_key,
        capacity_ip_midnight_census.midnight_date as census_date,
        capacity_ip_midnight_census.department_name,
        case
            when capacity_ip_midnight_census.department_center_abbr like 'KOPH%'
            then 'KOP' else 'PHL'
        end as campus,
        case
            when capacity_ip_midnight_census.bed_care_group like '%ICU%'
            then 'IP - ICU'
            else 'IP - Med/Surg'
        end as unit_type,
        1 as positive_ind
    from
        {{ ref('bioresponse_encounters_with_positives') }} as bioresponse_encounters_with_positives
        inner join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
            on stg_encounter_inpatient.encounter_key = bioresponse_encounters_with_positives.encounter_key
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on stg_encounter.encounter_key = bioresponse_encounters_with_positives.encounter_key
        inner join {{ ref('capacity_ip_midnight_census') }} as capacity_ip_midnight_census
            on stg_encounter_inpatient.encounter_key = capacity_ip_midnight_census.encounter_key
    where
        capacity_ip_midnight_census.midnight_date >= {{ var('start_data_date') }}
        and capacity_ip_midnight_census.midnight_date >= episode_start_date
        and capacity_ip_midnight_census.midnight_date <= min(
            coalesce(stg_encounter.hospital_discharge_date, current_date),
            episode_end_date
        )
)

select
    positive_ip.campus,
    positive_ip.diagnosis_hierarchy_1,
    positive_ip.census_date as stat_date,
    sum(positive_ip.positive_ind) as stat_numerator_val
from
    positive_ip
group by
    positive_ip.campus,
    positive_ip.diagnosis_hierarchy_1,
    positive_ip.census_date
