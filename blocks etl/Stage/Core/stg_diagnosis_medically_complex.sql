{{ config(meta = {
    'critical': true
}) }}

with complex_visits as (
    select
        stg_encounter.visit_key,
        stg_encounter.pat_key,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'TECH DEPENDENT' then 1 else 0 end
        ) as tech_dependent_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'HEMATU' then 1 else 0 end
        ) as hematu_ccc_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'RENAL' then 1 else 0 end
        ) as renal_ccc_ind,
        max(case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'GI' then 1 else 0 end) as gi_ccc_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'MALIGNANCY' then 1 else 0 end
        ) as malignancy_ccc_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'METABOLIC' then 1 else 0 end
        ) as metabolic_ccc_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'NEONATAL' then 1 else 0 end
        ) as neonatal_ccc_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'CONGENI GENETIC' then 1 else 0 end
        ) as congeni_genetic_ccc_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'RESP' then 1 else 0 end
        ) as resp_ccc_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'CVD' then 1 else 0 end
        ) as cvd_ccc_ind,
        max(
            case when stg_diagnosis_medically_complex_timeframe.ccc_group = 'NEUROMUSC' then 1 else 0 end
        ) as neuromusc_ccc_ind,
        hematu_ccc_ind
            + renal_ccc_ind
            + gi_ccc_ind
            + malignancy_ccc_ind
            + metabolic_ccc_ind
            + neonatal_ccc_ind
            + congeni_genetic_ccc_ind
            + resp_ccc_ind
            + cvd_ccc_ind
            + neuromusc_ccc_ind as sum_of_ccc_cat,
        case when sum_of_ccc_cat >= 1 then 1 else 0 end as complex_chronic_condition_ind,
        case
            when sum_of_ccc_cat >= 2 then 1
            when sum_of_ccc_cat >= 1 and tech_dependent_ind = 1 then 1
            else 0
        end as medically_complex_ind
    from
        {{ref('stg_diagnosis_medically_complex_timeframe')}} as stg_diagnosis_medically_complex_timeframe
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.pat_key = stg_diagnosis_medically_complex_timeframe.pat_key
    where
        stg_encounter.encounter_date between
        stg_diagnosis_medically_complex_timeframe.start_date
        and stg_diagnosis_medically_complex_timeframe.end_date
    group by
        stg_encounter.visit_key,
        stg_encounter.pat_key
)

select
    stg_encounter.visit_key,
    stg_encounter.encounter_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    coalesce(complex_visits.tech_dependent_ind, 0) as tech_dependent_ind,
    coalesce(complex_visits.hematu_ccc_ind, 0) as hematu_ccc_ind,
    coalesce(complex_visits.renal_ccc_ind, 0) as renal_ccc_ind,
    coalesce(complex_visits.gi_ccc_ind, 0) as gi_ccc_ind,
    coalesce(complex_visits.malignancy_ccc_ind, 0) as malignancy_ccc_ind,
    coalesce(complex_visits.metabolic_ccc_ind, 0) as metabolic_ccc_ind,
    coalesce(complex_visits.neonatal_ccc_ind, 0) as neonatal_ccc_ind,
    coalesce(complex_visits.congeni_genetic_ccc_ind, 0) as congeni_genetic_ccc_ind,
    coalesce(complex_visits.resp_ccc_ind, 0) as resp_ccc_ind,
    coalesce(complex_visits.cvd_ccc_ind, 0) as cvd_ccc_ind,
    coalesce(complex_visits.neuromusc_ccc_ind, 0) as neuromusc_ccc_ind,
    coalesce(complex_visits.complex_chronic_condition_ind, 0) as complex_chronic_condition_ind,
    coalesce(complex_visits.medically_complex_ind, 0) as medically_complex_ind,
    stg_encounter.pat_key
from
    {{ref('stg_encounter')}} as stg_encounter
    left join complex_visits on complex_visits.visit_key = stg_encounter.visit_key
