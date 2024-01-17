with smartphrases as (--region
    select
        smart_data_element_all.pat_key,
        max(case when concept_id = 'CHOPNEPHRO#004'
            then cast(element_value as varchar(100))
            else null end) as phenotype,
        max(case when concept_id = 'CHOPNEPHRO#007'
            then cast('1840-12-31' as date) + cast(element_value as int)
            else null end) as kidney_biopsy_date,
        max(case when concept_id = 'CHOPNEPHRO#008'
            then cast(element_value as varchar(100))
            else null end) as kidney_biopsy_result,
        max(case when concept_id = 'CHOPNEPHRO#010'
            then cast(element_value as varchar(100))
            else null end) as genetic_testing_performed,
        max(case when concept_id = 'CHOPNEPHRO#052'
            then cast(element_value as varchar(100))
            else null end) as imm_rec_rev,
        max(case when concept_id = 'CHOPNEPHRO#053'
            then cast(element_value as varchar(100))
            else null end) as tb,
        max(case when concept_id = 'CHOPNEPHRO#055'
            then cast(element_value as varchar(100))
            else null end) as rd_counseling,
        max(case when concept_id = 'CHOPNEPHRO#056'
            then cast(element_value as varchar(100))
            else null end) as patient_family_education
    from
        {{ ref('smart_data_element_all') }} as smart_data_element_all
        inner join (select
                        pat_key,
                        sde_key,
                        max(seq_num) as seq
                    from
                        {{ ref('smart_data_element_all') }}
                    group by
                    pat_key,
                    sde_key) as max_seq --getting msot recent sde per sde per patient
            on smart_data_element_all.pat_key = max_seq.pat_key
            and smart_data_element_all.sde_key = max_seq.sde_key
            and smart_data_element_all.seq_num = max_seq.seq
        inner join {{ ref('stg_glean_nephrology_cohort_metrics')}} as cohort_metrics
            on smart_data_element_all.pat_key = cohort_metrics.pat_key
    where
        concept_id in (
        'CHOPNEPHRO#004',
        'CHOPNEPHRO#007',
        'CHOPNEPHRO#008',
        'CHOPNEPHRO#010',
        'CHOPNEPHRO#052',
        'CHOPNEPHRO#053',
        'CHOPNEPHRO#055',
        'CHOPNEPHRO#056'
    )
    group by
        smart_data_element_all.pat_key
    --end region
)

select
    *
from
    smartphrases
