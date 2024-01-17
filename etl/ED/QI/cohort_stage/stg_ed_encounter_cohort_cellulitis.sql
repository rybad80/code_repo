with cohort as (
    select
        visit_key,
        pat_key,
        ed_arrival_date,
        ed_discharge_date,
        billing_dx_primary_icd10
    from
        {{ ref('stg_encounter_ed') }}
    where
        ed_patients_seen_ind = 1
        and year(ed_arrival_date) >= year(current_date) - 5 --ED QI Standard
        and ed_discharge_date is not null
        and age_years <= 21
        and age_days > 56
),
cellulitis_abscess_dx as (
    select
        cohort.visit_key,
        cohort.pat_key,
        max(case
            when lower(billing_diagnosis.diagnosis_name) like '%cellulitis%'
                or lower(billing_diagnosis.diagnosis_name) like '%phlegmon%'
                or lower(billing_diagnosis.diagnosis_name) like 'bacterial skin infection of leg'
            then 1 else 0 end) as cellulitis_ind,
        max(case
            when lower(billing_diagnosis.diagnosis_name) like '%abscess%'
                or lower(billing_diagnosis.diagnosis_name) like '%carbunc%'
                or lower(billing_diagnosis.diagnosis_name) like '%furunc%'
                or lower(billing_diagnosis.diagnosis_name) like '%forunc%'
                or lower(billing_diagnosis.diagnosis_name) like '%boil%'
                or lower(billing_diagnosis.diagnosis_name) like 'deep folliculitis'
                or lower(billing_diagnosis.diagnosis_name) like 'diffuse infection'
            then 1 else 0 end) as abscess_ind
    from
        cohort
        inner join {{ ref('diagnosis_encounter_all') }} as billing_diagnosis
            on cohort.visit_key = billing_diagnosis.visit_key
            and cohort.billing_dx_primary_icd10 = billing_diagnosis.icd10_code
    where
        lower(billing_diagnosis.icd10_code) in ( -- list of codes in which all dx names are included
            -- region L02 icd-10 codes - cutaneous abscess, furuncle and carbuncle
            'l02.21',
            'l02.211',
            'l02.212',
            'l02.213',
            'l02.214',
            'l02.219',
            'l02.221',
            'l02.222',
            'l02.223',
            'l02.224',
            'l02.229',
            'l02.231',
            'l02.232',
            'l02.233',
            'l02.234',
            'l02.239',
            'l02.31',
            'l02.32',
            'l02.33',
            'l02.41',
            'l02.411',
            'l02.412',
            'l02.413',
            'l02.414',
            'l02.415',
            'l02.416',
            'l02.419',
            'l02.421',
            'l02.422',
            'l02.423',
            'l02.424',
            'l02.425',
            'l02.426',
            'l02.429',
            'l02.431',
            'l02.432',
            'l02.433',
            'l02.434',
            'l02.435',
            'l02.436',
            'l02.439',
            'l02.838',
            'l02.92',
            'l02.93',
            -- end region
            -- region L03 icd-10 codes - cellulitis & acute lymphangitis
            'l03.11',
            'l03.311',
            'l03.312',
            'l03.313',
            'l03.314',
            'l03.317',
            'l03.319'
            -- end region
        )
        --region inclusion of specific icd10 codes with diagnosis name exclusions,
        --that unfortunately don't nicely fit in a regex
        or (
            lower(billing_diagnosis.icd10_code) = 'l02.91'
            and lower(billing_diagnosis.diagnosis_name) != 'abscess of skin with lymphangitis'
        )
        or (
            lower(billing_diagnosis.icd10_code) = 'l03.113'
            and lower(billing_diagnosis.diagnosis_name) not in (
                'cellulitis of hand, right',
                'cellulitis of right hand',
                'cellulitis of right hand excluding fingers and thumb'
            )
        )
        or (
            lower(billing_diagnosis.icd10_code) = 'l03.114'
            and lower(billing_diagnosis.diagnosis_name) not in (
                'cellulitis of hand, left',
                'cellulitis of left hand',
                'cellulitis of left hand excluding fingers and thumb'
            )
        )
        or (
            lower(billing_diagnosis.icd10_code) = 'l03.115'
            and lower(billing_diagnosis.diagnosis_name) not in (
                'cellulitis of both feet',
                'cellulitis of foot without toes, right',
                'cellulitis of foot, right',
                'cellulitis of right foot',
                'cellulitis of right foot due to methicillin-resistant staphylococcus aureus',
                'cellulitis of right foot without toes',
                'mrsa cellulitis of right foot'
            )
        )
        or (
            lower(billing_diagnosis.icd10_code) = 'l03.116'
            and lower(billing_diagnosis.diagnosis_name) not in (
                'cellulitis of foot without toes, left',
                'cellulitis of foot, left',
                'cellulitis of left foot',
                'cellulitis of left foot excluding toes',
                'mrsa cellulitis of left foot'
            )
        )
        or (
            lower(billing_diagnosis.icd10_code) = 'l03.119'
            and lower(billing_diagnosis.diagnosis_name) not in (
                'cellulitis of foot',
                'cellulitis of foot excluding toe',
                'cellulitis of foot without toes, left',
                'cellulitis of foot, left',
                'cellulitis of left foot',
                'cellulitis of left foot excluding toes',
                'abscess or cellulitis of foot',
                'cellulitis and abscess of hand, except fingers and thumb',
                'cellulitis of hand excluding fingers',
                'cellulitis of hand without fingers and thumb',
                'cellulitis of multiple sites of hand and fingers',
                'cellulitis of palm of hand',
                'cellulitis and abscess of foot excluding toe',
                'cellulitis and abscess of foot, except toes',
                'cellulitis and abscess of hand',
                'cellulitis of hand',
                'cellulitis of plantar aspect of foot',
                'cellulitis and abscess of foot'
            )
        )
        or (
            lower(billing_diagnosis.icd10_code) = 'l03.90'
            and lower(billing_diagnosis.diagnosis_name) not in (
                'cellulitis and abscess of foot',
                'cellulitis and abscess of foot excluding toe',
                'cellulitis and abscess of foot, except toes',
                'cellulitis and abscess of hand',
                'cellulitis and abscess of hand, except fingers and thumb',
                'cellulitis of digit',
                'cellulitis of hand',
                'cellulitis of hand excluding fingers',
                'cellulitis of hand without fingers and thumb',
                'cellulitis of multiple sites of hand and fingers',
                'cellulitis of palm of hand',
                --Unspecified cellulitis
                'cellulitis of skin with lymphangitis',
                'cellulitis with lymphangitis',
                'infectious lymphangitis',
                'pasteurella cellulitis due to cat bite',
                'sepsis due to cellulitis',
                'wound cellulitis',
                'acute bacterial lymphangitis',
                'acute lymphangitis',
                'acute lymphangitis, unspecified'
            )
        )
    group by
        cohort.visit_key,
        cohort.pat_key
)
select
    visit_key,
    pat_key,
    'CELLULITIS' as cohort,
    case when cellulitis_ind = 1 then 'CELLULITIS'
        when abscess_ind = 1 then 'ABSCESS'
        end as subcohort
from
    cellulitis_abscess_dx
