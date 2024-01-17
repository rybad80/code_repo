with vps_diagnosis as (
    --region
    select
        'PHL' as picu_unit,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'caseid'
            ])
        }} as vps_episode_key,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'caseid',
                'diagnosis_id'
            ])
        }} as diagnosis_vps_episode_key,
        caseid as case_id,
        diagnosis_id,
        category as diagnosis_category,
        subcategory as diagnosis_subcategory,
        dateofonset,
        resolveddate,
        dxstatus as diagnosis_status,
        present_on_admission,
        primarydx as primary_diagnosis,
        starcode as star_code,
        starcodedesc as star_code_name,
        icd9cm as icd9_code,
        icd9desc as icd9_code_name,
        icd10cm as icd10_code,
        icd10desc as icd10_code_name,
        stscode as sts_code,
        stscodedesc as sts_code_name,
        resolved_during_icu_admission,
        history as diagnosis_history,
        congenital
    from
        {{source('vps_phl_ods', 'vps_phl_diagnosis')}}

    union all

    select
        'KOPH' as picu_unit,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'caseid'
            ])
        }} as vps_episode_key,
        {{
            dbt_utils.surrogate_key([
                'picu_unit',
                'caseid',
                'diagnosis_id'
            ])
        }} as diagnosis_vps_episode_key,
        caseid as case_id,
        diagnosis_id,
        category as diagnosis_category,
        subcategory as diagnosis_subcategory,
        dateofonset,
        resolveddate,
        dxstatus as diagnosis_status,
        present_on_admission,
        primarydx as primary_diagnosis,
        starcode as star_code,
        starcodedesc as star_code_name,
        icd9cm as icd9_code,
        icd9desc as icd9_code_name,
        icd10cm as icd10_code,
        icd10desc as icd10_code_name,
        stscode as sts_code,
        stscodedesc as sts_code_name,
        resolved_during_icu_admission,
        history as diagnosis_history,
        congenital
    from
        {{source('vps_koph_ods', 'vps_koph_diagnosis')}}
    --end region
)

select
    picu_unit,
    vps_episode_key,
    diagnosis_vps_episode_key,
    case_id,
    diagnosis_id,
    diagnosis_category,
    diagnosis_subcategory,
    case when dateofonset is not null then to_timestamp(dateofonset, 'mm/dd/yyyy') end as onset_date,
    case when resolveddate is not null then to_timestamp(resolveddate, 'mm/dd/yyyy') end as resolved_date,
    diagnosis_status::varchar(35) as diagnosis_status,
    present_on_admission,
    primary_diagnosis,
    star_code,
    star_code_name,
    icd9_code,
    icd9_code_name,
    icd10_code,
    icd10_code_name,
    sts_code,
    sts_code_name,
    resolved_during_icu_admission::varchar(5) as resolved_during_admission,
    diagnosis_history,
    congenital::varchar(5) as congenital
from
    vps_diagnosis
