select
    cardiac_study_id as cath_study_id,
    mrn,
    dob,
    study_date as cath_date,
    case
        when lower(cath_proc_type) in ('intervention (non-tpvr)', 'tpvr') then 'Intervention'
        else cath_proc_type
    end as procedure_type_category,
    {{
        dbt_utils.surrogate_key([
            'cath_study_id'
        ])
    }} as primary_key,
    'cardiac_cath' as metric_id
from
    {{ ref('cardiac_cath') }}
