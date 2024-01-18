/*cardiac arrest cases and cardiac arrest patients will both source from this stage table,
with primary_key serving as the numerator for cases, and pat_key patients*/

select
    {{
        dbt_utils.surrogate_key([
            'cardiac_arrest_all.arrest_date',
            'cardiac_arrest_all.pat_key'
        ])
    }} as primary_key,
    cardiac_arrest_all.visit_key,
    cardiac_arrest_all.pat_key,
    cardiac_arrest_all.mrn,
    cardiac_arrest_all.arrest_date as metric_date,
    'cardiac_arrests' as metric_id_arrest_cases,
    'cardiac_arrest_pat' as metric_id_arrest_patients
from
    {{ref('cardiac_arrest_all')}} as cardiac_arrest_all
