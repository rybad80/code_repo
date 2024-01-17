{{ config(meta = {
    'critical': false
}) }}

select
    'research' as domain, --noqa: L029
    'High Impact Publications' as metric_name,
    recordid as primary_key,
    loaddate as metric_date,
    recordid as num,
    'count' as num_calculation,
    'count' as metric_type
from
    {{ source('ods', 'vw_pubmed_chop_nih') }}
