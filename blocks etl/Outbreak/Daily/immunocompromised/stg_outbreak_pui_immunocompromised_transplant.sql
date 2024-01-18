{{ config(materialized='table', dist='pat_key') }}

select
    cohort.pat_key,
    cohort.outbreak_type,
    'Transplant' as reason,
    min(case
        when transplant_info.pat_key = 10779193
        then '2019-04-09 00:00:00' else transplant_info.transplnt_surg_dt end
    ) as start_date,
    current_date as end_date,
    'Transplant' as reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_cohort') }} as cohort
    inner join {{source('cdw', 'transplant_info')}} as transplant_info
        on cohort.pat_key = transplant_info.pat_key
    left join {{source('cdw', 'cdw_dictionary')}} as episode_type
        on episode_type.dict_key = transplant_info.dict_transplnt_epsd_type_key
where
    episode_type.dict_nm = 'Recipient'
group by
    cohort.pat_key,
    cohort.outbreak_type
having
    start_date is not null
