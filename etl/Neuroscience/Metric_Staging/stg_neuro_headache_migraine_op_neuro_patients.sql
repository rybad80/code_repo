select
    'Headache/Migraine Patients' as metric_name,
    neuro_encounter.pat_key,
    neuro_encounter.encounter_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as desired_direction,
    neuro_encounter.pat_key as primary_key
from
    {{ ref('neuro_encounter')}} as neuro_encounter
    inner join {{ ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = neuro_encounter.visit_key
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
where
    neuro_encounter.headache_migraine_visit_ind = 1
    and neuro_encounter.office_visit_ind = 1
    and neuro_encounter.neurology_ind = 1
    and stg_encounter.department_id != 101001157 -- BGR EEG LAB
    and provider.prov_id
        not in ('1000034', '1000998', '532411', '6325') --EEG providers (list does not change often)  
group by
    neuro_encounter.pat_key,
    neuro_encounter.encounter_date
