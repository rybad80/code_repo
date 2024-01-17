/*Pull cJADAS score*/
select
    smart_data_element_all.pat_key,
    smart_data_element_all.visit_key,
    max((1.0 * smart_data_element_all.element_value)) as jadas --JADAS Score
from
    {{ref('smart_data_element_all')}} as smart_data_element_all
where
    lower(smart_data_element_all.concept_id) = 'epic#31000148666' --JADAS Scores
group by
    smart_data_element_all.pat_key,
    smart_data_element_all.visit_key
