{{
  config(
    meta = {
      'critical': true
    }
  )
}}
select
    lookup_bioresponse_lab_component.diagnosis_hierarchy_1,
    lookup_bioresponse_lab_component.diagnosis_hierarchy_2,
    lookup_bioresponse_lab_component.procedure_id,
    lookup_bioresponse_lab_component.result_component_id
from
    {{ ref('lookup_bioresponse_lab_component') }} as lookup_bioresponse_lab_component
group by
    lookup_bioresponse_lab_component.diagnosis_hierarchy_1,
    lookup_bioresponse_lab_component.diagnosis_hierarchy_2,
    lookup_bioresponse_lab_component.procedure_id,
    lookup_bioresponse_lab_component.result_component_id
