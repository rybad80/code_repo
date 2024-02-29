select
      pvs.mrn,
      pvs.patient_name,
      date(result_date) as result_date,
      max(case when LOWER(result_component_name) like '%cholesterol%'
           then result_value_numeric end) as cholesterol_value,
      max(case when LOWER(result_component_name) like '%cholesterol%'
           then reference_unit end) as cholesterol_unit,           
      max(case when LOWER(result_component_name) like '%triglycerides%'
           then result_value_numeric end) as triglycerides_value,
      max(case when LOWER(result_component_name) like '%triglycerides%'
           then reference_unit end) as triglycerides_unit,           
      max(case when LOWER(result_component_name) like '%hgb%'
           then result_value_numeric end) as hgb_value,      
      max(case when LOWER(result_component_name) like '%hgb%'
           then reference_unit end) as hgb_unit,                 
      max(case when LOWER(result_component_name) like '%b%natriuretic%peptide%'
           then result_value_numeric end) as bnp_value,     
      max(case when LOWER(result_component_name) like '%b%natriuretic%peptide%'
           then reference_unit end) as bnp_unit,                
      max(case when LOWER(result_component_name) like '%sirolimus%'
                 or lower(result_component_name) = 'rapamycin'
           then result_value_numeric end) as sirolimus_value,   
      max(case when LOWER(result_component_name) like '%sirolimus%'
                 or lower(result_component_name) = 'rapamycin'
           then reference_unit end) as sirolimus_unit
from
      chop_analytics..procedure_order_result_clinical as result
      inner join ocqi_prd..fact_pulmonary_vein_stenosis as pvs
         on result.mrn = pvs.mrn
where 
     lower(result_component_name) in
     (
      'hgb',
      'cholesterol',
      'cholesterol, total poc',
      'cholesterol, total-lc',
      'triglycerides-lc',
      'cholesterol, total-q',
      'triglycerides-q',
      'cholesterol, total',
      'ast',
      'alt',
      'triglycerides',
      'ggt',
      'b natriuretic peptide',
      'sirolimus, blood',
      'rapamycin'   
     )
and
    result_value_numeric is not null 
group by 
      pvs.mrn,
      pvs.patient_name,
      date(result_date)
    