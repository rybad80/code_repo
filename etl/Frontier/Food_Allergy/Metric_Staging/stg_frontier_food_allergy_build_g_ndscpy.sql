select distinct --distinct due to differing tax-ids
    'Food Allergy: Endoscopy Events (EoE)' as metric_name,
    {{
        dbt_utils.surrogate_key([
            'encounter_date',
            'tx_id'
        ])
    }} as primary_key,
    procedure_name as drill_down_one,
    encounter_date as metric_date,
    'count' as num_calculation,
    'count' as metric_type,
    'up' as direction,
    'frontier_fa_endoscopy' as metric_id,
    stg_frontier_food_allergy_endoscopy_hx.mrn as num
from
    {{ ref('stg_frontier_food_allergy_endoscopy_hx')}} as stg_frontier_food_allergy_endoscopy_hx
