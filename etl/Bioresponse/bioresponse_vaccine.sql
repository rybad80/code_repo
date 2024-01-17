select
    {{
        dbt_utils.surrogate_key([
            'diagnosis_hierarchy_1',
            'diagnosis_hierarchy_2',
            'procedure_order_clinical.csn',
            'procedure_order_clinical.procedure_id'
        ])
    }} as visit_vaccination_key,
    lookup_bioresponse_vaccine.diagnosis_hierarchy_1,
    lookup_bioresponse_vaccine.diagnosis_hierarchy_2,
    procedure_order_clinical.csn,
    min(procedure_order_clinical.placed_date) as first_placed_date,
    procedure_order_clinical.procedure_id,
    procedure_order_clinical.procedure_name,
    procedure_order_clinical.procedure_group_name,
    procedure_order_clinical.procedure_subgroup_name
from
    {{ ref('procedure_order_clinical') }} as procedure_order_clinical
    inner join {{ ref('lookup_bioresponse_vaccine') }} as lookup_bioresponse_vaccine
        on lookup_bioresponse_vaccine.procedure_id = procedure_order_clinical.procedure_id
where
    procedure_order_clinical.order_status = 'Completed'
    and procedure_order_type not in ('Future Order')
group by
    lookup_bioresponse_vaccine.diagnosis_hierarchy_1,
    lookup_bioresponse_vaccine.diagnosis_hierarchy_2,
    procedure_order_clinical.csn,
    procedure_order_clinical.procedure_id,
    procedure_order_clinical.procedure_name,
    procedure_order_clinical.procedure_group_name,
    procedure_order_clinical.procedure_subgroup_name
