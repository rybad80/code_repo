with test_results as (
    select
        procedure_order_clinical.pat_key,
        procedure_order_clinical.placed_date,
        procedure_order_clinical.procedure_order_id,
        procedure_order_result_clinical.result_component_name,
        procedure_order_result_clinical.result_component_id,
        lookup_outbreak_pui_labs.test_description,
        result_component_external_name,
        procedure_order_result_clinical.result_value,
        case when (lower(procedure_order_result_clinical.result_value) like '%positive%'
                    or lower(procedure_order_result_clinical.result_value) like '%influenza a h1%'
                    or lower(procedure_order_result_clinical.result_value) = 'detected'
                    or lower(procedure_order_result_clinical.result_value) like '%influenza a detected%'
                    or lower(procedure_order_result_clinical.result_value) like '%influenza b detected%'
                    or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                        and lower(procedure_order_result_clinical.result_value) not like '%not%')) then '3'
            when (lower(procedure_order_result_clinical.result_value) like '%not detected%'
                    or lower(procedure_order_result_clinical.result_value) like '%neg%'
                    or lower(procedure_order_result_clinical.result_value) like '%negative%') then '2' else '0'
        end as current_status
    from
        {{ref('procedure_order_clinical')}} as procedure_order_clinical
        left join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
            on procedure_order_clinical.proc_ord_key = procedure_order_result_clinical.proc_ord_key
            and lower(procedure_order_result_clinical.result_status) not in ('incomplete', 'not applicable')
        left join {{ref('lookup_outbreak_pui_labs')}} as lookup_outbreak_pui_labs
            on lookup_outbreak_pui_labs.result_component_id = procedure_order_result_clinical.result_component_id
    where
        procedure_order_clinical.placed_date >= '2020-10-01'
        and (
        lookup_outbreak_pui_labs.result_component_id is not null
        or lower(result_component_external_name) like '%parainfluenza%'  --parainfluenza 1-4
        or lower(result_component_external_name) like '%adenovirus%'  --adenovirus
        or lower(result_component_external_name) like '%rhinovirus%'  --rhinovirus/enterovirus
        )
)

    select
        pat_key,
        procedure_order_id,
        current_status,
        case
            when test_description is not null
                then test_description
            when lower(result_component_external_name) like '%parainfluenza%'  then 'parainfluenza 1-4'
            when lower(result_component_external_name) like '%adenovirus%'  then 'adenovirus'
            when lower(result_component_external_name) like '%rhinovirus%'  then 'rhinovirus/enterovirus'
        end as test_type,
        min(placed_date) as min_specimen_taken_date
    from
        test_results
    where
        current_status in (2, 3)
    group by
        1, 2, 3, 4
