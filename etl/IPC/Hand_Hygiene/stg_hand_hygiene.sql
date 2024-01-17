select
    redcap_data.project_id,
    redcap_data.record,
    redcap_data.field_name as field_nm,
    cast(
        coalesce(
            stg_redcap_porter_value_label.element_text,
            redcap_data.value
        ) as varchar(100)
    ) as response
from
    {{ ref('stg_redcap_all')}} as redcap_data
    left join {{ref('stg_redcap_porter_value_label')}} as stg_redcap_porter_value_label
        on redcap_data.project_id = stg_redcap_porter_value_label.project_id
        and redcap_data.field_name = stg_redcap_porter_value_label.field_name
        and redcap_data.value = stg_redcap_porter_value_label.element_id
where
    redcap_data.project_id in (
        695, --Hand Hygiene IP / Hand Hygiene Program
        659, --Hand Hygiene OP / Hand Hygiene Tool-Ambulatory
        895 --Hand Hygiene Program - Ambulatory GBP
    )
