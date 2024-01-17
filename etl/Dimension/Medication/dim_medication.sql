select
    'clarity' as source_name,
    {{
        dbt_utils.surrogate_key([
            'source_name',
            'clarity_medication.medication_id'
        ])
    }} as medication_key,
    clarity_medication.medication_id::varchar(100) as medication_id,
    source_name || coalesce(clarity_medication.medication_id::varchar(100), '') as integration_id,
    clarity_medication.name as medication_name,
    zc_form.title as medication_form,
    clarity_medication.strength as medication_strength,
    zc_admin_route.title as medication_route,
    coalesce(medication_route_groupers.route_group, zc_admin_route.title) as medication_route_group,
    upper(
        coalesce(clarity_medication.generic_name, clarity_medication.name)
    ) as generic_medication_name,
    clarity_medication.gpi as generic_product_identifier,
    zc_thera_class.name as therapeutic_class,
    clarity_medication.thera_class_c::int as therapeutic_class_id,
    zc_pharm_class.name as pharmacy_class,
    clarity_medication.pharm_class_c::int as pharmacy_class_id,
    zc_pharm_subclass.name as pharmacy_sub_class,
    clarity_medication.pharm_subclass_c::int as pharmacy_sub_class_id,
    zc_simple_generic.name as simple_generic_medication,
    clarity_medication.simple_generic_c::bigint as simple_generic_medication_id,
    zc_dea_class_code.name as dea_class_name,
    clarity_medication.dea_class_code_c::int as dea_class_id,
    case when clarity_medication.dea_class_code_c between 1 and 5 then 1 else 0 end as controlled_substance_ind,
    case when lookup_medication_specialty.medication_id is not null then 1 else 0 end as specialty_medication_ind,
    case
        when clarity_medication.thera_class_c = 1001 and clarity_medication.pharm_class_c not in (11, 12) then 1
        else 0
    end as antibiotic_ind,
    case when clarity_medication.investigatl_med_yn = 'Y' then 1 else 0 end  as investigational_ind,

    coalesce(
        nvl2(rx_med_three.record_state_c, 1, 0),
        nvl2(clarity_medication.record_state, 0, 1)
    ) as active_ind

from
    {{ source('clarity_ods', 'clarity_medication') }} as clarity_medication
    left join {{ source('clarity_ods', 'rx_med_two') }} as rx_med_two
        on rx_med_two.medication_id = clarity_medication.medication_id
    left join {{ source('clarity_ods', 'rx_med_three') }} as rx_med_three
         on rx_med_three.medication_id = clarity_medication.medication_id
    -- dictionaries
    left join {{ source('clarity_ods', 'zc_form') }} as zc_form
        on zc_form.form_c = rx_med_two.form_c
    left join {{ source('clarity_ods', 'zc_admin_route') }} as zc_admin_route
        on zc_admin_route.med_route_c  = rx_med_two.admin_route_c
    left join {{ source('clarity_ods', 'zc_simple_generic') }} as zc_simple_generic
        on zc_simple_generic.simple_generic_c  = clarity_medication.simple_generic_c
    left join {{ source('clarity_ods', 'zc_dea_class_code') }} as zc_dea_class_code
        on zc_dea_class_code.dea_class_code_c  = clarity_medication.dea_class_code_c
    left join {{ source('clarity_ods', 'zc_thera_class') }} as zc_thera_class
        on zc_thera_class.thera_class_c  = clarity_medication.thera_class_c
    left join {{ source('clarity_ods', 'zc_pharm_class') }} as zc_pharm_class
        on zc_pharm_class.pharm_class_c  = clarity_medication.pharm_class_c
    left join {{ source('clarity_ods', 'zc_pharm_subclass') }} as zc_pharm_subclass
        on zc_pharm_subclass.pharm_subclass_c  = clarity_medication.pharm_subclass_c
    left join {{ref('lookup_medication_specialty')}} as lookup_medication_specialty
        on lookup_medication_specialty.medication_id = clarity_medication.medication_id
    left join {{ ref('lookup_medication_route_groupers') }} as medication_route_groupers
        on medication_route_groupers.source_id = zc_admin_route.med_route_c
        and medication_route_groupers.source_system = 'CLARITY'
