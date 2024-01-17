select
    redcap_data.record::int as record,
    max(
        case
            when redcap_data.field_name = 'mrn'
            then redcap_data.value::varchar(255)
        end
    ) as mrn,
    max(
        case
            when redcap_data.field_name = 'csn'
            then redcap_data.value::numeric(14, 3)
        end
    ) as csn,
    max(
        case
            when redcap_data.field_name = 'pcu_adm_dt'
            then redcap_data.value::timestamp
        end
    ) as pcu_adm_dt,
    max(
        case
            when redcap_data.field_name = 'hsp_adm_rsn'
            then redcap_data.value::varchar(255)
        end
    ) as hsp_adm_rsn,
    max(
        case
            when redcap_data.field_name = 'pcu_adm_rsn'
            then redcap_data.value::varchar(255)
        end
    ) as pcu_adm_rsn,
    max(
        case
            when redcap_data.field_name = 'med_train'
            then stg_redcap_porter_value_label.element_text::varchar(30)
        end
    ) as med_train,
    max(
        case
            when redcap_data.field_name = 'gtube'
            then redcap_data.value::int
        end
    ) as gtube,
    max(
        case
            when redcap_data.field_name = 'trach'
            then redcap_data.value::int
        end
    ) as trach,
    max(
        case
            when redcap_data.field_name = 'trach_placement'
            then redcap_data.value::date
        end
    ) as trach_placement,
    max(
        case
            when redcap_data.field_name = 'new_trach'
            then redcap_data.value::int
        end
    ) as new_trach,
    max(
        case
            when redcap_data.field_name = 'feed_tube'
            then stg_redcap_porter_value_label.element_text::varchar(20)
        end
    ) as feed_tube,
    max(
        case
            when redcap_data.field_name = 'central_line'
            then stg_redcap_porter_value_label.element_text::varchar(20)
        end
    ) as central_line,
    max(
        case
            when redcap_data.field_name = 'tpn'
            then redcap_data.value::int
        end
    ) as tpn,
    max(
        case
            when redcap_data.field_name = 'team_color'
            then stg_redcap_porter_value_label.element_text::varchar(20)
        end
    ) as team_color,
    max(
        case
            when redcap_data.field_name = 'projected_disp'
            then stg_redcap_porter_value_label.element_text::varchar(50)
        end
    ) as projected_disp,
    max(
        case
            when redcap_data.field_name = 'destination'
            then stg_redcap_porter_value_label.element_text::varchar(50)
        end
    ) as destination,
    max(
        case
            when redcap_data.field_name = 'transport_mode'
            then stg_redcap_porter_value_label.element_text::varchar(60)
        end
    ) as transport_mode,
    max(
        case
            when redcap_data.field_name = 'pcu_disch_dt'
            then redcap_data.value::timestamp
        end
    ) as pcu_disch_dt
from
    {{ ref('stg_redcap_all')}} as redcap_data
    left join {{ref('stg_redcap_porter_value_label')}} as stg_redcap_porter_value_label
        on stg_redcap_porter_value_label.project_id = redcap_data.project_id
        and stg_redcap_porter_value_label.field_name = redcap_data.field_name
        and stg_redcap_porter_value_label.element_id = redcap_data.value
where
    redcap_data.project_id = 1289
group by
    redcap_data.record
