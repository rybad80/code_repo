{% set column_names = ['proc_id', 'procedure_title', 'procedure_code', 'procedure_category_name',
    'cpt_code', 'procedure_group_id', 'procedure_group_name'] %}

{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = column_names + ['update_date', 'hash_value', 'integration_id'],
    meta = {
      'critical': true
    }
  )
}}

with clarity_eap_cte as (
    select
        clarity_eap.proc_id,
        clarity_eap.proc_name as procedure_title,  -- "procedure_name" appears to be a reserved term
        -- Following the patterns from cdwuat..procedure, "default" entries should be 'INVALID'
        -- and -2 in the following 2 fields:
        coalesce(clarity_eap.proc_code, 'INVALID') as procedure_code,
        coalesce(clarity_eap.proc_group_id, '-2') as procedure_group_id,
        clarity_eap.proc_cat_id
    from
        {{ source('clarity_ods', 'clarity_eap') }} as clarity_eap
),

clarity_eap_over_time_cte as (
    select distinct
        clarity_eap_ot.proc_id,
        clarity_eap_ot.cpt_code,
        row_number() over(partition by clarity_eap_ot.proc_id
            order by clarity_eap_ot.contact_date_real desc) as row_num
    from
        {{ source('clarity_ods', 'clarity_eap_ot') }} as clarity_eap_ot
),

most_recent_eap_ot_rows as (
    select
        *
    from
        clarity_eap_over_time_cte
    where
        row_num = 1
),

main as (
    select
        clarity_eap_cte.proc_id,
        clarity_eap_cte.procedure_title,
        clarity_eap_cte.procedure_code,
        edp_proc_cat_info.proc_cat_name as procedure_category_name,
        most_recent_eap_ot_rows.cpt_code,
        clarity_eap_cte.procedure_group_id,
        -- Following the pattern of cdwuat..procedure, when group id is null (coalesced to -2)
        -- then group name should be converted to 'NOT APPLICABLE'
        case
            when procedure_group_id = -2
                then 'NOT APPLICABLE'
            else clarity_epg.proc_group_name
        end as procedure_group_name
    from
        clarity_eap_cte
    left join {{ source('clarity_ods', 'edp_proc_cat_info') }} as edp_proc_cat_info
        on clarity_eap_cte.proc_cat_id = edp_proc_cat_info.proc_cat_id
    left join most_recent_eap_ot_rows
        on clarity_eap_cte.proc_id = most_recent_eap_ot_rows.proc_id
    left join {{ source('clarity_ods', 'clarity_epg') }} as clarity_epg
        on clarity_eap_cte.procedure_group_id = clarity_epg.proc_group_id
),

incremental as (
    select
        *,
        {{
            dbt_utils.surrogate_key(column_names or [] )
        }} as hash_value,
        'CLARITY' || '~' || proc_id as integration_id,
        current_timestamp as create_date,
        'CLARITY' as create_source,
        current_timestamp as update_date,
        'CLARITY' as update_source
    from
        main
),

extra_rows as (
    select
        {{
            dbt_utils.surrogate_key([
                'integration_id'
            ])
        }} as procedure_key,
        *
    from
        incremental

    union all
    select
        -1 as procedure_key,
        -1 as proc_id,
        'DEFAULT' as procedure_title,
        'DEFAULT' as procedure_code,
        'DEFAULT' as procedure_category_name,
        'DEFAULT' as cpt_code,
        -1 as procedure_group_id,
        'DEFAULT' as procedure_group_name,
        -1,
        'DEFAULT',
        current_timestamp,
        'DEFAULT',
        current_timestamp,
        'DEFAULT'
    union all
    select
        -2 as procedure_key,
        -2 as proc_id,
        'INVALID' as procedure_title,
        'INVALID' as procedure_code,
        'INVALID' as procedure_category_name,
        'INVALID' as cpt_code,
        -2 as procedure_group_id,
        'INVALID' as procedure_group_name,
        -2,
        'INVALID',
        current_timestamp,
        'INVALID',
        current_timestamp,
        'INVALID'
)

select
    procedure_key,
    proc_id,
    procedure_title,
    procedure_code,
    procedure_category_name,
    cpt_code,
    procedure_group_id,
    procedure_group_name,
    hash_value,
    integration_id,
    create_date,
    create_source,
    update_date,
    update_source
from
    extra_rows
where
    1 = 1
    {%- if is_incremental() %}
        and hash_value not in
        (
            select
                hash_value
            from
                {{ this }} -- TDL dim table
            where integration_id = extra_rows.integration_id
        )
    {%- endif %}
