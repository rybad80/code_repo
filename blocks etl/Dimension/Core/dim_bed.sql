{{
  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = ['bed_id', 'bed_record_state', 'census_inclusion_ind',
        'update_date', 'hash_value', 'integration_id'],
      meta = {
      'critical': true
    }
  )
}}

with main as (
    select
        clarity_bed.bed_id::int as bed_id,
        clarity_bed.bed_label::varchar(30) as bed_name,
        coalesce(upper(clarity_bed.record_state)::varchar(30), 'NA') as bed_record_state,
        case
            when lower(clarity_bed.census_inclusn_yn) = 'y'
            then 1
            when  lower(clarity_bed.census_inclusn_yn) = 'n'
            then 0
            else -2
        end::smallint as census_inclusion_ind,
        'CLARITY' as create_source,
        -- We checked that bed_cont_date_real give a strict ordering:
        row_number() over(partition by clarity_bed.bed_id order by clarity_bed.bed_cont_date_real desc)
            as bed_line_number
    from
        {{ source('clarity_ods', 'clarity_bed') }} as clarity_bed

    union all

    select
        0 as bed_id,
        'DEFAULT' as bed_name,
        'DELETED' as bed_record_state,
        -2 as census_inclusion_ind,
        'DEFAULT' as create_source,
        1

    union all

    select
        -1 as bed_id,
        'INVALID' as bed_name,
        'DELETED' as bed_record_state,
        -2 as census_inclusion_ind,
        'DEFAULT' as create_source,
        1
),

deduped as (
    select * from main where bed_line_number = 1
),

incremental as (
    select
        bed_id,
        bed_name,
        bed_record_state,
        census_inclusion_ind,
        create_source || '~' || bed_id as integration_id,
        {{
            dbt_utils.surrogate_key([
                'bed_id',
                'bed_name',
                'bed_record_state',
                'census_inclusion_ind'
            ])
        }} as hash_value,
        current_timestamp as create_date,
        create_source,
        current_timestamp as update_date,
        'CLARITY' as update_source,
        bed_line_number
    from
        deduped
)

select
    {{ dbt_utils.surrogate_key([
        'integration_id'
    ]) }} as bed_key,
    bed_id,
    bed_name,
    bed_record_state,
    census_inclusion_ind,
    integration_id,
    hash_value,
    create_date,
    create_source,
    update_date,
    update_source
from
    incremental
where
    1 = 1
    {%- if is_incremental() %}
        and hash_value not in
        (
            select
                hash_value
            from
                {{ this }} -- TDL dim table
            where integration_id = incremental.integration_id
        )
    {%- endif %}
