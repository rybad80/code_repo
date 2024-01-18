{% set column_names = ['room_id', 'room_csn', 'room_number', 'room_name', 'record_state_id',
    'record_state_title', 'department_key', 'department_id'] %}

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

with initial as (
    select
        clarity_rom.room_id,
        clarity_rom.room_csn_id as room_csn,
        clarity_rom.room_number,
        clarity_rom.room_name,
        clarity_rom.record_state_c as record_state_id,
        zc_pba_rec_stat.title as record_state_title,
        coalesce(clarity_rom.department_id::int, 0) as department_id
    from
        {{ source('clarity_ods', 'clarity_rom') }} as clarity_rom  -- Yes, that's a typo: it should be clarity_room
    -- The following join is described in the description for clarity_rom.record_state in Metadata Universe:
    -- "To look up the value (the category title) of the deprecated column (record_state) after the Clarity Compass upgrade,
    -- join column CLARITY_ROM.RECORD_STATE_C to table ZC_PBA_REC_STAT column TITLE to get the TITLE value."
    left join {{ source('clarity_ods', 'zc_pba_rec_stat') }} as zc_pba_rec_stat
        on clarity_rom.record_state_c = zc_pba_rec_stat.pba_rec_stat_c

    union all
    select '0', 0, 'DEFAULT', 'DEFAULT', null, null, 0
    union all
    select '-1', -1, 'INVALID', 'INVALID', null, null, -1
),

main as (
    select
        initial.room_id::int as room_id,
        initial.room_csn::int as room_csn,
        initial.room_number,
        initial.room_name,
        initial.record_state_id::int as record_state_id,
        initial.record_state_title,
        -- The following is necessary since stg_department_all does not have "default rows" yet,
        -- as of 11/13/2023. The next line of code does the following:
        -- If department_key is null, then give 0 or -1 to match department_id.
        case
            when initial.department_id = 0 then 0  -- unknown
            when stg_department_all.department_id is null then -1  -- invalid
            else stg_department_all.department_key
        end as department_key,
        -- The following case statement "conforms" this foreign key to the stg_department_all table:
        case
            when stg_department_all.department_id is null then -1  -- invalid
            else stg_department_all.department_id::int
        end as department_id
    from
        initial
    left join {{ ref('stg_department_all') }} as stg_department_all
        on initial.department_id = stg_department_all.department_id
),

incremental as (
    select
        *,
        {{
            dbt_utils.surrogate_key(column_names or [] )
        }} as hash_value,
        'CLARITY' || '~' || room_id as integration_id,
        current_timestamp as create_date,
        'CLARITY' as create_source,
        current_timestamp as update_date,
        'CLARITY' as update_source
    from
        main
)

select
    {{
        dbt_utils.surrogate_key([
            'integration_id'
        ])
    }} as room_key,
    room_id,
    room_csn,
    room_number,
    room_name,
    record_state_id,
    record_state_title,
    department_key,
    department_id,
    hash_value,
    integration_id,
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
