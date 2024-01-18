{% set column_names = ['loc_id', 'serv_area_id', 'loc_name', 'location_abbr', 'pos_type',
    'gl_prefix', 'rpt_grp_one', 'rpt_grp_two', 'rpt_grp_three', 'rpt_grp_four', 'rpt_grp_five', 'rpt_grp_six',
    'rpt_grp_seven', 'rpt_grp_eight', 'rpt_grp_nine', 'rpt_grp_ten', 'rpt_grp_eleven_c', 'rpt_grp_twelve_c',
    'rpt_grp_thirteen_c', 'rpt_grp_fourteen_c', 'rpt_grp_fifteen_c', 'rpt_grp_sixteen_c',
    'rpt_grp_sevnteen_c', 'rpt_grp_eighteen_c', 'rpt_grp_nineteen_c', 'rpt_grp_twenty_c'] %}

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

with main as (
    select
        clarity_loc.loc_id,
        coalesce(clarity_loc.serv_area_id, 0) as serv_area_id,
        clarity_loc.loc_name,
        clarity_loc.location_abbr,
        clarity_loc.pos_type,
        clarity_loc.gl_prefix,
        clarity_loc.rpt_grp_one,
        clarity_loc.rpt_grp_two,
        clarity_loc.rpt_grp_three,
        clarity_loc.rpt_grp_four,
        clarity_loc.rpt_grp_five,
        zc_loc_rpt_grp_6.abbr as rpt_grp_six,
        -- There is no lookup for Report Group 7. See comment below:
        clarity_loc.rpt_grp_seven,
        clarity_loc.rpt_grp_eight,
        clarity_loc.rpt_grp_nine,
        clarity_loc.rpt_grp_ten,
        clarity_loc.rpt_grp_eleven_c,
        clarity_loc.rpt_grp_twelve_c,
        clarity_loc.rpt_grp_thirteen_c,
        clarity_loc.rpt_grp_fourteen_c,
        clarity_loc.rpt_grp_fifteen_c,
        clarity_loc.rpt_grp_sixteen_c,
        clarity_loc.rpt_grp_sevnteen_c,
        clarity_loc.rpt_grp_eighteen_c,
        clarity_loc.rpt_grp_nineteen_c,
        clarity_loc.rpt_grp_twenty_c
    from
        {{ source('clarity_ods', 'clarity_loc') }} as clarity_loc
    -- The following is a lookup table for Location Report Group 6. In order to match CDW table location,
    -- this field needed to be altered to be the abbreviation for the group rather than the ID.
    left join {{ source('clarity_ods', 'zc_loc_rpt_grp_6') }} as zc_loc_rpt_grp_6
        on clarity_loc.rpt_grp_six = zc_loc_rpt_grp_6.rpt_grp_six
),

incremental as (
    select
        *,
        {{
            dbt_utils.surrogate_key(column_names or [] )
        }} as hash_value,
        'CLARITY' || '~' || loc_id as integration_id,
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
    }} as location_key,
    *
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
