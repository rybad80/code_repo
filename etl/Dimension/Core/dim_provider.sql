{{  config(
    materialized = 'incremental',
    unique_key = 'integration_id',
    incremental_strategy = 'merge',
    merge_update_columns = ['prov_id', 'full_name', 'first_name', 'last_name', 'provider_type', 'provider_primary_specialty', 'title', 'active_stat_ind', 'user_id', 'update_date', 'hash_value', 'integration_id'],
    meta = {
    'critical': true
    }
)
}}

with combine_providers as (
  select
    prov_id,
    full_nm,
    first_nm,
    last_nm,
    prov_type,
    title,
    active_stat,
    user_id,
    null as provider_primary_specialty,
    create_by,
    upd_by
  from
    {{source('manual_ods','admin_provider')}}

  union all

  select
    prov_id,
    full_nm,
    first_nm,
    last_nm,
    prov_type,
    title,
    active_stat,
    user_id,
    null as provider_primary_specialty,
    create_by,
    upd_by
  from
    {{source('manual_ods','idx_provider')}}

  union all

  select
    prov_id,
    full_nm,
    first_nm,
    last_nm,
    prov_type,
    title,
    active_stat,
    null as provider_primary_specialty,
    user_id,
    create_by,
    upd_by
  from
    {{source('manual_ods','orm_provider')}}

  union all

  select
    prov_id,
    full_nm,
    first_nm,
    last_nm,
    prov_type,
    title,
    active_stat,
    user_id,
    null as provider_primary_specialty,
    create_by,
    upd_by
  from
    {{source('manual_ods','scm_provider')}}

  union all

  select
    clarity_ser.prov_id,
    prov_name as full_nm,
    case length(
      substr(
        upper(
          ltrim(
            rtrim(
              substr(prov_name, instr(prov_name, ',') + 1, length(prov_name))
            )
          )
        ),
        1,
        50
      )
    ) when 0 then null else substr(
      upper(
        ltrim(
          rtrim(
            substr(prov_name, instr(prov_name, ',') + 1, length(prov_name))
          )
        )
      ),
      1,
      50
    ) end as first_nm,
    case length(
      substr(
        ltrim(
          rtrim(
            substr(prov_name, 1, instr(prov_name, ',') - 1)
          )
        ),
        0,
        50
      )
    ) when 0 then null else substr(
      ltrim(
        rtrim(
          substr(prov_name, 1, instr(prov_name, ',') - 1)
        )
      ),
      0,
      50
    ) end as last_nm,
    prov_type,
    clinician_title as title,
    active_status as active_stat,
    user_id,
    zc_specialty.title as provider_primary_specialty,
    'CLARITY' as create_by,
    'CLARITY' as upd_by
  from
    {{source('clarity_ods','clarity_ser')}} as clarity_ser
    left join {{source('clarity_ods','clarity_ser_spec')}} as clarity_ser_spec
      on clarity_ser_spec.prov_id = clarity_ser.prov_id
      and clarity_ser_spec.line = 1
    left join {{source('clarity_ods','zc_specialty')}} as zc_specialty
      on zc_specialty.specialty_c = clarity_ser_spec.specialty_c

  union all

  select
    cast(doctor_number as varchar(50)) || '.005' as prov_id,
    doctor_name as full_nm,
    ltrim(rtrim(substr(doctor_name, instr( doctor_name, ',', 1, 1) + 1))) as first_name,
    substr(doctor_name, 1, instr( doctor_name, ',', 1, 1 ) - 1) as last_name,
    doctor_type as prov_type,
    doctor_title as title,
    null as active_stat,
    null as user_id,
    doctor_specialty as provider_primary_specialty,
    'FASTRACK' as create_by,
    'FASTRACK' as upd_by
  from
    {{source('fastrack_ods','doctorm')}}

  union all

  select
      provider_id as prov_id,
      upper(provider_name) as full_nm,
      upper(
          ltrim(
              rtrim(
                  substr(
                      provider_name,
                      instr(provider_name, ',') + 1,
                      length(provider_name) - instr(provider_name, ',')
                  ))
          )
      ) as first_name,
      upper(ltrim(rtrim(substr(provider_name, 1, instr(provider_name, ',') - 1)))) as last_name,
      null as prov_type,
      null as title,
      case
          when inactive_ind = 0
          then 'Active'
          when inactive_ind = 1
          then 'Inactive'
      end as active_stat,
      null as user_id,
      null as provider_primary_specialty,
      'WORKDAY' as create_by,
      'WORKDAY' as upd_by
  from
    {{source('workday_ods','provider')}}
)

  select
    {{
        dbt_utils.surrogate_key([
            'combine_providers.prov_id',
            'combine_providers.create_by'
        ])
    }} as provider_key,
    combine_providers.prov_id,
    combine_providers.full_nm as full_name,
    combine_providers.first_nm as first_name,
    combine_providers.last_nm as last_name,
    combine_providers.prov_type as provider_type,
    combine_providers.provider_primary_specialty,
    combine_providers.title,
    case when combine_providers.active_stat  = 'Active' then 1 else 0 end as active_stat_ind,
    combine_providers.user_id,
    {{
      dbt_utils.surrogate_key(
        ['prov_id', 'full_nm', 'first_nm', 'last_nm', 'prov_type', 'provider_primary_specialty',
          'title', 'active_stat', 'user_id'] or []
      )
    }} as hash_value,
    combine_providers.create_by || '~' || combine_providers.prov_id as integration_id,
    combine_providers.create_by as create_source,
    current_timestamp as create_date,
    combine_providers.upd_by as update_source,
    current_timestamp as update_date
  from
    combine_providers
where
    1=1
  {%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where integration_id = combine_providers.create_by || '~' || combine_providers.prov_id)
{%- endif %}

