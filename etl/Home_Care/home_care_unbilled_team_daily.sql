{{
  config(
    materialized = 'incremental',
    unique_key = 'unbilled_team_unique_key',
        meta = {
        'critical': false
    }
  )
}}

with fc as (
    select
        'financial clearance' as team,
        sum(hdms_net_charge) as total_daily_unbilled_team_revenue,
        current_date as unbilled_date

    from
        {{ ref('home_care_claim_details') }}

    where
        lower(
            hdms_unbilled_reason
        ) in (
            'prior auth not attached',
            'prior auth expired',
            'prior auth not returned'
        )
        and hdms_provider_identifier in (1, 2) -- infusion, dme
        and hdms_net_charge > 0
        and (lower(patient_status_codes) like ('%dme%')
            or lower(patient_status_codes) like ('%hme%')
            or lower(patient_status_codes) like ('%ret%')
            or lower(patient_status_codes) like ('%rse%')
            or lower(patient_status_codes) like ('%sup%')
            or lower(patient_status_codes) like ('%rsc%'))

),

pac as (
    select
        'pac' as team,
        sum(hdms_net_charge) as total_daily_unbilled_team_revenue,
        current_date as unbilled_date

    from
        {{ ref('home_care_claim_details') }}

    where
        lower(hdms_unbilled_reason) like ('%cmn%')
        and hdms_provider_identifier in (1, 2) -- infusion, dme
        and hdms_net_charge > 0
),

pharmacy as (
    select
        'pharmacy' as team,
        sum(hdms_net_charge) as total_daily_unbilled_team_revenue,
        current_date as unbilled_date

    from
        {{ ref('home_care_claim_details') }}

    where
        lower(hdms_unbilled_reason) like ('%auth%')
        and hdms_provider_identifier = 1 -- infusion
        and hdms_net_charge > 0
        and (
            lower(
                hdms_delivery_method
            ) like ('%rx%') or lower(hdms_delivery_method) like ('%clinician%')
        )
        and (
            lower(
                patient_status_codes
            ) like ('%phc%') or lower(patient_status_codes) like ('%phd%')
        )
),

team_final as (
    select * from fc
    union all
    select * from pac
    union all
    select * from pharmacy
),

incremental as (
    select
        team as hdms_unbilled_team,
        total_daily_unbilled_team_revenue,
        unbilled_date,
        {{ dbt_utils.surrogate_key([
            'unbilled_date',            
            'hdms_unbilled_team'
            ]) }} as unbilled_team_unique_key
    from
        team_final
)

select
    *

from
    incremental

where
    1 = 1
    {%- if is_incremental() %}
        and unbilled_team_unique_key not in
        (
            select unbilled_team_unique_key
            from
                {{ this }} -- TDL dim table
            where unbilled_team_unique_key = incremental.unbilled_team_unique_key
        )
    {%- endif %}
