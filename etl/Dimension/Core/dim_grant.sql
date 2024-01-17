{{ config(
    materialized = 'incremental',
    unique_key = 'grant_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['grant_id', 'grant_name', 'include_id_in_name_ind', 'inactive_ind',
        'fund_id', 'cost_center_id', 'cost_center_site_id', 'program_id', 'gift_id',
        'provider_id', 'grant_uses_revenue_for_remaining_balance_calculation', 'award_line_id',
        'award_number', 'award_name', 'proposal_id', 'revenue_category', 'lifecycle_status',
        'contract_line_status', 'contract_line_type', 'billing_schedule', 'billing_schedule_type',
        'start_date', 'end_date', 'indirect_rate', 'sponsor_id', 'sponsor_name', 'grant_manager_id',
        'grant_manager_name', 'principal_investigator_id', 'principal_investigator_name',
        'last_updated_date', 'update_date', 'hash_value', 'integration_id'],
    meta = {
        'critical': true
    }
) }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(
        from=source('workday_ods', 'workday_grant_details'),
        except=['upd_dt']
) %}

with grants as (
    select
        {{ dbt_utils.surrogate_key([
                'grant_wid'
        ]) }} as grant_key,
        grant_wid,
        grant_reference_id as grant_id,
        grant_name,
        include_grant_id_in_name as include_id_in_name_ind,
        inactive as inactive_ind,
        fund_reference_id as fund_id,
        cost_center_reference_id as cost_center_id,
        cost_center_site_reference_id as cost_center_site_id,
        program_reference_id as program_id,
        gift_reference_id as gift_id,
        provider_reference_id as provider_id,
        grant_uses_revenue_for_remaining_balance_calculation,
        award_line_reference_id as award_line_id,
        award_number,
        award_name,
        proposal_id,
        revenue_category,
        lifecycle_status,
        contract_line_status,
        contract_line_type,
        billing_schedule,
        billing_schedule_type,
        start_date,
        end_date,
        indirect_rate,
        sponsor_reference_id as sponsor_id,
        sponsor_name,
        grant_manager_reference_id as grant_manager_id,
        grant_manager_name,
        principal_investigator_reference_id as principal_investigator_id,
        principal_investigator_name,
        to_timestamp(substring(last_updated_date from 1 for 23), 'YYYY-MM-DD"T"HH24:MI:SS.US')
            as last_updated_date,
        {{
            dbt_utils.surrogate_key(column_names or [])
        }} as hash_value,
        'WORKDAY' || '~' || grant_reference_id as integration_id,
        current_timestamp as create_date,
        'WORKDAY' as create_by,
        current_timestamp as update_date,
        'WORKDAY' as update_by
    from
        {{ source('workday_ods', 'workday_grant_details') }}

    union all

    select
        0,
        'NA',
        'NA',
        'NA',
        0,
        0,
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        current_date,
        current_date,
        0,
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        'NA',
        current_timestamp,
        0,
        'NA',
        current_timestamp,
        'DEFAULT',
        current_timestamp,
        'DEFAULT'
)

select
    grants.grant_key,
    grants.grant_wid,
    grants.grant_id,
    grants.grant_name,
    grants.include_id_in_name_ind,
    grants.inactive_ind,
    grants.fund_id,
    grants.cost_center_id,
    grants.cost_center_site_id,
    grants.program_id,
    grants.gift_id,
    grants.provider_id,
    grants.grant_uses_revenue_for_remaining_balance_calculation,
    grants.award_line_id,
    grants.award_number,
    grants.award_name,
    grants.proposal_id,
    grants.revenue_category,
    grants.lifecycle_status,
    grants.contract_line_status,
    grants.contract_line_type,
    grants.billing_schedule,
    grants.billing_schedule_type,
    grants.start_date,
    grants.end_date,
    grants.indirect_rate,
    grants.sponsor_id,
    grants.sponsor_name,
    grants.grant_manager_id,
    grants.grant_manager_name,
    grants.principal_investigator_id,
    grants.principal_investigator_name,
    grants.last_updated_date,
    grants.hash_value,
    grants.integration_id,
    grants.create_date,
    grants.create_by,
    grants.update_date,
    grants.update_by
from
    grants
where 1 = 1
    {%- if is_incremental() %}
        and hash_value not in (
            select
                hash_value
            from
                {{ this }} as existing
            where
                existing.grant_wid = grants.grant_wid
        )
    {%- endif %}
