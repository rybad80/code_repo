{{ config(
    materialized = 'incremental',
    unique_key = ['award_wid', 'line_number'],
    incremental_strategy = 'merge',
    merge_update_columns = ['award_wid', 'award_id', 'line_number', 'receivable_contract_line_reference_id', 'line_item_description_override', 'deferred_revenue_ind', 'primary_grant_ind', 'line_amount', 'award_line_start_date', 'award_line_end_date', 'award_line_description', 'line_invoice_memo_override', 'line_federal_award_id_number', 'line_billing_notes', 'revenue_recognition_line_notes', 'affiliate_company_id', 'award_lifecycle_status_id', 'award_line_status_id', 'contract_line_type_id', 'revenue_category_id', 'cfda_number', 'cost_center_id', 'fund_id', 'gift_id', 'program_id', 'provider_id', 'cost_center_site_id', 'md5', 'upd_dt', 'upd_by']
) }}
with awardline_wktag_costcenter as (
    select distinct
        award_award_reference_wid,
        coalesce(cast(award_data_award_line_data_line_number as int), 0) as worktag_line_number,
        award_line_data_default_worktags_reference_cost_center_reference_id as cost_center_id
    from
        {{source('workday_ods', 'get_awards_data')}} as get_awards_data
    where
        award_line_data_default_worktags_reference_cost_center_reference_id is not null
),
awardline_wktag_fund as (
    select distinct
        award_award_reference_wid,
        coalesce(cast(award_data_award_line_data_line_number as int), 0) as worktag_line_number,
        award_line_data_default_worktags_reference_fund_id as fund_id
    from
        {{source('workday_ods', 'get_awards_data')}} as get_awards_data
    where
        award_line_data_default_worktags_reference_fund_id is not null
),
awardline_wktag_program as (
    select distinct
        award_award_reference_wid,
        coalesce(cast(award_data_award_line_data_line_number as int), 0) as worktag_line_number,
        award_line_data_default_worktags_reference_program_id as program_id
    from 
        {{source('workday_ods', 'get_awards_data')}} as get_awards_data
    where
        award_line_data_default_worktags_reference_program_id is not null
),
awardline_wktag_provider as ( 
    select distinct
        award_award_reference_wid,
        coalesce(cast(award_data_award_line_data_line_number as int), 0) as worktag_line_number,
        award_line_data_default_worktags_reference_organization_reference_id as provider_id
    from
        {{source('workday_ods', 'get_awards_data')}} as get_awards_data
    inner join
        {{ source('workday_ods', 'provider') }} as provider
            on get_awards_data.award_line_data_default_worktags_reference_wid = provider.provider_wid
    where
        award_line_data_default_worktags_reference_organization_reference_id is not null
),
awardline_wktag_cc_site as (
    select distinct
        award_award_reference_wid,
        coalesce(cast(award_data_award_line_data_line_number as int), 0) as worktag_line_number,
        award_line_data_default_worktags_reference_organization_reference_id as cost_center_site_id
    from
        {{source('workday_ods', 'get_awards_data')}} as get_awards_data
    inner join
        {{ ref('cost_center_site') }} as cost_center_site
            on get_awards_data.award_line_data_default_worktags_reference_wid = cost_center_site.cost_center_site_wid
    where
        award_line_data_default_worktags_reference_organization_reference_id is not null
),
unique_award_lines as (
    select
        award_award_reference_wid as award_wid,
        award_award_data_award_reference_id as award_id,
        coalesce(cast(award_data_award_line_data_line_number as int), 0) as line_number,
        max(award_data_award_line_data_receivable_contract_line_reference_id) as receivable_contract_line_reference_id
    from
        {{source('workday_ods', 'get_awards_data')}} as get_awards_data
    group by
        award_wid,
        award_id,
        line_number
),
final_output as (
    select distinct
        get_awards_data.award_award_reference_wid as award_wid,
        get_awards_data.award_award_data_award_reference_id as award_id,
        coalesce(cast(get_awards_data.award_data_award_line_data_line_number as int), 0) as line_number,
        unique_award_lines.receivable_contract_line_reference_id as receivable_contract_line_reference_id,
        get_awards_data.award_data_award_line_data_line_item_description_override as line_item_description_override,
        coalesce(cast(get_awards_data.award_data_award_line_data_deferred_revenue as int), -2) as deferred_revenue_ind,
        coalesce(cast(get_awards_data.award_data_award_line_data_primary_grant as int), -2) as primary_grant_ind,
        get_awards_data.award_data_award_line_data_line_amount as line_amount,
        to_timestamp(get_awards_data.award_data_award_line_data_award_line_start_date, 'yyyy-mm-dd') as award_line_start_date,
        to_timestamp(get_awards_data.award_data_award_line_data_award_line_end_date, 'yyyy-mm-dd') as award_line_end_date,
        get_awards_data.award_data_award_line_data_award_line_description as award_line_description,
        get_awards_data.award_data_award_line_data_line_invoice_memo_override as line_invoice_memo_override,
        get_awards_data.award_data_award_line_data_line_federal_award_id_number as line_federal_award_id_number,
        get_awards_data.award_data_award_line_data_line_billing_notes as line_billing_notes,
        null as revenue_recognition_line_notes,
        get_awards_data.award_line_data_intercompany_affiliate_reference_company_reference_id as affiliate_company_id,
        get_awards_data.award_line_data_award_lifecycle_status_reference_award_lifecycle_status_id as award_lifecycle_status_id,
        get_awards_data.award_line_data_line_status_reference_document_status_id as award_line_status_id,
        get_awards_data.award_line_data_line_type_reference_contract_line_type_id as contract_line_type_id,
        get_awards_data.award_line_data_revenue_category_reference_revenue_category_id as revenue_category_id,
        get_awards_data.award_line_data_line_cfda_number_reference_cfda_number as cfda_number,
        get_awards_data.awardline_wktag_costcenter.cost_center_id as cost_center_id,
        get_awards_data.awardline_wktag_fund.fund_id as fund_id,
        null as gift_id,
        get_awards_data.awardline_wktag_program.program_id as program_id,
        get_awards_data.awardline_wktag_provider.provider_id,
        get_awards_data.awardline_wktag_cc_site.cost_center_site_id,
        cast({{
            dbt_utils.surrogate_key([
                'award_wid',
                'award_id',
                'line_number',
                'receivable_contract_line_reference_id',
                'line_item_description_override',
                'deferred_revenue_ind',
                'primary_grant_ind',
                'line_amount',
                'award_line_start_date',
                'award_line_end_date',
                'award_line_description',
                'line_invoice_memo_override',
                'line_federal_award_id_number',
                'line_billing_notes',
                'revenue_recognition_line_notes',
                'affiliate_company_id',
                'award_lifecycle_status_id',
                'award_line_status_id',
                'contract_line_type_id',
                'revenue_category_id',
                'cfda_number',
                'cost_center_id',
                'fund_id',
                'gift_id',
                'program_id',
                'provider_id',
                'cost_center_site_id'
                ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from
        {{source('workday_ods', 'get_awards_data')}} as get_awards_data
    inner join
        unique_award_lines
            on get_awards_data.award_award_reference_wid = unique_award_lines.award_wid
            and coalesce(cast(get_awards_data.award_data_award_line_data_line_number as int), 0) = unique_award_lines.line_number
            and get_awards_data.award_data_award_line_data_receivable_contract_line_reference_id = unique_award_lines.receivable_contract_line_reference_id
    left join
        awardline_wktag_costcenter
            on get_awards_data.award_award_reference_wid = awardline_wktag_costcenter.award_award_reference_wid
            and coalesce(cast(award_data_award_line_data_line_number as int), 0) = awardline_wktag_costcenter.worktag_line_number
    left join
        awardline_wktag_fund
            on get_awards_data.award_award_reference_wid = awardline_wktag_fund.award_award_reference_wid
            and coalesce(cast(award_data_award_line_data_line_number as int), 0) = awardline_wktag_fund.worktag_line_number
    left join
        awardline_wktag_program
            on get_awards_data.award_award_reference_wid = awardline_wktag_program.award_award_reference_wid
            and coalesce(cast(award_data_award_line_data_line_number as int), 0) = awardline_wktag_program.worktag_line_number
    left join
        awardline_wktag_provider
            on get_awards_data.award_award_reference_wid = awardline_wktag_provider.award_award_reference_wid
            and coalesce(cast(award_data_award_line_data_line_number as int), 0) = awardline_wktag_provider.worktag_line_number
    left join
        awardline_wktag_cc_site
            on get_awards_data.award_award_reference_wid = awardline_wktag_cc_site.award_award_reference_wid
            and coalesce(cast(award_data_award_line_data_line_number as int), 0) = awardline_wktag_cc_site.worktag_line_number
    where award_award_data_award_reference_id is not null
)
select *
from final_output
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                award_wid = final_output.award_wid
                and line_number = final_output.line_number
        )
    {%- endif %}
