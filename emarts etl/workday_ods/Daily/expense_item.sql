{{ config(
    materialized = 'incremental',
    unique_key = 'expense_item_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['expense_item_wid','expense_item_id','item_name','item_description','synonyms_and_keywords','item_unit_price','per_diem_expense_ind','instructional_text','travel_journal_ind','quantity_and_per_unit_amount_not_enterable_ind','inactive_ind','fixed_expense_ind','maximum_expense_ind','allow_overage_ind','based_on_allowance_plan_ind','memo_required_ind','display_arrival_date_before_departure_date_ind','spend_category_wid','spend_category_id','expense_item_group_wid','expense_item_group_id', 'md5', 'upd_dt', 'upd_by']
) }}
    select distinct
        expense_item_expense_item_reference_wid as expense_item_wid,
        expense_item_expense_item_reference_expense_item_id as expense_item_id,
        expense_item_expense_item_data_item_name as item_name,
        expense_item_expense_item_data_item_description as item_description,
        null as synonyms_and_keywords,
        expense_item_expense_item_data_item_unit_price as item_unit_price,
        coalesce(cast(expense_item_expense_item_data_per_diem_expense as int),-2) as per_diem_expense_ind,
        expense_item_expense_item_data_instructional_text as instructional_text,
        coalesce(cast(expense_item_expense_item_data_travel_journal as int), -2) as travel_journal_ind,
        coalesce(cast(expense_item_expense_item_data_quantity_and_per_unit_amount_not_enterable as int), -2) as quantity_and_per_unit_amount_not_enterable_ind,
        coalesce(cast(expense_item_expense_item_data_inactive as int), -2) as inactive_ind,
        coalesce(cast(expense_item_expense_item_data_fixed_expense as int), -2) as fixed_expense_ind,
        coalesce(cast(expense_item_expense_item_data_maximum_daily_expense as int),-2) as maximum_expense_ind,
        coalesce(cast(expense_item_expense_item_data_allow_overage as int),-2) as allow_overage_ind,
        coalesce(cast(expense_item_expense_item_data_based_on_allowance_plan as int),-2) as based_on_allowance_plan_ind,
        coalesce(cast(expense_item_expense_item_data_memo_required as int), -2) as memo_required_ind,
        coalesce(cast(expense_item_expense_item_data_display_arrival_date_before_departure_date as int), -2) as display_arrival_date_before_departure_date_ind,
        expense_item_data_resource_category_reference_wid as spend_category_wid,
        expense_item_data_resource_category_reference_spend_category_id as spend_category_id,
        expense_item_data_expense_item_group_reference_wid as expense_item_group_wid,
        expense_item_data_expense_item_group_reference_expense_item_group_id as expense_item_group_id,
        cast({{
            dbt_utils.surrogate_key([
                'expense_item_wid',
                'expense_item_id',
                'item_name',
                'item_description',
                'synonyms_and_keywords',
                'item_unit_price',
                'per_diem_expense_ind',
                'instructional_text',
                'travel_journal_ind',
                'quantity_and_per_unit_amount_not_enterable_ind',
                'inactive_ind',
                'fixed_expense_ind',
                'maximum_expense_ind',
                'allow_overage_ind',
                'based_on_allowance_plan_ind',
                'memo_required_ind',
                'display_arrival_date_before_departure_date_ind',
                'spend_category_wid',
                'spend_category_id',
                'expense_item_group_wid',
                'expense_item_group_id'
            ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from 
        {{source('workday_ods', 'get_expense_items')}} as get_expense_items
    where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                expense_item_wid = get_expense_items.expense_item_expense_item_reference_wid
        )
    {%- endif %}
