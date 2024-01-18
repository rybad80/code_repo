{{ config(
    materialized = 'incremental',
    unique_key = 'expense_item_group_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['expense_item_group_wid','expense_item_group_id','expense_item_group_name','md5', 'upd_dt', 'upd_by']
) }}
    select distinct
        expense_item_group_reference_wid as expense_item_group_wid,
        expense_item_group_reference_expense_item_group_id as expense_item_group_id,
        expense_item_group_data_expense_item_group_name as expense_item_group_name,
        cast({{
            dbt_utils.surrogate_key([
                'expense_item_group_wid',
                'expense_item_group_id',
                'expense_item_group_name'
             ])
        }} as varchar(100)) as md5,
        current_timestamp as create_dt,
        'WORKDAY' as create_by,
        current_timestamp as upd_dt,
        'WORKDAY' as upd_by
    from 
         {{source('workday_ods', 'get_expense_item_groups')}} as get_expense_item_groups
    where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                expense_item_group_wid = get_expense_item_groups.expense_item_group_reference_wid
        )
    {%- endif %}    