{{
  config(
    materialized = 'incremental',
    unique_key = 'expense_item_wid',
    incremental_strategy = 'merge',
    merge_update_columns = ['expense_item_wid', 'expense_item_id', 'item_name', 'item_desc', 'synonyms_and_keywords', 'item_unit_price', 'per_diem_expense_ind', 'instructional_text', 'travel_journal_ind', 'quantity_and_per_unit_amount_not_enterable_ind', 'inactive_ind', 'fixed_expense_ind', 'maximum_expense_ind', 'allow_overage_ind', 'based_on_allowance_plan_ind', 'memo_required_ind', 'display_arrival_date_before_departure_date_ind', 'spend_category_id', 'expense_item_group_id', 'hash_value', 'integration_id', 'update_date'],
    meta = {
        'critical': true
    }
  )
}}

{% set column_names = [
    'expense_item.expense_item_wid',
    'expense_item.expense_item_id',
    'expense_item.item_name',
    'expense_item.item_description',
    'expense_item.synonyms_and_keywords',
    'expense_item.item_unit_price',
    'expense_item.per_diem_expense_ind',
    'expense_item.instructional_text',
    'expense_item.travel_journal_ind',
    'expense_item.quantity_and_per_unit_amount_not_enterable_ind',
    'expense_item.inactive_ind',
    'expense_item.fixed_expense_ind',
    'expense_item.maximum_expense_ind',
    'expense_item.allow_overage_ind',
    'expense_item.based_on_allowance_plan_ind',
    'expense_item.memo_required_ind',
    'expense_item.display_arrival_date_before_departure_date_ind',
    'expense_item.spend_category_id',
    'expense_item.expense_item_group_id'
] %}
with expense_item
as (
select
    {{
        dbt_utils.surrogate_key([
            'expense_item.expense_item_wid'
        ])
    }} as expense_item_key,
    expense_item.expense_item_wid,
    expense_item.expense_item_id,
    expense_item.item_name,
    expense_item.item_description as item_desc,
    expense_item.synonyms_and_keywords,
    expense_item.item_unit_price,
    expense_item.per_diem_expense_ind,
    expense_item.instructional_text,
    expense_item.travel_journal_ind,
    expense_item.quantity_and_per_unit_amount_not_enterable_ind,
    expense_item.inactive_ind,
    expense_item.fixed_expense_ind,
    expense_item.maximum_expense_ind,
    expense_item.allow_overage_ind,
    expense_item.based_on_allowance_plan_ind,
    expense_item.memo_required_ind,
    expense_item.display_arrival_date_before_departure_date_ind,
    nvl(dim_spend_category.spend_category_key, -1) as spend_category_key,
    expense_item.spend_category_id,
    expense_item.expense_item_group_id,
    {{
        dbt_utils.surrogate_key(column_names or [] )
    }} as hash_value,
    expense_item.create_by || '~' || expense_item.expense_item_id as integration_id,
    current_timestamp as create_date,
    expense_item.create_by,
    current_timestamp as update_date,
    expense_item.upd_by as update_by
from
    {{source('workday_ods', 'expense_item')}} as expense_item
    left join  {{ref('dim_spend_category')}}  as dim_spend_category on
        nvl(expense_item.spend_category_wid, '0') = dim_spend_category.spend_category_wid
--
union all
--
select
    -1,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    0,
    0,
    'N/A',
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    -1,
    'N/A',
    'N/A',
    -1,
    'NA',
    CURRENT_TIMESTAMP,
    'UNSPECIFIED',
    CURRENT_TIMESTAMP, 
    'UNSPECIFIED'
--
union all
--
select
    -2,
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    'N/A',
    0,
    0,
    'N/A',
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    -2,
    'N/A',
    'N/A',
    -2,
    'NA',
    CURRENT_TIMESTAMP,
    'NOT APPLICABLE',
    CURRENT_TIMESTAMP, 
    'NOT APPLICABLE'
)
select
    expense_item_key,
    expense_item.expense_item_wid,
    expense_item.expense_item_id,
    expense_item.item_name,
    expense_item.item_desc,
    expense_item.synonyms_and_keywords,
    expense_item.item_unit_price,
    expense_item.per_diem_expense_ind,
    expense_item.instructional_text,
    expense_item.travel_journal_ind,
    expense_item.quantity_and_per_unit_amount_not_enterable_ind,
    expense_item.inactive_ind,
    expense_item.fixed_expense_ind,
    expense_item.maximum_expense_ind,
    expense_item.allow_overage_ind,
    expense_item.based_on_allowance_plan_ind,
    expense_item.memo_required_ind,
    expense_item.display_arrival_date_before_departure_date_ind,
    expense_item.spend_category_key,
    expense_item.spend_category_id,
    expense_item.expense_item_group_id,
    expense_item.hash_value,
    expense_item.integration_id,
    expense_item.create_date,
    expense_item.create_by,
    expense_item.update_date,
    expense_item.update_by
from
    expense_item
where
    1 = 1
{%- if is_incremental() %}
    and hash_value not in (
    select
        hash_value
    from
        {{ this }}
    where expense_item_wid = expense_item.expense_item_wid)
{%- endif %}
