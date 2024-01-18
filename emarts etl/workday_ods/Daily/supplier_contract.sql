{{
    config(
        materialized = 'incremental',
        unique_key = 'supplier_contract_wid',
        incremental_strategy = 'merge',
        merge_update_columns = ['supplier_contract_wid','supplier_contract_id','supplier_contract_number','contract_name','contract_specialist_worker_wid','contract_specialist_employee_id','supplier_wid','supplier_id','supplier_contract_start_date','supplier_contract_end_date', 'md5', 'upd_dt', 'upd_by']
    )
}}
with sup_contract as (
    select distinct
        supplier_contract_wid,
        supplier_contract_id,
        supplier_contract_number,
        contract_name,
        contract_specialist_worker_wid,
        contract_specialist_worker_empid as contract_specialist_employee_id,
        supplier_wid,
        supplier_id,
        to_date(substring(supplier_contract_start_date,1,10),'yyyy-mm-dd') as supplier_contract_start_date,
        to_date(substring(supplier_contract_end_date,1,10),'yyyy-mm-dd') as supplier_contract_end_date
    from
        {{source('workday_ods', 'workday_supplier_contract')}} as workday_supplier_contract
)
select
    supplier_contract_wid,
    supplier_contract_id,
    supplier_contract_number,
    contract_name,
    contract_specialist_worker_wid,
    contract_specialist_employee_id,
    supplier_wid,
    supplier_id,
    supplier_contract_start_date,
    supplier_contract_end_date,
    cast({{
        dbt_utils.surrogate_key([
            'supplier_contract_wid',
            'supplier_contract_id',
            'supplier_contract_number',
            'contract_name',
            'contract_specialist_worker_wid',
            'contract_specialist_employee_id',
            'supplier_wid',
            'supplier_id',
            'supplier_contract_start_date',
            'supplier_contract_end_date'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    sup_contract
where
    1 = 1
    {%- if is_incremental() %}
        and md5 not in (
            select md5
            from
                {{ this }}
            where
                supplier_contract_wid = sup_contract.supplier_contract_wid
        )
    {%- endif %}
