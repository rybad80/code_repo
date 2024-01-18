with ledgeracctsummarybase as
(
select 
    ledger_account_summary_wid,
    ledger_account_summary_id,
    ledger_account_summary_name,
    ledger_account_summary_parent_id
from 
{{ ref('ledger_account_summary') }} as ledger_account_summary
),
level1 as (
    select 
        base.ledger_account_summary_wid,
        base.ledger_account_summary_id,
        base.ledger_account_summary_name,
        base.ledger_account_summary_wid as level_1_ledger_account_summary_wid,
        base.ledger_account_summary_id as level_1_ledger_account_summary_id,
        base.ledger_account_summary_name as level_1_ledger_account_summary_name,
        null as level_2_ledger_account_summary_wid,
        null as level_2_ledger_account_summary_id,
        null as level_2_ledger_account_summary_name,
        null as level_3_ledger_account_summary_wid,
        null as level_3_ledger_account_summary_id,
        null as level_3_ledger_account_summary_name,
        null as level_4_ledger_account_summary_wid,
        null as level_4_ledger_account_summary_id,
        null as level_4_ledger_account_summary_name,
        null as level_5_ledger_account_summary_wid,
        null as level_5_ledger_account_summary_id,
        null as level_5_ledger_account_summary_name,
        null as level_6_ledger_account_summary_wid,
        null as level_6_ledger_account_summary_id,
        null as level_6_ledger_account_summary_name
    from
        ledgeracctsummarybase base
    where
        base.ledger_account_summary_parent_id is null
),
level2 as (
    select
        base.ledger_account_summary_wid,
        base.ledger_account_summary_id,
        base.ledger_account_summary_name,
        level1.level_1_ledger_account_summary_wid,
        level1.level_1_ledger_account_summary_id,
        level1.level_1_ledger_account_summary_name,
        base.ledger_account_summary_wid as level_2_ledger_account_summary_wid,
        base.ledger_account_summary_id as level_2_ledger_account_summary_id,
        base.ledger_account_summary_name as level_2_ledger_account_summary_name,
        null as level_3_ledger_account_summary_wid,
        null as level_3_ledger_account_summary_id,
        null as level_3_ledger_account_summary_name,
        null as level_4_ledger_account_summary_wid,
        null as level_4_ledger_account_summary_id,
        null as level_4_ledger_account_summary_name,
        null as level_5_ledger_account_summary_wid,
        null as level_5_ledger_account_summary_id,
        null as level_5_ledger_account_summary_name,
        null as level_6_ledger_account_summary_wid,
        null as level_6_ledger_account_summary_id,
        null as level_6_ledger_account_summary_name
    from
        level1
    inner join
        ledgeracctsummarybase base
            on level1.level_1_ledger_account_summary_id = base.ledger_account_summary_parent_id
),
level3 as (
    select
        base.ledger_account_summary_wid,
        base.ledger_account_summary_id,
        base.ledger_account_summary_name,
        level2.level_1_ledger_account_summary_wid,
        level2.level_1_ledger_account_summary_id,
        level2.level_1_ledger_account_summary_name,
        level2.level_2_ledger_account_summary_wid,
        level2.level_2_ledger_account_summary_id,
        level2.level_2_ledger_account_summary_name,
        base.ledger_account_summary_wid as level_3_ledger_account_summary_wid,
        base.ledger_account_summary_id as level_3_ledger_account_summary_id,
        base.ledger_account_summary_name as level_3_ledger_account_summary_name,
        null as level_4_ledger_account_summary_wid,
        null as level_4_ledger_account_summary_id,
        null as level_4_ledger_account_summary_name,
        null as level_5_ledger_account_summary_wid,
        null as level_5_ledger_account_summary_id,
        null as level_5_ledger_account_summary_name,
        null as level_6_ledger_account_summary_wid,
        null as level_6_ledger_account_summary_id,
        null as level_6_ledger_account_summary_name
    from level2
    inner join ledgeracctsummarybase base on
        level2.level_2_ledger_account_summary_id = base.ledger_account_summary_parent_id
),
level4 as (
    select
        base.ledger_account_summary_wid,
        base.ledger_account_summary_id,
        base.ledger_account_summary_name,
        level3.level_1_ledger_account_summary_wid,
        level3.level_1_ledger_account_summary_id,
        level3.level_1_ledger_account_summary_name,
        level3.level_2_ledger_account_summary_wid,
        level3.level_2_ledger_account_summary_id,
        level3.level_2_ledger_account_summary_name,
        level3.level_3_ledger_account_summary_wid,
        level3.level_3_ledger_account_summary_id,
        level3.level_3_ledger_account_summary_name,
        base.ledger_account_summary_wid as level_4_ledger_account_summary_wid,
        base.ledger_account_summary_id as level_4_ledger_account_summary_id,
        base.ledger_account_summary_name as level_4_ledger_account_summary_name,
        null as level_5_ledger_account_summary_wid,
        null as level_5_ledger_account_summary_id,
        null as level_5_ledger_account_summary_name,
        null as level_6_ledger_account_summary_wid,
        null as level_6_ledger_account_summary_id,
        null as level_6_ledger_account_summary_name
    from level3 
    inner join ledgeracctsummarybase base on 
        level3.level_3_ledger_account_summary_id = base.ledger_account_summary_parent_id
),
level5 as (
    select
        base.ledger_account_summary_wid,
        base.ledger_account_summary_id,
        base.ledger_account_summary_name,
        level4.level_1_ledger_account_summary_wid,
        level4.level_1_ledger_account_summary_id,
        level4.level_1_ledger_account_summary_name,
        level4.level_2_ledger_account_summary_wid,
        level4.level_2_ledger_account_summary_id,
        level4.level_2_ledger_account_summary_name,
        level4.level_3_ledger_account_summary_wid,
        level4.level_3_ledger_account_summary_id,
        level4.level_3_ledger_account_summary_name,
        level4.level_4_ledger_account_summary_wid,
        level4.level_4_ledger_account_summary_id,
        level4.level_4_ledger_account_summary_name,
        base.ledger_account_summary_wid as level_5_ledger_account_summary_wid,
        base.ledger_account_summary_id as level_5_ledger_account_summary_id,
        base.ledger_account_summary_name as level_5_ledger_account_summary_name,
        null as level_6_ledger_account_summary_wid,
        null as level_6_ledger_account_summary_id,
        null as level_6_ledger_account_summary_name
    from level4 
    inner join ledgeracctsummarybase base on 
        level4.level_4_ledger_account_summary_id = base.ledger_account_summary_parent_id
),
level6 as (
    select
        base.ledger_account_summary_wid,
        base.ledger_account_summary_id,
        base.ledger_account_summary_name,
        level5.level_1_ledger_account_summary_wid,
        level5.level_1_ledger_account_summary_id,
        level5.level_1_ledger_account_summary_name,
        level5.level_2_ledger_account_summary_wid,
        level5.level_2_ledger_account_summary_id,
        level5.level_2_ledger_account_summary_name,
        level5.level_3_ledger_account_summary_wid,
        level5.level_3_ledger_account_summary_id,
        level5.level_3_ledger_account_summary_name,
        level5.level_4_ledger_account_summary_wid,
        level5.level_4_ledger_account_summary_id,
        level5.level_4_ledger_account_summary_name,
        level5.level_5_ledger_account_summary_wid,
        level5.level_5_ledger_account_summary_id,
        level5.level_5_ledger_account_summary_name,
        base.ledger_account_summary_wid as level_6_ledger_account_summary_wid,
        base.ledger_account_summary_id as level_6_ledger_account_summary_id,
        base.ledger_account_summary_name as level_6_ledger_account_summary_name
    from level5 
    inner join ledgeracctsummarybase base on 
        level5.level_5_ledger_account_summary_id = base.ledger_account_summary_parent_id
), 
finaloutput as (
    select ledger_account_summary_wid, ledger_account_summary_id, ledger_account_summary_name
    , level_1_ledger_account_summary_wid, level_1_ledger_account_summary_id, level_1_ledger_account_summary_name
    ,level_2_ledger_account_summary_wid, level_2_ledger_account_summary_id, level_2_ledger_account_summary_name
    ,level_3_ledger_account_summary_wid, level_3_ledger_account_summary_id, level_3_ledger_account_summary_name
    ,level_4_ledger_account_summary_wid, level_4_ledger_account_summary_id, level_4_ledger_account_summary_name
    ,level_5_ledger_account_summary_wid, level_5_ledger_account_summary_id, level_5_ledger_account_summary_name
    ,level_6_ledger_account_summary_wid, level_6_ledger_account_summary_id, level_6_ledger_account_summary_name
    from level1
    union
    select ledger_account_summary_wid, ledger_account_summary_id, ledger_account_summary_name
    , level_1_ledger_account_summary_wid, level_1_ledger_account_summary_id, level_1_ledger_account_summary_name
    ,level_2_ledger_account_summary_wid, level_2_ledger_account_summary_id, level_2_ledger_account_summary_name
    ,level_3_ledger_account_summary_wid, level_3_ledger_account_summary_id, level_3_ledger_account_summary_name
    ,level_4_ledger_account_summary_wid, level_4_ledger_account_summary_id, level_4_ledger_account_summary_name
    ,level_5_ledger_account_summary_wid, level_5_ledger_account_summary_id, level_5_ledger_account_summary_name
    ,level_6_ledger_account_summary_wid, level_6_ledger_account_summary_id, level_6_ledger_account_summary_name
    from level2
    union
    select ledger_account_summary_wid, ledger_account_summary_id, ledger_account_summary_name
    , level_1_ledger_account_summary_wid, level_1_ledger_account_summary_id, level_1_ledger_account_summary_name
    ,level_2_ledger_account_summary_wid, level_2_ledger_account_summary_id, level_2_ledger_account_summary_name
    ,level_3_ledger_account_summary_wid, level_3_ledger_account_summary_id, level_3_ledger_account_summary_name
    ,level_4_ledger_account_summary_wid, level_4_ledger_account_summary_id, level_4_ledger_account_summary_name
    ,level_5_ledger_account_summary_wid, level_5_ledger_account_summary_id, level_5_ledger_account_summary_name
    ,level_6_ledger_account_summary_wid, level_6_ledger_account_summary_id, level_6_ledger_account_summary_name
    from level3
    union
    select ledger_account_summary_wid, ledger_account_summary_id, ledger_account_summary_name
    , level_1_ledger_account_summary_wid, level_1_ledger_account_summary_id, level_1_ledger_account_summary_name
    ,level_2_ledger_account_summary_wid, level_2_ledger_account_summary_id, level_2_ledger_account_summary_name
    ,level_3_ledger_account_summary_wid, level_3_ledger_account_summary_id, level_3_ledger_account_summary_name
    ,level_4_ledger_account_summary_wid, level_4_ledger_account_summary_id, level_4_ledger_account_summary_name
    ,level_5_ledger_account_summary_wid, level_5_ledger_account_summary_id, level_5_ledger_account_summary_name
    ,level_6_ledger_account_summary_wid, level_6_ledger_account_summary_id, level_6_ledger_account_summary_name
    from level4
    union
    select ledger_account_summary_wid, ledger_account_summary_id, ledger_account_summary_name
    , level_1_ledger_account_summary_wid, level_1_ledger_account_summary_id, level_1_ledger_account_summary_name
    ,level_2_ledger_account_summary_wid, level_2_ledger_account_summary_id, level_2_ledger_account_summary_name
    ,level_3_ledger_account_summary_wid, level_3_ledger_account_summary_id, level_3_ledger_account_summary_name
    ,level_4_ledger_account_summary_wid, level_4_ledger_account_summary_id, level_4_ledger_account_summary_name
    ,level_5_ledger_account_summary_wid, level_5_ledger_account_summary_id, level_5_ledger_account_summary_name
    ,level_6_ledger_account_summary_wid, level_6_ledger_account_summary_id, level_6_ledger_account_summary_name
    from level5
    union
    select ledger_account_summary_wid, ledger_account_summary_id, ledger_account_summary_name
    , level_1_ledger_account_summary_wid, level_1_ledger_account_summary_id, level_1_ledger_account_summary_name
    ,level_2_ledger_account_summary_wid, level_2_ledger_account_summary_id, level_2_ledger_account_summary_name
    ,level_3_ledger_account_summary_wid, level_3_ledger_account_summary_id, level_3_ledger_account_summary_name
    ,level_4_ledger_account_summary_wid, level_4_ledger_account_summary_id, level_4_ledger_account_summary_name
    ,level_5_ledger_account_summary_wid, level_5_ledger_account_summary_id, level_5_ledger_account_summary_name
    ,level_6_ledger_account_summary_wid, level_6_ledger_account_summary_id, level_6_ledger_account_summary_name
    from level6
)
select distinct
    ledger_account_summary_wid,
    ledger_account_summary_id,
    ledger_account_summary_name,
    level_1_ledger_account_summary_wid,
    level_1_ledger_account_summary_id,
    level_1_ledger_account_summary_name,
    level_2_ledger_account_summary_wid,
    level_2_ledger_account_summary_id,
    level_2_ledger_account_summary_name,
    level_3_ledger_account_summary_wid,
    level_3_ledger_account_summary_id,
    level_3_ledger_account_summary_name,
    level_4_ledger_account_summary_wid,
    level_4_ledger_account_summary_id,
    level_4_ledger_account_summary_name,
    level_5_ledger_account_summary_wid,
    level_5_ledger_account_summary_id,
    level_5_ledger_account_summary_name,
    level_6_ledger_account_summary_wid,
    level_6_ledger_account_summary_id,
    level_6_ledger_account_summary_name,
    cast({{
        dbt_utils.surrogate_key([
            'ledger_account_summary_wid',
            'ledger_account_summary_id',
            'ledger_account_summary_name',
            'level_1_ledger_account_summary_wid',
            'level_1_ledger_account_summary_id',
            'level_1_ledger_account_summary_name',
            'level_2_ledger_account_summary_wid',
            'level_2_ledger_account_summary_id',
            'level_2_ledger_account_summary_name',
            'level_3_ledger_account_summary_wid',
            'level_3_ledger_account_summary_id',
            'level_3_ledger_account_summary_name',
            'level_4_ledger_account_summary_wid',
            'level_4_ledger_account_summary_id',
            'level_4_ledger_account_summary_name',
            'level_5_ledger_account_summary_wid',
            'level_5_ledger_account_summary_id',
            'level_5_ledger_account_summary_name',
            'level_6_ledger_account_summary_wid',
            'level_6_ledger_account_summary_id',
            'level_6_ledger_account_summary_name'
        ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from finaloutput
where
    1 = 1
order by 
level_1_ledger_account_summary_id, level_2_ledger_account_summary_id, level_3_ledger_account_summary_id
, level_4_ledger_account_summary_id, level_5_ledger_account_summary_id, level_6_ledger_account_summary_id
