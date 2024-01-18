{{ config(meta = {
    'critical': true
}) }}

with subordinate_worker_row as (
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl1_emp_key as superior_emp_key,
        1 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl2_emp_key as superior_emp_key,
        2 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl2_emp_key > 0
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl3_emp_key as superior_emp_key,
        3 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl3_emp_key > 0
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl4_emp_key as superior_emp_key,
        4 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl4_emp_key > 0
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl5_emp_key as superior_emp_key,
        5 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl5_emp_key > 0
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl6_emp_key as superior_emp_key,
        6 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl6_emp_key > 0
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl7_emp_key as superior_emp_key,
        7 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl7_emp_key > 0
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl8_emp_key as superior_emp_key,
        8 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl8_emp_key > 0
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl9_emp_key as superior_emp_key,
        9 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl9_emp_key > 0
    union
    select
        wd_worker_id as worker_id,
        mgr_emp_key,
        lvl10_emp_key as superior_emp_key,
        10 as superior_lvl
    from {{ref('stg_worker_management_level')}}
    where lvl10_emp_key > 0
)

    select
        worker_id,
        mgr_emp_key,
        superior_emp_key,
        superior_lvl,
        case when mgr_emp_key = superior_emp_key
            then 1 else 0 end as direct_supervisor_ind
    from subordinate_worker_row
    group by
        worker_id,
        mgr_emp_key,
        superior_emp_key,
        superior_lvl,
        case when mgr_emp_key = superior_emp_key
        then 1 else 0 end
