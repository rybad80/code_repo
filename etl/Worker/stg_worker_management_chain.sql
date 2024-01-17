{{ config(meta = {
    'critical': true
}) }}

with
mgmt_level_sort as (
--this is a sort order; there is no intelligence reasoning here,
--from HR perspective, zeroes and ones are identical as per Maria Tickner
    select
        management_level,
        case management_level
            when 'CEO' then 5
            when 'EVP' then 10
            when 'Department Chair' then 11
            when 'SVP' then 20
            when 'VP' then 30
            when 'AVP / SrDir / Dir' then 40
            when 'Division Chief / Assoc Chair' then 41
            when 'Mid-Management' then 50
            when 'MedDir / PgmDir / Assoc Chief' then 51
            when 'Supv / Team Lead' then 70
            when 'Individual Contributor' then 80 else 9999
        end as management_level_sort_num,
        sum(active_ind) as active_worker_row_cnt,
        row_number() over (
            order by management_level_sort_num desc --noqa: L028
        ) as management_level_rank
    from
        {{ref('worker')}}
    group by
        management_level,
        management_level_sort_num
),
cno_leader as (
    select
        workday_emp_key as cno_emp_key,
        display_name || ', ' || job_title as cno_display
    from
        {{ref('worker')}}
    where
        job_title like '%System Chief Nursing Officer%'
        and active_ind = 1
    /* Paula Agosto as of 2/3/2022:  10167 SVP & System Chief nursing_ Officer */
),
mgmt_chain_attributes as (
    select
        stg_worker_management_attributes.*,
        lvl3_info.management_level as lvl_03_management_level,
        lvl4_info.management_level as lvl_04_management_level,
        lvl5_info.management_level as lvl_05_management_level,
        lvl6_info.management_level as lvl_06_management_level,
        lvl4_info.nurse_mgmt_sort_num as nursing_lvl_04_sort_num,
        lvl5_info.nurse_mgmt_sort_num as nursing_lvl_05_sort_num,
        lvl6_info.nurse_mgmt_sort_num as nursing_lvl_06_sort_num,
        coalesce(
            lvl5_info.acno_direct_report_ind,
            lvl6_info.acno_direct_report_ind,
            0
        ) as in_acno_org_ind,
        case
            when cno_leader.cno_emp_key is null then 'No' else 'Yes'
        end as in_cno_org,
        case
            when cno_leader.cno_emp_key is null then 0 else 1
        end as in_cno_org_ind,
        case
            when
                cno_leader.cno_emp_key is null then 'not CNO org'
            else 'under CNO'
        end as cno_org_display,
        coalesce((coalesce(lvl9_info.management_level_rank, 0) * 1)
            + (coalesce(lvl8_info.management_level_rank, 0) * 10)
            + (coalesce(lvl7_info.management_level_rank, 0) * 100)
            + (coalesce(lvl6_info.management_level_rank, 0) * 1000)
            + (coalesce(lvl5_info.management_level_rank, 0) * 10000)
            + (coalesce(lvl4_info.management_level_rank, 0) * 100000)
            + (coalesce(lvl3_info.management_level_rank, 0) * 1000000)
            + (coalesce(lvl2_info.management_level_rank, 0) * 10000000), 0
        ) as rank_chain_by_mgmt_level
    from
        {{ref('stg_worker_management_attributes')}} as stg_worker_management_attributes
        left join {{ref('stg_worker_management_sort')}} as lvl2_info
            on stg_worker_management_attributes.lvl2_emp_key = lvl2_info.mgmt_emp_key
        left join {{ref('stg_worker_management_sort')}} as lvl3_info
            on stg_worker_management_attributes.lvl3_emp_key = lvl3_info.mgmt_emp_key
        left join {{ref('stg_worker_management_sort')}} as lvl4_info
            on stg_worker_management_attributes.lvl4_emp_key = lvl4_info.mgmt_emp_key
        left join {{ref('stg_worker_management_sort')}} as lvl5_info
            on stg_worker_management_attributes.lvl5_emp_key = lvl5_info.mgmt_emp_key
        left join {{ref('stg_worker_management_sort')}} as lvl6_info
            on stg_worker_management_attributes.lvl6_emp_key = lvl6_info.mgmt_emp_key
        left join {{ref('stg_worker_management_sort')}} as lvl7_info
            on stg_worker_management_attributes.lvl7_emp_key = lvl7_info.mgmt_emp_key
        left join {{ref('stg_worker_management_sort')}} as lvl8_info
            on stg_worker_management_attributes.lvl8_emp_key = lvl8_info.mgmt_emp_key
        left join {{ref('stg_worker_management_sort')}} as lvl9_info
            on stg_worker_management_attributes.lvl9_emp_key = lvl9_info.mgmt_emp_key
        left join cno_leader on cno_leader.cno_emp_key in
            (
                stg_worker_management_attributes.lvl2_emp_key,
                stg_worker_management_attributes.lvl3_emp_key,
                stg_worker_management_attributes.lvl4_emp_key,
                stg_worker_management_attributes.lvl5_emp_key
            )
),
mgmt_chain_attributes_mgmt_level_sort as (
    select
        mgmt_chain_attributes.*,
        case when mgmt_chain_attributes.worker_management_level = 'CEO' then 0
            when mgmt_chain_attributes.worker_active_ind = 0 then 999999999
            else
                dense_rank() over (
                    partition by
                        drill_mgmt_l01
                    order by mgmt_chain_attributes.rank_chain_by_mgmt_level
                )
        end as sort_by_mgmt_level
    from
        mgmt_chain_attributes
        left join mgmt_level_sort
            on mgmt_chain_attributes.worker_management_level = mgmt_level_sort.management_level
)
select
    worker_management_level,
    legal_reporting_name,
    worker_active_ind,
    worker_id,
    clarity_emp_key,
    prov_key,
    ad_login,
    emp_key,
    full_drill_mgmt_path,
    drill_mgmt_l01,
    wd_worker_id,
    reporting_level,
    full_names_reporting_chain,
    id_reporting_chain_ceo_to_worker,
    lvl1_emp_key,
    lvl2_emp_key,
    lvl3_emp_key,
    lvl4_emp_key,
    lvl5_emp_key,
    lvl6_emp_key,
    lvl7_emp_key,
    lvl8_emp_key,
    lvl9_emp_key,
    lvl10_emp_key,
    lvl_01_reporting_nm,
    lvl_02_reporting_nm,
    lvl_03_reporting_nm,
    lvl_04_reporting_nm,
    lvl_05_reporting_nm,
    lvl_06_reporting_nm,
    lvl_07_reporting_nm,
    lvl_08_reporting_nm,
    lvl_09_reporting_nm,
    lvl_10_reporting_nm,
    lvl_03_management_level,
    lvl_04_management_level,
    lvl_05_management_level,
    lvl_06_management_level,
    nursing_lvl_04_sort_num,
    nursing_lvl_05_sort_num,
    nursing_lvl_06_sort_num,
    in_acno_org_ind,
    in_cno_org,
    in_cno_org_ind,
    cno_org_display,
    rank_chain_by_mgmt_level,
    sort_by_mgmt_level
from mgmt_chain_attributes_mgmt_level_sort
