{{ config(meta = {
    'critical': true
}) }}

with mgmt_level_sort as (
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

acno_leader as (
    /* Associate Chief Nursing Officer who report to the CNO */
    select
        worker.workday_emp_key as acno_emp_key,
        worker.display_name || ', ' || worker.job_title as acno_display
    from
        {{ref('worker')}} as worker
    --left join
    --    cno_leader on worker.workday_emp_key = cno_leader.cno_emp_key
    where worker.job_title like '%Associate Chief Nursing Officer%'
        and worker.active_ind = 1
      --  and cno_leader.cno_emp_key is null
),

employee_nm_sort_num as (
    /* this is needed because in combining the management level and name for sort,
    we need to turn it into a number for Qlik Sense's sort by expression */
    select
        orderbynm.mgmt_emp_key,
        orderbynm.management_level as management_level,
        orderbynm.nm_sort_num * case
            when cno_leader.cno_emp_key is null then 10 else 1
        end
        /* cno factor push the level 3 cno to top  */
        * case when acno_leader.acno_emp_key is null then 20 else 1 end
        /* acno factor, push the level 4 acnos to top */
        as mgr_nm_sort_num,
        case when cno_leader.cno_emp_key is null then '' else 'cno-' end
        || case when acno_leader.acno_emp_key is null then '' else 'acno-' end
        || orderbynm.management_level as nursing_management_level
    from
        (
            select
                manager_sub.emp_key as mgmt_emp_key,
                employee.management_level,
                row_number() over
                    (
                        order by
                            employee.legal_reporting_nm,
                            employee.emp_id
                    ) as nm_sort_num
            from
                (
                    select
                        manager.emp_key
                    from
                        {{source('cdw', 'manager')}} as manager
                    where
                        manager.active_ind = 1
                    group by
                        manager.emp_key
                ) as manager_sub
        inner join
            {{source('cdw', 'employee')}} as employee
            on manager_sub.emp_key = employee.emp_key) as orderbynm
        left join cno_leader
            on orderbynm.mgmt_emp_key = cno_leader.cno_emp_key
        left join acno_leader
            on orderbynm.mgmt_emp_key = acno_leader.acno_emp_key
)
select
    worker.workday_emp_key as mgmt_emp_key,
    mgmt_level_sort.management_level_sort_num,
    mgmt_level_sort.management_level_rank,
    employee_nm_sort_num.nursing_management_level,
    employee_nm_sort_num.management_level,
    employee_nm_sort_num.mgr_nm_sort_num,
    case
        when cno_leader.cno_emp_key is null then 1000 else 1
    end as cno_list_factor,
    case
        when cno_leader.cno_emp_key is null then null else 1
    end as cno_direct_report_ind,
    case
        when acno_leader.acno_emp_key is null then 5000 else 10
    end as acno_list_factor,
    case
        when acno_leader.acno_emp_key is null then null else 1
    end as acno_direct_report_ind,
    mgmt_level_sort.management_level_sort_num * cno_list_factor
        * acno_list_factor::float8 as nurse_mgmt_sort_num
from
    {{ref('worker')}} as worker
    left join employee_nm_sort_num on
        worker.workday_emp_key = employee_nm_sort_num.mgmt_emp_key
    inner join {{source('cdw', 'manager')}} as manager
        on worker.mgr_key = manager.mgr_key
    left join mgmt_level_sort
        on worker.management_level = mgmt_level_sort.management_level
    left join cno_leader
        on manager.emp_key = cno_leader.cno_emp_key
    left join acno_leader
        on manager.emp_key = acno_emp_key
