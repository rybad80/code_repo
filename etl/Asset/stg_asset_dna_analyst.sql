with
analyst as (
    select
        worker_wid,
        manager_worker_wid
    from
        {{ ref('worker') }}
    where
        cost_center_id = 13530 -- DnA Cost Center
        or manager_id = '3353' -- manager is 'Kennedy, Andrea T'
),
coop as (
    select
        worker_wid
    from
        {{ ref('worker') }}
    where
        ad_login in (
            'mcdougallj',
            'rahmani1',
            'zhangt11'
        )

),
expat as (
    select
        worker_wid
    from
        {{ ref('worker') }}
    where
        ad_login in (
            'bruhnr',
            'burkpappas',
            'dixith',
            'kauffmane',
            'keren',
            'luan',
            'maduc',
            'rahmingn',
            'porterej',
            'sagdeon',
            'schmuckern',
            'wildenhaip'
        )
),
manager as (
    select
        worker.worker_wid
    from
        analyst
        inner join {{ ref('worker') }} as worker on analyst.manager_worker_wid = worker.worker_wid
    group by
        worker.worker_wid
)
select
    employee.worker_id,
    employee.ad_login,
    employee.preferred_reporting_name,
    employee.worker_type,
    employee.employee_ind,
    employee.active_ind,
    case when expat.worker_wid is not null then 1 else 0 end as expat_ind,
    employee.manager_name,
    employee.manager_id,
    employee.cost_center_name,
    employee.cost_center_id,
    employee.job_family,
    employee.job_family_id,
    employee.reporting_chain
from
    {{ ref('worker') }} as employee
    left join analyst on analyst.worker_wid = employee.worker_wid
    left join coop on coop.worker_wid = employee.worker_wid
    left join manager on manager.worker_wid = employee.worker_wid
    left join expat on expat.worker_wid = employee.worker_wid
where
    coalesce(analyst.worker_wid, coop.worker_wid, manager.worker_wid, expat.worker_wid) is not null
    or employee.ad_login in ('com', 'jettc') -- left
