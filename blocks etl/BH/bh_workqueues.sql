with
first_last_release as (
select
    referral_wq_items.referral_id,
    referral_wq_items.item_id,
    referral_wq.workqueue_name,
    referral.pat_id,
    referral_wq_usr_hx.start_instant_dttm as release_start_dt,
    zc_rfl_status.title as referral_status,
    referral.referring_prov_id as referred_from_provider_id,
    referring_prov.full_name as referred_from_provider_name,
    referral.referral_prov_id as referred_to_provider_id,
    referred_prov.full_name as referred_to_provider_name,
    referral.refd_to_dept_id as referred_to_dept_id,
    referred_dept_nm.department_name as referred_to_department_name,
    referred_dept_nm.location_name  as referred_to_location_name,
    referral.refd_by_dept_id as referred_from_dept_id,
    referring_dept_nm.department_name as referred_from_department_name,
    referring_dept_nm.location_name  as referred_from_location_name,
    case when referral_wq_usr_hx.history_activity_c = 1
        then referral_wq_usr_hx.start_instant_dttm
    end as entry_start_dt,
    case when referral_wq_usr_hx.history_activity_c =  3
        then row_number() over(partition by referral_wq_items.referral_id, referral_wq.workqueue_id
                                order by release_start_dt desc)
    end as latest_release,
    case when referral_wq_usr_hx.history_activity_c = 1
        then row_number() over( partition by referral_wq_items.referral_id, referral_wq.workqueue_id
                                order by entry_start_dt desc)
    end as initial_entry_ind
from {{source('clarity_ods', 'referral_wq')}} as  referral_wq
left join  {{source('clarity_ods', 'referral_wq_items')}} as referral_wq_items
     on referral_wq_items.workqueue_id  = referral_wq.workqueue_id
left join   {{source('clarity_ods', 'referral_wq_usr_hx')}} as referral_wq_usr_hx
	on referral_wq_usr_hx.item_id = referral_wq_items.item_id
left join {{source('clarity_ods', 'referral')}} as referral
	on referral_wq_items.referral_id  = referral.referral_id
left join  {{source('clarity_ods', 'zc_rfl_status')}} as zc_rfl_status
	on referral.rfl_status_c = zc_rfl_status.rfl_status_c
left join {{ref('stg_department_all')}} as referred_dept_nm
	on referral.refd_to_dept_id = referred_dept_nm.department_id
left join  {{ref('stg_department_all')}} as referring_dept_nm
	on referral.refd_by_dept_id  = referring_dept_nm.department_id
left join {{ref('dim_provider')}} as referred_prov
	on referral.referral_prov_id = referred_prov.prov_id
left join {{ref('dim_provider')}} as referring_prov
	on referral.referring_prov_id  = referring_prov.prov_id
where (referral_wq.workqueue_name  like 'DCAPBS%' or referral_wq.workqueue_name = 'Referral to BE-WEHL')
and referral_wq_usr_hx.history_activity_c in (1, 2, 3) -- Release, Reentry, entry

),



first_last_sum as (
select
    referral_id,
    item_id,
    pat_id,
    workqueue_name,
    referral_status,
    referred_from_provider_id,
    referred_from_provider_name,
    referred_to_provider_id,
    referred_to_provider_name,
    referred_to_dept_id,
    referred_to_department_name,
    referred_to_location_name,
    referred_from_dept_id,
    referred_from_department_name,
    referred_from_location_name,
    max(latest_release) as latest_release,
    max(case when latest_release = 1 and referral_status in  (
                    'CLOSED',
                    'CANCELED',
                    'DENIED',
                    'ADMINISTRATIVELY DENIED',
                    'PRECERT NOT NEEDED',
                    'SINGLE CASE AGREEMENT'
                ) then release_start_dt
        end
    ) as referral_closed_date,
    max(case when initial_entry_ind = 1 then release_start_dt end) as referral_entry_date
from first_last_release
group by
    referral_id,
    item_id,
    pat_id,
    workqueue_name,
    referral_status,
    referred_from_provider_id,
    referred_from_provider_name,
    referred_to_provider_id,
    referred_to_provider_name,
    referred_to_dept_id,
    referred_to_department_name,
    referred_to_location_name,
    referred_from_dept_id,
    referred_from_department_name,
    referred_from_location_name

)



select
    referral_id,
    item_id,
    pat_id,
    workqueue_name,
    referral_status,
    referred_from_provider_id,
    referred_from_provider_name,
    referred_to_provider_id,
    referred_to_provider_name,
    referred_to_dept_id,
    referred_to_department_name,
    referred_to_location_name,
    referred_from_dept_id,
    referred_from_department_name,
    referred_from_location_name,
    latest_release,
    referral_closed_date,
    referral_entry_date,
    case when latest_release is not null then referral_entry_date end as referral_release_date,
    case  when referral_status in  (
                'CLOSED',
                'CANCELED',
                'DENIED',
                'ADMINISTRATIVELY DENIED',
                'PRECERT NOT NEEDED',
                'SINGLE CASE AGREEMENT'
            ) then 1  else 0
    end as referral_closed_ind,
    case
        when referral_closed_ind = 1 then (date(referral_closed_date) - date(referral_entry_date)) else null
    end as days_to_close
from first_last_sum
