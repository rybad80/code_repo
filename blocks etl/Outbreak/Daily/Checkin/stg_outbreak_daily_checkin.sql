/* This code combines employees invited to register for the daily check-in app
and employees who have registered/used the daily check-in app. */

with invited as (
    -- region cohort of invited employees.
    -- Employees who have been invited but not yet registered will have null checkin_id.
    select
        user_id as invited_user_id,
        min(datetime_sent) as invite_date
    from
        {{source('ods', 'employee_registration_email')}}
    group by
        invited_user_id
    -- end region
),
cohort as (
    -- region combining historical and new record for CHOP employee daily check-in since November 18th 2020
    select
        id as checkin_id,
        emp_id as employee_id,
        user_id as reg_user_id,
        name as checkin_employee_name,
        department,
        title,
        next_shift,
        red_flag,
        create_datetime,
        symptoms,
        location as checkin_location,
        division,
        vaccine
    from
        {{source('ods', 'emp_symptom_tracker_history')}}
    where
        create_datetime >= '11-18-2020'
    union all
    select
        id as checkin_id,
        emp_id as employee_id,
        user_id as reg_user_id,
        name as checkin_employee_name,
        department,
        title,
        next_shift,
        red_flag,
        create_datetime,
        symptoms,
        location as checkin_location,
        division,
        vaccine
    from
        {{source('ods', 'emp_symptom_tracker')}}
    where
        create_datetime >= '11-18-2020'
    -- end region
),
invited_registered as (
    -- region combining invited to registered data
    select
        invited.*,
        cohort.*
    from
        invited
        full join cohort on invited.invited_user_id = cohort.reg_user_id
    -- end region
),
add_info as (
    -- region adding location (for who only registered) and manager name from workday
    select
        invited_registered.invited_user_id,
        invited_registered.invite_date,
        invited_registered.checkin_id,
        coalesce(cast(invited_registered.employee_id as varchar(100)), add_invited.wd_worker_id) as employee_id,
        invited_registered.reg_user_id,
        coalesce(add_manager.manager_nm, add_invited.manager_nm) as manager_nm,
        coalesce(
            invited_registered.checkin_employee_name,
            add_invited.legal_reporting_nm
        ) as checkin_employee_name,
        invited_registered.department,
        invited_registered.title,
        invited_registered.next_shift,
        invited_registered.red_flag,
        invited_registered.create_datetime,
        invited_registered.symptoms,
        invited_registered.division,
        invited_registered.vaccine,
        coalesce(invited_registered.checkin_location, add_manager.loc_nm, add_invited.loc_nm) as checkin_location,
        case when checkin_id is null then 1 else 0 end as registered_only_ind
    from
        invited_registered
        left join {{source('workday', 'worker_contact_list')}} as add_manager
            on invited_registered.employee_id = add_manager.wd_worker_id
        left join {{source('workday', 'worker_contact_list')}} as add_invited
            on upper(invited_registered.invited_user_id) = add_invited.ad_login
    -- end region
),

pay_period as (
-- region cte for pulling current and previous pay period
select
    fiscal_year,
    period,
    to_date(start_dt_key, 'yyyymmdd') as pp_start_dt,
    to_date(end_dt_key, 'yyyymmdd') as pp_end_dt,
    case when (current_date >= pp_start_dt and current_date <= pp_end_dt) then 1 else 0 end as current_pp_ind,
    lead(current_pp_ind) over (order by fiscal_year, period) as prev_pp_ind
from {{source('cdw', 'master_pay_periods')}}
where fiscal_year in (2021, 2022, 2023)
order by fiscal_year, period
-- end region
)
select distinct checkin_id,
    invited_user_id,
    invite_date,
    employee_id,
    reg_user_id,
    manager_nm as manager_name,
    checkin_employee_name,
    department,
    title,
    next_shift,
    case
        when reg_user_id is not null then count(*) over (partition by reg_user_id)
    end as emp_check_count,
    case
        when red_flag is null and emp_check_count < 2 then '5' else red_flag
    end as red_flag,
    /*
        null records are for initial registration
        0 is green, 1 is red, 2 is red-manager, 3 is yellow and 4 is symptoms post-vaccine
        5 for those who are registered only, need to distinguish from the first registration
        of those who have checked in
    */
    create_datetime,
    case when red_flag in (0, 1, 2, 3, 4) then current_pp_ind else 0 end as current_pp_ind,
    case when red_flag in (0, 1, 2, 3, 4) then prev_pp_ind else 0 end as prev_pp_ind,
    symptoms,
    division,
    vaccine,
    -- region cleaning location list
    case when lower(checkin_location) in (
        'karabots pediatric care center, west philadelphia',
        'karabots primary care center, norristown') then 'Care Network-Karabots'
        when lower(checkin_location) = 'primary care, broomall' then 'Care Network-Broomall'
        when lower(checkin_location) = 'primary care, central bucks' then 'Care Network-Central Bucks'
        when lower(checkin_location) = 'primary care, chadds ford' then 'Care Network-Chadds Ford'
        when lower(checkin_location) = 'primary care, chestnut hill' then 'Care Network-Chestnut Hill'
        when lower(checkin_location) in (
            'primary care, chop campus',
            'specialty care, market street') then '3550 Market Street'
        when lower(checkin_location) = 'primary care, coatesville' then 'Care Network-Coatesville'
        when lower(checkin_location) = 'primary care, cobbs creek' then 'Care Network-Cobbs Creek'
        when lower(checkin_location) = 'primary care, drexel hill' then 'Care Network-Drexel Hill'
        when lower(checkin_location) = 'primary care, flourtown' then 'Care Network-Flourtown'
        when lower(checkin_location) = 'primary care, gibbsboro' then 'Care Network-Gibbsboro'
        when lower(checkin_location) = 'primary care, harborview/cape may' then 'Care Network-Cape May'
        when lower(checkin_location) = 'primary care, harborview/smithville' then 'Care Network-Smithville'
        when lower(checkin_location) = 'primary care, harborview/somers point' then 'Care Network-Somers Point'
        when lower(checkin_location) in (
            'care, haverford',
            'urgent care, haverford',
            'primary care, haverford') then 'Care Network - Haverford'
        when lower(checkin_location) = 'primary care, highpoint' then 'Care Network-Highpoint'
        when lower(checkin_location) = 'primary care, indian valley' then 'Care Network-Indian Valley'
        when lower(checkin_location) = 'primary care, souderton' then 'Care Network - Souderton'
        when lower(checkin_location) = 'primary care, kennett square' then 'Care Network-Kennett Sq'
        when lower(checkin_location) = 'primary care, media' then 'Care Network -Media Granite Run'
        when lower(checkin_location) = 'primary care, mount laurel' then 'Care Network-Mt Laurel'
        when lower(checkin_location) = 'primary care, moorsetown' then 'Care Network - Moorestown'
        when lower(checkin_location) = 'primary care, newtown' then 'Care Network - Newtown'
        when lower(checkin_location) = 'primary care, paoli' then 'Care Network-Paoli'
        when lower(checkin_location) = 'primary care, pottstown' then 'Care Network-Pottstown'
        when lower(checkin_location) = 'primary care, roxborough' then 'Care Network-Roxborough'
        when lower(checkin_location) = 'primary care, salem road (burlington township)'
            then 'Care Network-Salem Road'
        when lower(checkin_location) = 'primary care, south philadelphia' then 'Care Network-South Phila'
        when lower(checkin_location) = 'primary care, springfield' then 'Care Network - Springfield'
        when lower(checkin_location) = 'primary care, west chester' then 'Care Network-West Chester'
        when lower(checkin_location) = 'primary care, west grove' then 'Care Network-West Grove'
        when lower(checkin_location) = 'buerger center for advanced pediatric care'
            then 'Buerger Center Ambulatory Care'
        when lower(checkin_location) in (
            'cardiac center, allentown',
            'pediatric cardiologists in allentown, pa') then 'Allentown'
        when lower(checkin_location) in (
            'gender and sexuality development program voorhees',
            'specialty care & surgery center, voorhees',
            'pediatric cardiologists in voorhees, nj') then 'Voorhees Specialty Care'
        when lower(checkin_location) in (
            'neonatal follow-up program at virtua',
            'pediatric cardiologists at virtua',
            'specialty care, virtua') then 'Specialty Care CHOP at Virtua'
        when lower(checkin_location) in (
            'pediatric imaging center at specialty care, king of prussia',
            'specialty care & surgery center, king of prussia',
            'urgent care, king of prussia, pa',
            'hematology, specialty care and surgery center, king of prussia',
            'outpatient oncology, specialty care & surgery center, king of prussia',
            'pediatric cardiologists in king of prussia, pa') then 'King of Prussia Spec Care'
        when lower(checkin_location) = 'perelman center for advanced medicine' then 'The Perelman Center'
        when lower(checkin_location) = 'psychiatry and behavioral science in university city, pa'
            then '3440 Market Street'
        when lower(checkin_location) in (
            'specialty care & surgery center, brandywine valley',
            'urgent care, brandywine valley',
            'pediatric cardiologists in brandywine valley, pa') then 'Brandywine Valley Spec Care'
        when lower(checkin_location) in (
            'specialty care & surgery center, bucks county',
            'urgent care, bucks county, pa',
            'pediatric cardiologists in bucks county, pa') then 'Bucks County Specialty Care'
        when lower(checkin_location) in (
            'specialty care, abington',
            'pediatric cardiologists in abington, pa') then 'Abington Specialty Care'
        when lower(checkin_location) in (
            'specialty care, atlantic county',
            'pediatric cardiologists in atlantic county, nj') then 'Atlantic County Specialty Care'
        when lower(checkin_location) in (
            'specialty care, exton',
            'pediatric cardiologists in exton, pa') then 'Exton Specialty Care'
        when lower(checkin_location) in (
            'specialty care, lancaster',
            'pediatric cardiologists in lancaster, pa') then 'Lancaster Specialty Care Center'
        when lower(checkin_location) in (
            'specialty care, pediatric cardiology at saint peter''s university hospital', --noqa: PRS, L048
            'pediatric cardiologists in saint peter''s university hospital') --noqa: PRS, L048
                then 'St. Peters Hospital Cardiac'
        when lower(checkin_location) in (
            'specialty care, princeton at plainsboro',
            'pediatric cardiologists in princeton at plainsboro, nj')
                then 'Princeton Plainsboro Spec Care'
        when lower(checkin_location) = 'working remotely (awa)'
            then 'Home/Remote Office checkin_location'
        when lower(checkin_location) = 'colket translational research building'
            then 'Colket Translational Research'
        when lower(checkin_location) = 'main campus' then 'Main Hospital'
        else checkin_location
    end as checkin_location_cleaned,
    -- end region
    -- region creating indicator to create timeframe filtering
    case
        when create_datetime >= current_date - interval '1 day' and red_flag in (0, 1, 2, 3, 4) then 1 else 0
    end as checkin_last_24_hr_ind,
    case
        when create_datetime >= current_date - interval '2 days'
            and create_datetime <= current_date - interval '1 day'
            and red_flag in (0, 1, 2, 3, 4) then 1 else 0
    end as checkin_last_24_to_48_ind,
    case
        when create_datetime >= current_date - interval '7 days'
            and red_flag in (0, 1, 2, 3, 4) then 1 else 0
    end as checkin_last_week_ind,
    case
        when create_datetime >= current_date - interval '30 days'
            and red_flag in (0, 1, 2, 3, 4) then 1 else 0
    end as checkin_last_month_ind
    -- end region
from
    add_info
    left join pay_period
        on date_trunc('day', add_info.create_datetime)
            between pay_period.pp_start_dt and pay_period.pp_end_dt
