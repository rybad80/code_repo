with medication_route_details as (
--region medication details at the route granularity
    select
        cohort.visit_key,
        lookup_medication.event_name || '_administered' as event_name,
--        medication_order_administration.medication_order_id,
        case when
            lookup_order_route.route_group = 'Not Applicable'
            then lookup_admin_route.route_group
            else lookup_order_route.route_group
        end as med_route,
        min(medication_order_administration.administration_date) as med_start_date,
        max(medication_order_administration.administration_date) as med_end_date,
        case when
            med_start_date = med_end_date then 1 --single instance
            else extract(epoch from med_end_date - med_start_date) / 60
        end as medication_duration
    from
        {{ref('stg_ed_encounter_cohort_all')}} as cohort
        inner join {{ref('medication_order_administration')}} as medication_order_administration
            on cohort.visit_key = medication_order_administration.visit_key
        inner join {{ ref('lookup_ed_events_medication_order_administration') }} as lookup_medication
            on (upper(medication_order_administration.medication_name) like lookup_medication.pattern
            or upper(medication_order_administration.generic_medication_name) like lookup_medication.pattern)
        inner join {{ source('cdw', 'department') }} as department
            on medication_order_administration.medication_administration_dept_key = department.dept_key
        inner join {{ source('clarity_ods', 'clarity_dep') }} as clarity_dep
          on department.dept_id = clarity_dep.department_id
        left join {{ ref('lookup_ed_events_medication_route') }} as lookup_order_route
            on lookup_order_route.route = medication_order_administration.order_route
        left join {{ ref('lookup_ed_events_medication_route') }} as lookup_admin_route
            on lookup_admin_route.route = medication_order_administration.admin_route
    where
        clarity_dep.dep_ed_type_c in (
          1, -- Emergency Department
          3  -- Observation
        )
        and medication_order_administration.administration_date is not null --performed
        and medication_order_administration.administration_date <= cohort.disch_ed_dt
    group by
        cohort.visit_key,
        lookup_medication.event_name,
--        medication_order_administration.medication_order_id,
        med_route
    having
        medication_duration > 0

    union all

    select
        cohort.visit_key,
        lookup_medication.event_name || '_discharge_med' as event_name,
--        medication_order_administration.medication_order_id,
        case when
            lookup_order_route.route_group = 'Not Applicable'
            then lookup_admin_route.route_group
            else lookup_order_route.route_group
        end as med_route,
        max(medication_order_administration.medication_start_date) as med_start_date,
        min(medication_order_administration.medication_end_date) as med_end_date,
        extract(epoch from med_end_date - med_start_date) / 60 as medication_duration
    from
        {{ref('stg_ed_encounter_cohort_all')}} as cohort
        inner join {{ref('medication_order_administration')}} as medication_order_administration
            on cohort.visit_key = medication_order_administration.visit_key
        inner join {{ ref('lookup_ed_events_medication_order_administration') }} as lookup_medication
            on (upper(medication_order_administration.medication_name) like lookup_medication.pattern
            or upper(medication_order_administration.generic_medication_name) like lookup_medication.pattern)
        left join {{ ref('lookup_ed_events_medication_route') }} as lookup_order_route
            on lookup_order_route.route = medication_order_administration.order_route
        left join {{ ref('lookup_ed_events_medication_route') }} as lookup_admin_route
            on lookup_admin_route.route = medication_order_administration.admin_route
    where
        medication_order_administration.discharge_med_ind = 1
        and medication_order_administration.order_class != 'Historical Med'
    group by
        cohort.visit_key,
        lookup_medication.event_name,
--        medication_order_administration.medication_order_id,
        med_route
    having
        medication_duration > 0
--end region
)

--medication granularity
select
    visit_key,
    'medication_details' as event_category,
    event_name || '_route' as event_name,
    'medication_order_administration' as event_source,
    med_start_date as event_timestamp,
    cast(group_concat(med_route) as varchar(200)) as meas_val,
    1 as event_repeat_number
from
    medication_route_details
group by
    visit_key,
    event_name,
    event_timestamp

union

select
    visit_key,
    'medication_details' as event_category,
    event_name || '_duration' as event_name,
    'medication_order_administration' as event_source,
    med_start_date as event_timestamp,
    cast(sum(medication_duration) as varchar(200)) as meas_val,
    1 as event_repeat_number
from
    medication_route_details
group by
    visit_key,
    event_name,
    event_timestamp
