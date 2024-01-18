with anesthesia_raw as (
    select
        stg_nicu_chnd_timestamps.log_key,
        medication_order_administration.medication_name,
        sum(admin_dose::numeric(8, 2)) as dose
    from
        {{ ref('stg_nicu_chnd_timestamps') }} as stg_nicu_chnd_timestamps
        inner join {{ ref('medication_order_administration') }} as medication_order_administration
            on stg_nicu_chnd_timestamps.anes_visit_key = medication_order_administration.visit_key
    where
        medication_order_administration.administration_type_id = '1' --GIVEN
        and (
            lower(medication_order_administration.medication_name) like '%dexamethasone%'
            or lower(medication_order_administration.medication_name) like '%hydrocortisone%'
        )
    group by
        stg_nicu_chnd_timestamps.log_key,
        medication_order_administration.medication_name

    union all

    select
        stg_nicu_chnd_timestamps.log_key,
        medication_order_administration.medication_name,
        sum(admin_dose::numeric(8, 2)) as dose
    from
        {{ ref('stg_nicu_chnd_timestamps') }} as stg_nicu_chnd_timestamps
        inner join {{ ref('medication_order_administration') }} as medication_order_administration
            on stg_nicu_chnd_timestamps.visit_key = medication_order_administration.visit_key
        inner join {{ source('cdw', 'medication_order') }} as medication_order
            on medication_order_administration.med_ord_key = medication_order.med_ord_key
        inner join {{ source('cdw', 'department')}} as department --join on login department key
            on medication_order.login_dept_key = department.dept_key
    where
        medication_order_administration.administration_type_id = '1' --GIVEN
        and department.dept_id = 101001507 --INP MED/SURG/NEONAT
        and (
            lower(medication_order_administration.medication_name) like '%dexamethasone%'
            or lower(medication_order_administration.medication_name) like '%hydrocortisone%'
        ) and medication_order_administration.administration_date
        between stg_nicu_chnd_timestamps.anesthesia_start_date and stg_nicu_chnd_timestamps.anesthesia_stop_date
    group by
        stg_nicu_chnd_timestamps.log_key,
        medication_order_administration.medication_name
),

anesthesia_values as (
    select
        log_key,
        medication_name,
        dose,
        count(*) over (
            partition by log_key
        ) as count_num
    from
        anesthesia_raw
)

select
    log_key,
    max(case
        when count_num = 1 then medication_name
        when count_num = 2 then 'MULTIPLE STEROIDS ADMINISTERED'
    end) as anesthesia_med,
    max(case
        when count_num = 1 then dose
        when count_num = 2 then 0
    end) as total_dose
from
    anesthesia_values
group by
    log_key
