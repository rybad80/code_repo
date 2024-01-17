with med_admins as (
    select
        medication_order_administration.visit_key,
        medication_order_administration.med_ord_key as action_key,
        medication_order_administration.administration_seq_number as action_seq_num,
        medication_administration.pri_emp_key as emp_key,
        medication_order_administration.administration_date as event_date,
        medication_order_administration.administration_department as event_location,
        row_number() over (
            partition by
                medication_order_administration.visit_key,
                medication_administration.pri_emp_key,
                medication_order_administration.administration_date,
                medication_order_administration.administration_department
            order by action_key desc, action_seq_num desc
        ) as medication_line
    from
        {{ref('medication_order_administration')}} as medication_order_administration
        inner join {{source('cdw', 'medication_administration')}} as medication_administration
            on medication_administration.med_ord_key = medication_order_administration.med_ord_key
            and medication_administration.seq_num = medication_order_administration.administration_seq_number
    where
        medication_order_administration.administration_type_id in (
                    1, --given
                    6, --new bag
                    7, --restarted
                    9, --rate change
                    12, --bolus
                    13, --push
                    102, --pt/caregiver admin - non high alert
                    103, --pt/caregiver admin - high alert
                    105, --given by other
                    106, --new syringe
                    112, --iv started
                    115, --iv restarted
                    116, --divided dose
                    117, --started by other
                    119, --neb restarted
                    122.0020, --performed
                    123, --added to bicarbonate concentrate
                    127, --bolus from bag/bottle/syringe
                    131, --'pump association'
                    135, --patch applied
                    139, --instill
                    141 --gravity/alternate infusion method
         )
         and medication_order_administration.administration_date > '01-01-2013'
)

select
    med_admins.visit_key,
    med_admins.action_key,
    med_admins.action_seq_num,
    employee.prov_key,
    employee.emp_key,
    employee.full_nm as employee_name,
    med_admins.event_date,
    'medication adminstered' as event_description,
    med_admins.event_location,
    'med_ord_key' as action_key_field
from
    med_admins
    inner join {{source('cdw', 'employee')}} as employee
        on employee.emp_key = med_admins.emp_key
where
    med_admins.medication_line = 1
