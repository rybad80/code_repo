--region encounters where chest x-ray or ct scan performed 48 hours prior to or after admission
select
    stg_asp_ip_cap_cohort.visit_key
from
    {{ref('stg_asp_ip_cap_cohort')}} as stg_asp_ip_cap_cohort
    inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
        on stg_asp_ip_cap_cohort.pat_key = procedure_order_clinical.pat_key
where
    --patient had chest x-ray or ct scan within 48 hours prior to or after admission
    procedure_order_clinical.procedure_name like '%CHEST%'
    and regexp_like(
        procedure_order_clinical.procedure_name,
        'XR|X(.)?RAY|TOMO|^CT|[^[:alpha:]]CT' --xr, x-ray, tomography, or a word beginning with CT
    )
    --hours_between returns absolute value
    --use procedure date if known, otherwise use placed date
    and hours_between(
        stg_asp_ip_cap_cohort.hospital_admit_date,
        coalesce(procedure_order_clinical.specimen_taken_date, procedure_order_clinical.placed_date)
    ) < 48
    and procedure_order_clinical.order_status in(
        'Completed',
        'Sent'
    )
group by
    stg_asp_ip_cap_cohort.visit_key
