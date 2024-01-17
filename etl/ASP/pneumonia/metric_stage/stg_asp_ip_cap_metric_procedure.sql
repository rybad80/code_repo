select
    asp_ip_cap_cohort.visit_key,
    1 as cap_pathway_48_hrs_ind
from
    {{ ref('asp_ip_cap_cohort')}} as asp_ip_cap_cohort
    inner join {{ ref('procedure_order_clinical')}} as procedure_order_clinical
        on asp_ip_cap_cohort.visit_key = procedure_order_clinical.visit_key
where
    procedure_order_clinical.procedure_id = 94550 --Initiate Pneumonia Pathway
    and procedure_order_clinical.placed_date
        < asp_ip_cap_cohort.hospital_admit_date + interval('48 hours')
group by
    asp_ip_cap_cohort.visit_key
