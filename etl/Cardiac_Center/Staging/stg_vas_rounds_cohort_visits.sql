select distinct cohort.patient_name,
cohort.mrn,
cohort.pat_key,
cohort.visit_key,
cohort.hospital_admit_date,
cohort.hospital_discharge_date,
cohort.csn_number,
adt_bed.bed_name as current_cicu_bed_name
from {{ ref('stg_vas_rounds_cohort') }}  as cohort
left join {{ ref('adt_bed') }} as adt_bed on cohort.visit_key = adt_bed.visit_key
and department_group_name like '%CICU%'
and adt_bed.exit_date is null
