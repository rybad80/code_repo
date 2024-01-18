-- RUNTIME_S   QH_ESTCOST  LOG_COST    QH_SNIPPETS QH_ESTMEM   N_CHAR
-- 00:00:04    352         2.546542    9           0           1906

with cohort as (
select
    mrn,
    patient_name,
    pat_key
from {{ref('stg_usnews_pulm_asthma_base')}}
where primary_dx_ind = 1
group by
    mrn,
    patient_name,
    pat_key
),
/* cancer dx: based on stg_cancer_center_visit */
cancer_pat as (
select
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    cancer_center_visit.visit_date,
--    min(cancer_center_visit.visit_date) as dx_first_encounter_date, -- first visit patient had with cancer center
    'cancer' as exc_cohort,
    '1' as date_cutoff_ind
from
    cohort
    inner join {{ref('cancer_center_visit')}} as cancer_center_visit
        on cohort.pat_key = cancer_center_visit.pat_key
group by
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    cancer_center_visit.visit_date,
    exc_cohort,
    date_cutoff_ind
),
/* transplant: patients receiving any transplants, based on stack transplant_recipients */
transplant_pat as (
select
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    transplant_recipients.transplant_date,
    transplant_recipients.reason_removed,
    'transplant' as exc_cohort,
    '0' as date_cutoff_ind
from
    cohort
    inner join {{ref('transplant_recipients')}} as transplant_recipients
        on cohort.mrn = transplant_recipients.mrn
where transplant_date is not null
group by
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    transplant_recipients.transplant_date,
    transplant_recipients.reason_removed,
    exc_cohort,
    date_cutoff_ind
),
/* chromosomal abnormality: patients on registry_patient_chroma_ab,
* accessible via cardiac_registry_patient stack */
chrom_ab_pat as (
select
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    cardiac_registry_patient.last_contact_date,
    cardiac_registry_patient.last_contact_method,
    'chrom ab' as exc_cohort,
    '0' as date_cutoff_ind
from
    cohort
    inner join {{ref('cardiac_registry_patient')}} as cardiac_registry_patient
        on cohort.mrn = cardiac_registry_patient.mrn
where
    lower(chromosomal_ab) not like '%no chromosomal abnormality identified%'
    and lower(chromosomal_ab) not like '%no chromosomal or genetic abnormality identified%'
group by
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    cardiac_registry_patient.last_contact_date,
    cardiac_registry_patient.last_contact_method,
    exc_cohort,
    date_cutoff_ind
)
select
    mrn,
    patient_name,
    pat_key,
    visit_date as index_date,
    exc_cohort,
    date_cutoff_ind
from cancer_pat
union all
select
    mrn,
    patient_name,
    pat_key,
    transplant_date as index_date,
    exc_cohort,
    date_cutoff_ind
from transplant_pat
union all
select
    mrn,
    patient_name,
    pat_key,
    last_contact_date as index_date,
    exc_cohort,
    date_cutoff_ind
from chrom_ab_pat
