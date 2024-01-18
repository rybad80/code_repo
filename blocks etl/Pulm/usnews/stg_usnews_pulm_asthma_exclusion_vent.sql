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
/* streamlined codes for vent-dependent and NIPPV patients
 * vent-dependent patient: patients on CHOP TDC registry, with TDC Current Status include 'invasive', 'liberated',
 * 'trach only', or 'transitioned to non-invasive' with at least one pulmonary visit in the current submission year
 * NIPPV patient: patients on CHOP TDC registry, with TDC Current Status include 'non-invasive' with at least one
 * pulmonary visit in the last 3 submission years
 * */
/* validated as of 2/19 */
vent_nippv_pat as (
select
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    sde.element_value,
    sde.entered_date,
    case
        when lower(sde.element_value) in ('invasive', 'trach only', 'liberated', 'transitioned to non-invasive')
        then 'vent-dependent'
        when lower(sde.element_value) in ('non-invasive')
        then 'nippv'
        end as exc_cohort,
    case when sde2.mrn is not null then 1 else 0 end as exc_ind,
    '1' as date_cutoff_ind
from cohort
    inner join {{ref('patient_all')}} as patient_all
       on cohort.pat_key = patient_all.pat_key
    inner join {{source('cdw', 'registry_data_membership')}} as reg
        on cohort.pat_key = reg.pat_key
    inner join {{source('cdw', 'registry_configuration')}} as reg_config
        on reg_config.registry_config_key = reg.registry_config_key
    inner join {{ref('smart_data_element_all')}} as sde
        on cohort.pat_key = sde.pat_key
        and sde.concept_id = 'CHOP#5231' -- TDC Current Status (HP Report)
    left join {{ref('smart_data_element_all')}} as sde2 -- need to look into if this is necessary
        on cohort.pat_key = sde2.pat_key
        and sde2.concept_id = 'CHOP#6705' -- TDC Exclusion Report (HP Report)
where
    reg_config.registry_id = '100126' -- CHOP TDC Registry
    and lower(sde.context_name) = 'patient'
    and lower(sde.element_value) in (
            'non-invasive',
            'invasive',
            'trach only',
            'liberated',
            'transitioned to non-invasive')
    and patient_all.deceased_ind != 1
    and exc_ind != 1
group by
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    sde.element_value,
    sde.entered_date,
    exc_cohort,
    exc_ind,
    date_cutoff_ind
)
select
    mrn,
    patient_name,
    pat_key,
    entered_date as index_date,
    exc_cohort,
    date_cutoff_ind
from vent_nippv_pat
where exc_cohort is not null
