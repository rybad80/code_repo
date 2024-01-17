with usnews_metadata_calendar
as (
select
    usnews_metadata_calendar.*
from
    {{ ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
where
    usnews_metadata_calendar.question_number like 'g22%'
    and lower(usnews_metadata_calendar.code_type) = 'cpt_code'
),
neph_enct_dialysis_details
as (
select distinct
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date,
    usnews_metadata_calendar.division,
    nephrology_encounter_dialysis.pat_key,
    nephrology_encounter_dialysis.patient_name,
    nephrology_encounter_dialysis.mrn,
    nephrology_encounter_dialysis.dob,
    nephrology_encounter_dialysis.maintenance_dialysis_start_date,
    nephrology_encounter_dialysis.encounter_date,
    usnews_metadata_calendar.code,
    nephrology_encounter_dialysis.maintenance_dialysis_ind
from
    {{ ref('nephrology_encounter_dialysis')}} as nephrology_encounter_dialysis
inner join usnews_metadata_calendar
        on
    nephrology_encounter_dialysis.encounter_date between usnews_metadata_calendar.start_date
            and usnews_metadata_calendar.end_date
)
select
    neph_enct_dialysis_details.submission_year,
    neph_enct_dialysis_details.start_date,
    neph_enct_dialysis_details.end_date,
    neph_enct_dialysis_details.division,
    neph_enct_dialysis_details.pat_key,
    neph_enct_dialysis_details.patient_name,
    neph_enct_dialysis_details.mrn,
    neph_enct_dialysis_details.dob,
    procedure_billing.age_years,
    procedure_billing.service_date,
    procedure_billing.cpt_code,
    neph_enct_dialysis_details.maintenance_dialysis_start_date,
    case
        when service_date between neph_enct_dialysis_details.start_date
        and neph_enct_dialysis_details.end_date then 1
        else 0
    end as during_year_ind,
    case
        when neph_enct_dialysis_details.maintenance_dialysis_start_date <= procedure_billing.service_date
        and during_year_ind = 1 then 0
        when during_year_ind = 0 then 0
        else 1
    end as excluded_ind,
    -- exclude accesses performed in same year prior to 90 day after min dialysis date
    case
        when procedure_billing.cpt_code in (
            '36557',
            '36558'
        --'36581' -- remove from list - these are rewires. will need to check with usnwr if they should count.
        ) then 'hd_central_caths'
        when procedure_billing.cpt_code in (
            '36825',
            '36830',
            '35251',
            '35351',
            '35840',
            '36818',
            '36819',
            '36821',
            '36833',
            '37607'
        --'37799' -- remove from list per ben
        ) then 'hd_fistula_graft'
        when procedure_billing.cpt_code in (
            '49324',
            '49421'
        ) then 'pd_cath'
    end as question_group,
    procedure_billing.procedure_name
from
    {{ ref('procedure_billing')}} as procedure_billing
inner join neph_enct_dialysis_details
on
    neph_enct_dialysis_details.pat_key = procedure_billing.pat_key
    and neph_enct_dialysis_details.code = procedure_billing.cpt_code
where
    service_date <= neph_enct_dialysis_details.end_date
    and neph_enct_dialysis_details.maintenance_dialysis_ind = 1
