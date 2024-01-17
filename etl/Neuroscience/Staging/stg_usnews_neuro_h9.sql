{{ config(meta = {
    'critical': false
}) }}

with first_neuro_bill as (
    select
        procedure_billing.mrn,
        min(procedure_billing.service_date) as first_service_date --"new" neuro date
    from
        {{ref('procedure_billing')}} as procedure_billing
    left join
        {{source('cdw', 'department')}} as department
        on procedure_billing.department_id = department.dept_id
    where
        (lower(department.specialty) in ('neurosurgery', 'neurology')
        or lower(procedure_billing.provider_specialty) in ('neu', 'nrs'))
        and (lower(department.specialty) != 'genetics')
    group by procedure_billing.mrn
),

first_bill_submission as (
    select
        usnews_metadata_calendar.submission_year,
        first_neuro_bill.mrn,
        first_neuro_bill.first_service_date
    from
        first_neuro_bill
    inner join
        {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on first_service_date between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
        and lower(usnews_metadata_calendar.metric_id) = 'h9a'
)

select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    usnews_billing.pat_key as primary_key,
    lookup_usnews_metadata.division,
    lookup_usnews_metadata.question_number,
    lookup_usnews_metadata.metric_name,
    usnews_billing.submission_year,
    usnews_billing.mrn,
    usnews_billing.patient_name,
    usnews_billing.dob,
    usnews_billing.service_date as metric_date,
    usnews_billing.metric_id,
    usnews_billing.mrn as num,
    usnews_billing.age_years,
    usnews_billing.service_date as index_date,
    usnews_billing.cpt_code,
    usnews_billing.procedure_name,
    usnews_billing.department_specialty,
    usnews_billing.provider_specialty,
    usnews_billing.provider_name
from
    {{ ref('usnews_billing') }} as usnews_billing
    inner join {{ ref('lookup_usnews_metadata') }} as lookup_usnews_metadata
        on usnews_billing.metric_id = lookup_usnews_metadata.metric_id
    left join first_bill_submission
        on usnews_billing.mrn = first_bill_submission.mrn
        and usnews_billing.submission_year = first_bill_submission.submission_year
where
    ((usnews_billing.metric_id = 'h9a'
        and usnews_billing.cpt_code not in ('99291', '99292')) --removing critical care codes
    or (usnews_billing.metric_id = 'h9a'
        and usnews_billing.cpt_code in ('99291', '99292') --critical care codes only
        and first_bill_submission.mrn is not null)) -- and first bill with neuro
    and (lower(usnews_billing.department_specialty) in ('neurosurgery', 'neurology')
        or lower(usnews_billing.provider_specialty) in ('neu', 'nrs'))
    and (lower(usnews_billing.department_specialty) != 'genetics')
