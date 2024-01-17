select
    diabetes_visit_cohort.encounter_key,
    diabetes_visit_cohort.patient_key,
    year(diabetes_visit_cohort.endo_vis_dt) as visit_year,
    diabetes_visit_cohort.endo_vis_dt as endo_visit_date,
    year(current_date) + 1 as diabetes_usnwr_year,
    (current_date - interval('12 month')) as start_date,
    current_date as end_date
from
    {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
where
    (diabetes_visit_cohort.endo_vis_dt < current_date
        and diabetes_visit_cohort.endo_vis_dt >= current_date - interval('12 month'))
    and diabetes_visit_cohort.visit_type in (
        'NEW DIABETES TYPE 1 TRANSFER',
        'NEW DIABETES PATIENT',
        'NEW DIABETES TYPE 2 TRANSFER',
        'FOLLOW UP DIABETES',
        'DIABETES T1Y1 FOLLOW UP',
        'VIDEO VISIT DIABETES'
    )
    and lower(diabetes_visit_cohort.enc_type) = 'office visit'
union all
select
    diabetes_visit_cohort.encounter_key,
    diabetes_visit_cohort.patient_key,
    year(diabetes_visit_cohort.endo_vis_dt) as visit_year,
    diabetes_visit_cohort.endo_vis_dt as endo_visit_date,
    usnews_metadata_calendar.submission_year as diabetes_usnwr_year,
    usnews_metadata_calendar.start_date,
    usnews_metadata_calendar.end_date
from
    {{ ref('usnews_metadata_calendar') }} as usnews_metadata_calendar
    inner join {{ ref('diabetes_visit_cohort') }} as diabetes_visit_cohort
        on (diabetes_visit_cohort.endo_vis_dt between usnews_metadata_calendar.start_date
            and usnews_metadata_calendar.end_date)
            and lower(usnews_metadata_calendar.question_number) like 'c29%'
            and usnews_metadata_calendar.submission_year <= year(current_date)
where
    diabetes_visit_cohort.visit_type in (
        'NEW DIABETES TYPE 1 TRANSFER',
        'NEW DIABETES PATIENT',
        'NEW DIABETES TYPE 2 TRANSFER',
        'FOLLOW UP DIABETES',
        'DIABETES T1Y1 FOLLOW UP',
        'VIDEO VISIT DIABETES'
    )
    and lower(diabetes_visit_cohort.enc_type) = 'office visit'
