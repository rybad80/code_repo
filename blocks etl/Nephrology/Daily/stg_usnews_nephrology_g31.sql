select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    transplant.pat_key,
    transplant.mrn,
    transplant.patient_name,
    patient.dob,
    extract(year from age(transplant.transplant_date, patient.dob)) as age_at_transplant,
    transplant.transplant_date,
    min(dialysis.encounter_date) as first_maintenance_dialysis_therapy_date,
    case when transplant.transplant_date < first_maintenance_dialysis_therapy_date
           or first_maintenance_dialysis_therapy_date is null
         then 1
         else 0
         end as preemptive_transplant_ind
from {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
    inner join {{ref('transplant_recipients')}} as transplant
        on usnews_metadata_calendar.question_number = 'g31'
    left join {{ref('nephrology_encounter_dialysis')}} as dialysis
        on transplant.pat_key = dialysis.pat_key
    inner join {{source('cdw', 'patient')}} as patient
        on transplant.pat_key = patient.pat_key
where lower(organ) = 'kidney'
    and age_at_transplant < age_lt
    and transplant_date >= start_date
    and transplant_date <= end_date
group by
    submission_year,
    division,
    question_number,
    metric_id,
    transplant.pat_key,
    transplant.mrn,
    transplant.patient_name,
    patient.dob,
    age_at_transplant,
    transplant.transplant_date
