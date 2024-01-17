/*Looking at diagnosis that occurrs anytime prior to a metric_date*/
    select
        usnews_metadata_calendar.submission_year,
        diagnosis_encounter_all.mrn,
        min(diagnosis_encounter_all.encounter_date) as min_diagnosis_date,
        usnews_metadata_calendar.question_number
    from
        {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        inner join {{ ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
            on diagnosis_encounter_all.icd10_code = usnews_metadata_calendar.code
                and diagnosis_encounter_all.encounter_date
                    between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
    group by
        usnews_metadata_calendar.submission_year,
        diagnosis_encounter_all.mrn,
        usnews_metadata_calendar.question_number
