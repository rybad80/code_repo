with proc_stage as (
    select
        usnews_metadata_calendar.submission_year,
        usnews_metadata_calendar.start_date,
        usnews_metadata_calendar.end_date,
        usnews_metadata_calendar.division,
        usnews_metadata_calendar.question_number,
        usnews_metadata_calendar.metric_name,
        usnews_metadata_calendar.metric_id,
        usnews_metadata_calendar.code_type,
        usnews_metadata_calendar.code,
        surgery_procedure.or_proc_key,
        surgery_procedure.cpt_code,
        surgery_procedure.or_procedure_name
    from
        {{ ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        left join {{ ref('surgery_procedure')}} as surgery_procedure
            on usnews_metadata_calendar.code = surgery_procedure.cpt_code
                and usnews_metadata_calendar.code_type in ('CPT_CODE')
    where
        usnews_metadata_calendar.division = 'Gastroenterology'
        and	usnews_metadata_calendar.code_type in ('CPT_CODE')
        and surgery_procedure.surgery_date between
            usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
)

select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    surgery_procedure.pat_key as primary_key,
    surgery_procedure.surgery_date as metric_date,
    surgery_procedure.mrn as num,
    proc_stage.metric_name,
    proc_stage.metric_id,
    /*used for validation*/
    proc_stage.submission_year,
    surgery_procedure.patient_name,
    surgery_procedure.mrn,
    surgery_procedure.dob,
    surgery_procedure.surgery_date as index_date,
    proc_stage.question_number,
    proc_stage.division,
    surgery_procedure.cpt_code,
    proc_stage.or_procedure_name as procedure_name,
    surgery_procedure.visit_key
from
    proc_stage
    inner join {{ ref('surgery_procedure') }} as surgery_procedure
        on surgery_procedure.or_proc_key = proc_stage.or_proc_key
            and surgery_procedure.surgery_date between
                proc_stage.start_date and proc_stage.end_date
    left join {{ ref('stg_usnews_pat_dx_level')}} as stg_usnews_pat_dx_level
        on surgery_procedure.mrn = stg_usnews_pat_dx_level.mrn
            and proc_stage.question_number = stg_usnews_pat_dx_level.question_number
            and proc_stage.submission_year = stg_usnews_pat_dx_level.submission_year
where
    (proc_stage.metric_id in (
        'd17a',
        'd17c',
        'd17d',
        'd17e',
        'd17f',
        'd17g')
            and stg_usnews_pat_dx_level.min_diagnosis_date <= surgery_procedure.surgery_date
    )
    or (proc_stage.metric_id = 'd17b')
