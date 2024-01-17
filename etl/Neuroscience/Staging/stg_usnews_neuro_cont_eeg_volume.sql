{{ config(meta = {
    'critical': false
}) }}

select distinct
    'operational' as domain, --noqa: L029
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.submission_year,
    stg_neuro_cont_eeg.pat_key as primary_key,
    stg_neuro_cont_eeg.mrn,
    stg_neuro_cont_eeg.patient_name,
    stg_neuro_cont_eeg.dob,
    stg_neuro_cont_eeg.service_date as metric_date,
    stg_neuro_cont_eeg.usnews_metric_id as metric_id,
    stg_neuro_cont_eeg.mrn as num,
    stg_neuro_cont_eeg.age_years,
    stg_neuro_cont_eeg.service_date as index_date,
    stg_neuro_cont_eeg.cpt_code,
    stg_neuro_cont_eeg.procedure_name,
    stg_neuro_cont_eeg.department_specialty,
    stg_neuro_cont_eeg.provider_specialty,
    stg_neuro_cont_eeg.provider_name
from
	{{ref('stg_neuro_cont_eeg')}} as stg_neuro_cont_eeg
inner join
	{{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
	on stg_neuro_cont_eeg.usnews_metric_id = usnews_metadata_calendar.metric_id
	and stg_neuro_cont_eeg.service_date between usnews_metadata_calendar.start_date
        and usnews_metadata_calendar.end_date
	and stg_neuro_cont_eeg.age_years between usnews_metadata_calendar.age_gte
        and usnews_metadata_calendar.age_lt
