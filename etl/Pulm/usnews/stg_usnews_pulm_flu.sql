with stage as ( -- union all flu denom cohorts
select
    denom_cohort.submission_year,
    denom_cohort.start_date,
    denom_cohort.end_date,
    denom_cohort.flu_start_date,
    denom_cohort.flu_end_date,
    denom_cohort.division,
    denom_cohort.question_number,
    denom_cohort.metric_id,
    denom_cohort.metric_name,
    denom_cohort.mrn,
    denom_cohort.patient_name,
    denom_cohort.dob,
    denom_cohort.pat_key,
    max(denom_cohort.encounter_date) as most_recent_encounter_date
from
    {{ref('stg_usnews_pulm_j27_1')}} as denom_cohort
group by
    denom_cohort.submission_year,
    denom_cohort.start_date,
    denom_cohort.end_date,
    denom_cohort.flu_start_date,
    denom_cohort.flu_end_date,
    denom_cohort.division,
    denom_cohort.question_number,
    denom_cohort.metric_id,
    denom_cohort.metric_name,
    denom_cohort.mrn,
    denom_cohort.patient_name,
    denom_cohort.dob,
    denom_cohort.pat_key

union all

select
    denom_cohort.submission_year,
    denom_cohort.start_date,
    denom_cohort.end_date,
    denom_cohort.flu_start_date,
    denom_cohort.flu_end_date,
    denom_cohort.division,
    denom_cohort.question_number,
    denom_cohort.metric_id,
    denom_cohort.metric_name,
    denom_cohort.mrn,
    denom_cohort.patient_name,
    denom_cohort.dob,
    denom_cohort.pat_key,
    max(denom_cohort.encounter_date) as most_recent_encounter_date
from
    {{ref('stg_usnews_pulm_j15')}} as denom_cohort
group by
    denom_cohort.submission_year,
    denom_cohort.start_date,
    denom_cohort.end_date,
    denom_cohort.flu_start_date,
    denom_cohort.flu_end_date,
    denom_cohort.division,
    denom_cohort.question_number,
    denom_cohort.metric_id,
    denom_cohort.metric_name,
    denom_cohort.mrn,
    denom_cohort.patient_name,
    denom_cohort.dob,
    denom_cohort.pat_key
)
-- final join to vaccination_all
select
    stage.submission_year,
    stage.start_date,
    stage.end_date,
    stage.flu_start_date,
    stage.flu_end_date,
    stage.division,
    stage.question_number,
    stage.metric_id,
    stage.metric_name,
    stage.mrn,
    stage.patient_name,
    stage.dob,
    max(vaccination_all.received_date) as received_date,
    max(vaccination_all.documented_date) as documented_date,
    stage.most_recent_encounter_date,
    stage.pat_key
from stage
    left join {{ref('vaccination_all')}} as vaccination_all
        on stage.pat_key = vaccination_all.pat_key
        and vaccination_all.influenza_vaccine_ind = 1
        and coalesce(vaccination_all.received_date, vaccination_all.documented_date)
            between stage.flu_start_date and stage.flu_end_date
group by
    stage.submission_year,
    stage.start_date,
    stage.end_date,
    stage.flu_start_date,
    stage.flu_end_date,
    stage.division,
    stage.question_number,
    stage.metric_id,
    stage.metric_name,
    stage.mrn,
    stage.patient_name,
    stage.dob,
    stage.most_recent_encounter_date,
    stage.pat_key
