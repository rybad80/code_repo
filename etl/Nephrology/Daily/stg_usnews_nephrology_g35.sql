with cohort as (
    select
        transplant_recipients.pat_key,
        transplant_recipients.most_recent_transplant_date,
        vaccination_all.grouper_records_numeric_id,
        vaccination_all.received_date,
        row_number() over (
            partition by vaccination_all.mrn, vaccination_all.grouper_records_numeric_id order by received_date
        ) as seq_num
    from
        {{ref('transplant_recipients')}} as transplant_recipients
        left join
            {{ref('vaccination_all')}} as vaccination_all on
                transplant_recipients.pat_key = vaccination_all.pat_key
            and vaccination_all.grouper_records_numeric_id in (28, 89)
            and vaccination_all.received_date is not null
    where
        lower(transplant_recipients.organ) = 'kidney'
        and lower(transplant_recipients.recipient_donor) = 'recipient'
        and transplant_recipients.deceased_ind = '0'
        /*ACTIVE FOLLOW UP TRANSPLANT PATIENTS*/
        and (lower(transplant_recipients.curr_stage) = 'transplanted'
        and lower(transplant_recipients.phoenix_episode_status) = 'active follow-up')
)
select distinct
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    cohort.pat_key,
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.dob,
    (date(end_date) - date(stg_patient.dob)) / 365.25 as age_at_year_end,
    cohort.most_recent_transplant_date,
    pneumovax1.received_date as pneumovax_vaccine_date_1,
    pneumovax2.received_date as pneumovax_vaccine_date_2,
    prevnar1.received_date as prevnar_13_vaccine_date_1,
    prevnar2.received_date as prevnar_13_vaccine_date_2,
    prevnar3.received_date as prevnar_13_vaccine_date_3,
    prevnar4.received_date as prevnar_13_vaccine_date_4,
    /* Patient is fully vaccinated if 1 pneumovax or 4 prevnars. Else if over 6y/o, one prevnar is sufficient */
    case when (pneumovax_vaccine_date_1 is not null
        and pneumovax_vaccine_date_1 <= usnews_metadata_calendar.end_date)
            then 1
        when (prevnar_13_vaccine_date_4 is not null
            and prevnar_13_vaccine_date_4 <= usnews_metadata_calendar.end_date)
            then 1
        when (age_at_year_end >= 6 and prevnar_13_vaccine_date_1 is not null
            and prevnar_13_vaccine_date_1 <= usnews_metadata_calendar.end_date)
            then 1
        else 0
        end as vaccinated_ind
from
    {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
    inner join cohort on usnews_metadata_calendar.question_number = 'g35.1'
    inner join {{ref('stg_patient')}} as stg_patient on cohort.pat_key = stg_patient.pat_key
    left join cohort as pneumovax1 on cohort.pat_key = pneumovax1.pat_key
        and pneumovax1.grouper_records_numeric_id = 28 and pneumovax1.seq_num = 1
    left join cohort as pneumovax2 on cohort.pat_key = pneumovax2.pat_key
        and pneumovax2.grouper_records_numeric_id = 28 and pneumovax2.seq_num = 2
    left join cohort as prevnar1 on cohort.pat_key = prevnar1.pat_key
        and prevnar1.grouper_records_numeric_id = 89 and prevnar1.seq_num = 1
    left join cohort as prevnar2 on cohort.pat_key = prevnar2.pat_key
        and prevnar2.grouper_records_numeric_id = 89 and prevnar2.seq_num = 2
    left join cohort as prevnar3 on cohort.pat_key = prevnar3.pat_key
        and prevnar3.grouper_records_numeric_id = 89 and prevnar3.seq_num = 3
    left join cohort as prevnar4 on cohort.pat_key = prevnar4.pat_key
        and prevnar4.grouper_records_numeric_id = 89 and prevnar4.seq_num = 4
where
    age_at_year_end >= age_gte and age_at_year_end < age_lt
