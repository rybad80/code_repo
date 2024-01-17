select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    cardiac_surgery.pat_key,
    cardiac_surgery.visit_key,
    cardiac_surgery.visit_key as primary_key,
    primary_key as num,
    null as denom,
    cardiac_surgery.mrn,
    cardiac_surgery.patient_name,
    cardiac_surgery.dob,
    cast(cardiac_surgery.surg_date as date) as dos,
    cardiac_surgery.surgeon,
    cast(cardiac_surgery.hospital_admit_date as date) as doa,
    cast(cardiac_surgery.hospital_discharge_date as date) as dod,
    cardiac_surgery.sex,
    cardiac_surgery.reop,
    cardiac_surgery.surg_year,
    cardiac_surgery.surg_weight_kg,
    cardiac_surgery.surg_age_days,
    cardiac_surgery.surg_age_category,
    cardiac_surgery.proc_name,
    cardiac_surgery.proc_short_term_34,
    cardiac_surgery.primary_proc_name,
    cardiac_surgery.primary_proc_id_32,
    cardiac_surgery.op_type,
    cardiac_surgery.open_closed,
    cardiac_surgery.stat_level,
    extract(year from age(cardiac_surgery.surg_date, cardiac_surgery.dob))
        as age_at_transplant,
    (
        extract(
            epoch from cardiac_surgery.hospital_discharge_date
            - cardiac_surgery.hospital_admit_date
        ) / 86400
    ) as hospitalization_los_days,
    (case
        when cardiac_surgery.mort_case_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_case_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_all_intervals,
    (case
        when cardiac_surgery.mort_30_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_30_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_30_days,
    (case
        when cardiac_surgery.mort_dc_stat_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_dc_stat_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_at_discharge

from {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
inner join {{ref('cardiac_surgery')}} as cardiac_surgery
    on usnews_metadata_calendar.question_number = 'e22'

where surg_date between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
    and primary_proc_id_32 = '890'
    and age_at_transplant < 18
union all
select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    cardiac_surgery.pat_key,
    cardiac_surgery.visit_key,
    cardiac_surgery.visit_key as primary_key,
    primary_key as num,
    null as denom,
    cardiac_surgery.mrn,
    cardiac_surgery.patient_name,
    cardiac_surgery.dob,
    cast(cardiac_surgery.surg_date as date) as dos,
    cardiac_surgery.surgeon,
    cast(cardiac_surgery.hospital_admit_date as date) as doa,
    cast(cardiac_surgery.hospital_discharge_date as date) as dod,
    cardiac_surgery.sex,
    cardiac_surgery.reop,
    cardiac_surgery.surg_year,
    cardiac_surgery.surg_weight_kg,
    cardiac_surgery.surg_age_days,
    cardiac_surgery.surg_age_category,
    cardiac_surgery.proc_name,
    cardiac_surgery.proc_short_term_34,
    cardiac_surgery.primary_proc_name,
    cardiac_surgery.primary_proc_id_32,
    cardiac_surgery.op_type,
    cardiac_surgery.open_closed,
    cardiac_surgery.stat_level,
    extract(year from age(cardiac_surgery.surg_date, cardiac_surgery.dob))
        as age_at_transplant,
    (
        extract(
            epoch from cardiac_surgery.hospital_discharge_date
            - cardiac_surgery.hospital_admit_date
        ) / 86400
    ) as hospitalization_los_days,
    (case
        when cardiac_surgery.mort_case_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_case_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_all_intervals,
    (case
        when cardiac_surgery.mort_30_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_30_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_30_days,
    (case
        when cardiac_surgery.mort_dc_stat_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_dc_stat_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_at_discharge

from {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
inner join {{ref('cardiac_surgery')}} as cardiac_surgery
    on usnews_metadata_calendar.question_number = 'e22.1'

where surg_date between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
    and primary_proc_id_32 = '890'
    and age_at_transplant < 1
