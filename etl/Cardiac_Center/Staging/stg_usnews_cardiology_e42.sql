select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
	cardiac_surgery.pat_key,
	cardiac_surgery.visit_key,
	cardiac_surgery.visit_key as primary_key,
    cardiac_surgery.casenum,
	cardiac_surgery.stat_level,
	cardiac_surgery.surgeon,
	cardiac_surgery.mrn,
	cardiac_surgery.patient_name,
	cardiac_surgery.dob,
	cardiac_surgery.sex,
	cast(cardiac_surgery.hospital_admit_date as date) as doa,
	cast(cardiac_surgery.hospital_discharge_date as date) as dod,
	(
        extract(
            epoch from cardiac_surgery.hospital_discharge_date
             - cardiac_surgery.surg_date
             ) / 86400.0
        ) as postprocedure_los_days,
	cast(postprocedure_los_days as bigint) as num,
	null as denom,
	cardiac_surgery.index_ind,
    cardiac_surgery.reop,
	cardiac_surgery.surg_year,
	cast(cardiac_surgery.surg_date as date) as dos,
    cardiac_surgery.surg_weight_kg,
	cardiac_surgery.surg_age_days,
	cardiac_surgery.surg_age_category,
	cardiac_surgery.proc_name,
	cardiac_surgery.proc_short_term_34,
	cardiac_surgery.primary_proc_name,
	cardiac_surgery.primary_proc_id_32,
	cardiac_surgery.op_type,
	cardiac_surgery.open_closed,
	extract(month from cardiac_surgery.surg_date) as month,
	(case
        when cardiac_surgery.mort_30_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_30_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_30_days,
	(case
        when cardiac_surgery.mort_dc_stat_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_dc_stat_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_at_discharge,
	extract(week from cardiac_surgery.surg_date) as week
from {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
inner join
    {{ref('cardiac_surgery')}} as cardiac_surgery
    on usnews_metadata_calendar.question_number like '%e42a%'
where
	surg_date between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
	and index_ind = 1
	and stat_level = 1
	and lower(op_type) like '%cpb%'
	and dod is not null
group by
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    cardiac_surgery.pat_key,
	cardiac_surgery.visit_key,
	cardiac_surgery.casenum,
    cardiac_surgery.mrn,
    cardiac_surgery.patient_name,
    cardiac_surgery.dob,
	cardiac_surgery.index_ind,
	cardiac_surgery.surg_date,
    dos,
    cardiac_surgery.surgeon,
	doa,
	dod,
	postprocedure_los_days,
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
	mortality_30_days,
	mortality_at_discharge,
	cardiac_surgery.stat_level

union all

select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
	cardiac_surgery.pat_key,
	cardiac_surgery.visit_key,
	cardiac_surgery.visit_key as primary_key,
    cardiac_surgery.casenum,
	cardiac_surgery.stat_level,
	cardiac_surgery.surgeon,
	cardiac_surgery.mrn,
	cardiac_surgery.patient_name,
	cardiac_surgery.dob,
	cardiac_surgery.sex,
	cast(cardiac_surgery.hospital_admit_date as date) as doa,
	cast(cardiac_surgery.hospital_discharge_date as date) as dod,
	(
        extract(
            epoch from cardiac_surgery.hospital_discharge_date
             - cardiac_surgery.surg_date
             ) / 86400.0
        ) as postprocedure_los_days,
	cast(postprocedure_los_days as bigint) as num,
	null as denom,
	cardiac_surgery.index_ind,
    cardiac_surgery.reop,
	cardiac_surgery.surg_year,
	cast(cardiac_surgery.surg_date as date) as dos,
    cardiac_surgery.surg_weight_kg,
	cardiac_surgery.surg_age_days,
	cardiac_surgery.surg_age_category,
	cardiac_surgery.proc_name,
	cardiac_surgery.proc_short_term_34,
	cardiac_surgery.primary_proc_name,
	cardiac_surgery.primary_proc_id_32,
	cardiac_surgery.op_type,
	cardiac_surgery.open_closed,
	extract(month from cardiac_surgery.surg_date) as month,
	(case
        when cardiac_surgery.mort_30_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_30_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_30_days,
	(case
        when cardiac_surgery.mort_dc_stat_ind = 1 then 'DECEASED'
        when cardiac_surgery.mort_dc_stat_ind = 0 then 'ALIVE' else 'UNKNOWN'
    end) as mortality_at_discharge,
	extract(week from cardiac_surgery.surg_date) as week
from {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
inner join
    {{ref('cardiac_surgery')}} as cardiac_surgery
    on usnews_metadata_calendar.question_number like '%e42b%'
where
	surg_date between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
	and index_ind = 1
	and stat_level = 3
	and lower(op_type) like '%cpb%'
	and dod is not null
group by
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    cardiac_surgery.pat_key,
	cardiac_surgery.visit_key,
	cardiac_surgery.casenum,
    cardiac_surgery.mrn,
    cardiac_surgery.patient_name,
    cardiac_surgery.dob,
	cardiac_surgery.index_ind,
	cardiac_surgery.surg_date,
    dos,
    cardiac_surgery.surgeon,
	doa,
	dod,
	postprocedure_los_days,
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
	mortality_30_days,
	mortality_at_discharge,
	cardiac_surgery.stat_level
	