with cohort as (
select
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    cardiac_surgery.casenum,
	cardiac_surgery.pat_key,
	cardiac_surgery.visit_key,
	cardiac_surgery.visit_key as primary_key,
	cardiac_surgery.stat_level,
	cardiac_surgery.surgeon,
	cast(cardiac_surgery.hospital_admit_date as date) as doa,
	cast(cardiac_surgery.hospital_discharge_date as date) as dod,
    (
        extract(
            epoch from cardiac_surgery.hospital_discharge_date
            - cardiac_surgery.surg_date
        ) / 86400.0
    ) as postprocedure_los_days,
    cardiac_surgery.mrn,
	cardiac_surgery.patient_name,
	cardiac_surgery.dob,
	cardiac_surgery.sex,
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
    extract(week from cardiac_surgery.surg_date) as week,
	(case
        when
            lower(
                cardiac_surgery.pc4_postop_complication_names
            ) like 'arrhythmia necessitating pacemaker, permanent pacemaker'
            then 1
        else 0
    end) as complication_ind,
    case when complication_ind = 1 then primary_key end as num,
	primary_key as denom

from {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
inner join
    {{ref('cardiac_surgery')}} as cardiac_surgery
	on usnews_metadata_calendar.question_number = 'e37.4'
left join
    {{source('ccis_ods', 'centripetus_cases')}} as centripetus_cases
    on cardiac_surgery.casenum = centripetus_cases.casenumber
left join {{source('ccis_ods', 'centripetus_complications')}} as complications
    on complications.casenumber = cardiac_surgery.casenum
where
	surg_date between usnews_metadata_calendar.start_date and usnews_metadata_calendar.end_date
	and primary_proc_id_32 in ('100', '110', '120', '130', '150')
	and centripetus_cases.reopinadm = 999

group by
    usnews_metadata_calendar.submission_year,
    usnews_metadata_calendar.division,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.metric_id,
    cardiac_surgery.casenum,
	cardiac_surgery.pat_key,
	cardiac_surgery.visit_key,
	primary_key,
	cardiac_surgery.stat_level,
	cardiac_surgery.surgeon,
	doa,
	dod,
	postprocedure_los_days,
	cardiac_surgery.mrn,
	cardiac_surgery.patient_name,
	cardiac_surgery.dob,
	cardiac_surgery.sex,
	cardiac_surgery.index_ind,
    cardiac_surgery.reop,
	cardiac_surgery.surg_year,
	dos,
    cardiac_surgery.surg_weight_kg,
	cardiac_surgery.surg_age_days,
	cardiac_surgery.surg_age_category,
	cardiac_surgery.proc_name,
	cardiac_surgery.proc_short_term_34,
	cardiac_surgery.primary_proc_name,
	cardiac_surgery.primary_proc_id_32,
	cardiac_surgery.op_type,
	cardiac_surgery.open_closed,
	month,
	mortality_30_days,
	mortality_at_discharge,
	week,
	cardiac_surgery.pc4_postop_complication_names
)

select
	cohort.submission_year,
    cohort.division,
    cohort.question_number,
	cohort.metric_id,
	cohort.casenum,
	cohort.pat_key,
	cohort.visit_key,
	cohort.primary_key,
	cohort.stat_level,
	cohort.surgeon,
	cohort.doa,
	cohort.dod,
    cohort.postprocedure_los_days,
    cohort.mrn,
	cohort.patient_name,
	cohort.dob,
	cohort.sex,
	cohort.index_ind,
    cohort.reop,
	cohort.surg_year,
	cohort.dos,
    cohort.surg_weight_kg,
	cohort.surg_age_days,
	cohort.surg_age_category,
	cohort.proc_name,
	cohort.proc_short_term_34,
	cohort.primary_proc_name,
	cohort.primary_proc_id_32,
	cohort.op_type,
	cohort.open_closed,
	cohort.month,
    cohort.mortality_30_days,
    cohort.mortality_at_discharge,
    cohort.week,
	cohort.complication_ind,
    cohort.num,
	cohort.denom,
	centripetus_diagnosis.casenumber as caseid,
	centripetus_diagnosis.diagid,
	centripetus_diagnosis.diagnosisname as diagnosis_name,
	centripetus_diagnosis.diagshrtlst as diagnosis_list,
	diaglist.stsid30
from
	cohort
	inner join {{source('ccis_ods', 'centripetus_diagnosis')}} as centripetus_diagnosis
        on centripetus_diagnosis.casenumber = cohort.casenum
	inner join {{source('ccis_ods', 'centripetus_vdiagnosislist')}} as diaglist
        on diaglist.id = centripetus_diagnosis.diagid
where
	centripetus_diagnosis.sort = 1
	and diaglist.stsid30 in ('71', '73', '75', '77', '79', '80')
