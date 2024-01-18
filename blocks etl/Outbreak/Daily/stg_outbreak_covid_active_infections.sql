{{ config(meta = {
    'critical': true
}) }}

/*All stretches of active COVID infections by patient. Used to determine
if a patient counts as actively COVID pos at a given timestamp.
Active stretch starts with:
(-Any positive test OR
-First recorded COVID dx for patient OR
-COVID-19 infection status in bugsy) AND
-FOR INPATIENTS ONLY: start of isolation precautions
Active stretch ends with earliest of:
-History of COVID added to problem list
-30 days from start
-FOR INPATIENTS ONLY: removal from isolation precautions*/
with tests as (
    select
        pat_key,
        result_date as start_date,
        cast(null as timestamp) as end_date,
        'positive covid test' as source_desc
    from
        {{ref('outbreak_covid_cohort_patient')}}
    where
        current_status = 3
        and false_positive_manual_review_ind = 0
),

diagnoses as (
    select
        pat_key,
        covid_noted_date as start_date,
        cast(null as timestamp) as end_date,
        'diagnosis' as source_desc
    from
        {{ref('stg_outbreak_covid_patient_encounter_dx_status')}}
    where
        covid_noted_date <= coalesce(covid_resolved_noted_date, current_date)
),

bugsy_infections as (
    select
        stg_encounter.pat_key,
        to_timestamp(
            timezone(
                infections.add_utc_dttm,
                'UTC',
                'America/New_York'),
            'YYYY-MM-DD HH:MI:SS')
        as start_date,
        to_timestamp(
            timezone(
                infections.resolve_utc_dttm,
                'UTC',
                'America/New_York'),
            'YYYY-MM-DD HH:MI:SS')
        as end_date,
        'bugsy' as source_desc
    from
        {{source('clarity_ods', 'infections')}} as infections
        inner join {{ref('stg_encounter')}} as stg_encounter
            on stg_encounter.csn = infections.pat_enc_csn_id
    where
        infections.infection_type_c = 81 --COVID-19 (SARS-CoV-2)
),

all_infection_start as (
    select
    *
    from
        tests
    union all
    select
        *
    from
        diagnoses
    union all
    select
        *
    from
        bugsy_infections
),

all_infections_temp as (
    select
        all_infection_start.pat_key,
        all_infection_start.start_date,
        min(
            coalesce(
                all_infection_start.end_date,
                all_infection_start.start_date + interval '30 days'
            ),
            coalesce(
                stg_outbreak_covid_patient_encounter_dx_status.covid_resolved_noted_date,
                current_date + 1
            ),
            current_date + 1
        ) as end_date_temp,
        case
            when end_date_temp > current_date then null
            else end_date_temp
        end as end_date,
        all_infection_start.source_desc,
        row_number() over (
            partition by all_infection_start.pat_key
            order by all_infection_start.start_date)
        as seq
    from
        all_infection_start
        left join {{ref('stg_outbreak_covid_patient_encounter_dx_status')}}
            as stg_outbreak_covid_patient_encounter_dx_status
            on stg_outbreak_covid_patient_encounter_dx_status.pat_key
            = all_infection_start.pat_key
),

all_infections as (
    select
        *
    from
        all_infections_temp
    where
        coalesce(end_date, current_date) > start_date
),

isolation_precautions_temp as (
    select
        encounter_inpatient.visit_key,
        encounter_inpatient.pat_key,
        /*earliest precaution start occuring after admit*/
        min(case
            when encounter_inpatient.hospital_admit_date > hospital_isolation.isolation_added_dt
            then encounter_inpatient.hospital_admit_date
            else hospital_isolation.isolation_added_dt end)
        as precautions_start,
        /*latest precaution end occuring before disch*/
        max(case
            when encounter_inpatient.hospital_discharge_date < hospital_isolation.isolation_removed_dt
            then encounter_inpatient.hospital_discharge_date
            else hospital_isolation.isolation_removed_dt end)
        as precautions_end_temp,
        /*track if precaution is still active so we can overwrite temp end date
        with null in the next step*/
        max(case when hospital_isolation.isolation_removed_dt is null
            and encounter_inpatient.hospital_discharge_date is null
            then 1 else 0 end)
        as ongoing_precaution_ind
    from
        {{ref('encounter_inpatient')}} as encounter_inpatient
        inner join {{source('cdw', 'hospital_isolation')}}
            as hospital_isolation
            on hospital_isolation.visit_key = encounter_inpatient.visit_key
        inner join {{source('cdw', 'dim_patient_isolation')}}
            as dim_patient_isolation
            on dim_patient_isolation.dim_patient_isolation_key
            = hospital_isolation.dim_patient_isolation_key
    where
        dim_patient_isolation.patient_isolation_id in (
            8,	--EXPANDED PRECAUTIONS
            13) --MODIFIED EXPANDED PRECAUTIONS
    group by
        encounter_inpatient.visit_key,
        encounter_inpatient.pat_key
),

isolation_precautions as (
    select
        *,
        case
            when ongoing_precaution_ind = 0 then precautions_end_temp
        end as precautions_end
    from
        isolation_precautions_temp
),

/*Calculate start and end dates for inpatients using isolation precautions*/
ip_infections as (
    select
        all_infections.pat_key,
        /*visit_key level because an infection */
        isolation_precautions.visit_key,
        all_infections.seq,
        all_infections.source_desc,
        /*max of start dates*/
        max(case
            when all_infections.start_date >= isolation_precautions.precautions_start
                then all_infections.start_date
            else isolation_precautions.precautions_start end)
        as start_date_ip,
        /*min of end dates*/
        min(case
            when coalesce(all_infections.end_date, current_date)
            <= coalesce(isolation_precautions.precautions_end, current_date)
                then all_infections.end_date
            else isolation_precautions.precautions_end end)
        as end_date_ip
    from
        all_infections
        inner join isolation_precautions
            on isolation_precautions.pat_key = all_infections.pat_key
    where
        /*iso precautions overlap with active infection*/
        isolation_precautions.precautions_start
            between all_infections.start_date
            and coalesce(all_infections.end_date, current_date)
        or all_infections.start_date
            between isolation_precautions.precautions_start
            and coalesce(isolation_precautions.precautions_end, current_date)
    group by
        all_infections.pat_key,
        all_infections.seq,
        all_infections.source_desc,
        isolation_precautions.visit_key
)

select
    all_infections.pat_key,
    /*visit_key = 0 for infection periods not overlapping an IP encounter*/
    coalesce(ip_infections.visit_key, 0) as visit_key,
    all_infections.start_date,
    all_infections.end_date,
    ip_infections.start_date_ip,
    ip_infections.end_date_ip,
    all_infections.source_desc
from
    all_infections
    left join ip_infections
        on ip_infections.pat_key = all_infections.pat_key
        and ip_infections.seq = all_infections.seq
        and ip_infections.source_desc = all_infections.source_desc
