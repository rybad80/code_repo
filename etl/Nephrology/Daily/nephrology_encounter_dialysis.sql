{{ config(meta = {
    'critical': true
}) }}

with unique_visits as (-- all dialysis visits
	select
		pat_key,
		mrn,
		patient_name,
        dob,
        visit_key,
		encounter_date,
        age_years,
        (extract(epoch from date(year(encounter_date) || '-12-31'))
            - extract(epoch from dob)) / (60.0 * 60.0 * 24.00 * 365.25) as age_at_year_end,
        year(encounter_date) as calendar_year,
        /* need most recent visit type to determine HD vs PD */
        row_number() over (partition by pat_key order by encounter_date desc) as seq,
        case when visit_type_id = '7300' then 'HD' else 'PD' end as dialysis_type -- dialysis visit type = HD
	from {{ref('stg_encounter')}}
	where
		(
			(visit_type_id = '3016' and department_id = 101001608) -- PD SUPPLY CHARGING / BGR DIALYSIS
			or (visit_type_id = '7300' and department_id = 101001608)  -- DIALYSIS / BGR DIALYSIS
            /* PD TEACHING / BGR DIALYSIS for pd before May 2019 */
            or (visit_type_id = '7301' and department_id = 101001608 and encounter_date < '2019-05-01')

		)
		and encounter_date >= '2016-01-01'
		and appointment_status_id = 2
),

maintenance_dialysis_and_transplant as (
    /*
    * This code utilizes two different maintenance dialysis start dates
    * to anchor the beginning of maintenance dialysis:
    *    -maintenance_dialysis_start_date (90 days after first dialysis or, for post-transplant
    *    patients, 90 days after first post-transplant dialysis)
    *    -maintenance_dialysis_start_date_updt (90 days after neph_esrd_start_dt field extracted from Epic)
    * Encounters for non-transplant patients, or transplant patients prior to transplant,
    * are considered "maintenance dialysis" if the encounter occurs after either of these
    * two dates. Encounters for transplant patients post-transplant are considered
    * maintenance dialysis only if they occur after maintenance_dialysis_start_date (because
    * neph_esrd_start_dt is not always reset in Epic after transplant).
    *
    * This cte splits the dialysis encounters for transplant patients into time periods
    * before and after transplant in order to automatically reset maintenance_dialysis_start_date
    * post-transplant.
    */
    select
        unique_visits.pat_key,
        min(encounter_date) as min_encounter_date,
        max(encounter_date) as max_encounter_date,
        min_encounter_date + 90 as maintenance_dialysis_start_date,
        /*
        * This "case when" statement overwrites maintenance_dialysis_start_date_updt with
        * maintenance_dialysis_start_date for transplant patients post-transplant so that
        * only maintenance_dialysis_start_date is used to anchor the start of maintenance
        * dialysis for post-transplant encounters.
        */
        case when lag(most_recent_transplant_date)
            over(partition by unique_visits.pat_key order by min_encounter_date) is not null
                then maintenance_dialysis_start_date
                else neph_esrd_start_dt + 90 end as maintenance_dialysis_start_date_updt,
        most_recent_transplant_date,
        case when transplant_recipients.pat_key is null then 0 else 1 end as transplant_received
    from unique_visits
        left join {{ ref('transplant_recipients')}} as transplant_recipients
                on transplant_recipients.pat_key = unique_visits.pat_key
                    and lower(organ) = 'kidney'
                    and encounter_date <= most_recent_transplant_date
        left join {{source('cdw', 'patient')}} as patient on unique_visits.pat_key = patient.pat_key
        left join {{source('clarity_ods', 'patient_4')}} as patient_4 on patient.pat_id = patient_4.pat_id
    group by
        unique_visits.pat_key,
        most_recent_transplant_date,
        transplant_recipients.pat_key,
        neph_esrd_start_dt
),

recorded_weight as (
    /* This CTE finds the most recent weight for each visit in the unique_visit CTE.
    This is necessary because the weight is not taken at the PD visit types in the
    unique_visits CTE */
    select
        stg_encounter.pat_key,
        max(round(meas_val_num * 0.02835, 2)) as visit_weight,
        flowsheet_all.recorded_date,
        unique_visits.visit_key,
        row_number() over (partition by unique_visits.visit_key order by recorded_date desc) as weight_seq
    from unique_visits
        inner join {{ref('stg_encounter')}} as stg_encounter
            on unique_visits.pat_key = stg_encounter.pat_key
        inner join {{ref('flowsheet_all')}} as flowsheet_all
            on flowsheet_all.visit_key = stg_encounter.visit_key
    where
		( /* adding separately from above for weights only */
        (visit_type_id = '8982' and department_id = 101012142) -- PD CLINIC / BGR NEPHROLOGY
		or (visit_type_id = '3016' and department_id = 101001608) -- PD SUPPLY CHARGING / BGR DIALYSIS
		or (visit_type_id = '7300' and department_id = 101001608)  -- DIALYSIS / BGR DIALYSIS
        )
        and flowsheet_all.recorded_date >= '2016-01-01'
		and stg_encounter.appointment_status_id in (2, 6) -- completed, arrived
        and flowsheet_all.flowsheet_id = 40000288 -- dry weight
        and flowsheet_all.recorded_date <= unique_visits.encounter_date
    group by
        stg_encounter.pat_key,
        flowsheet_all.recorded_date,
        unique_visits.visit_key

),

dialysis_type as (
    select
        pat_key,
        dialysis_type as most_recent_dialysis_type
    from
        unique_visits
    where
        seq = 1
)

select
    unique_visits.calendar_year,
    unique_visits.pat_key,
    unique_visits.patient_name,
    unique_visits.mrn,
    unique_visits.dob,
    unique_visits.age_years,
    unique_visits.visit_key,
    unique_visits.encounter_date,
    unique_visits.age_at_year_end,
    stg_patient.death_date,
    recorded_weight.visit_weight as most_recent_weight_recorded,
    unique_visits.dialysis_type,
    unique_visits.seq,
    maintenance_dialysis_start_date,
    maintenance_dialysis_start_date_updt,
    dialysis_type.most_recent_dialysis_type,
    case when unique_visits.encounter_date >= date(calendar_year || '-10-01') then 1
        else 0 end as usnwr_flu_season_ind,
    case when maintenance_dialysis_start_date <= unique_visits.encounter_date
        or maintenance_dialysis_start_date_updt <= unique_visits.encounter_date then 1
        else 0 end as maintenance_dialysis_ind,
    max(most_recent_transplant_date) over (partition by unique_visits.pat_key) as most_recent_transplant_date
from unique_visits
    left join recorded_weight
        on unique_visits.visit_key = recorded_weight.visit_key
            and weight_seq = 1
    inner join {{ ref('stg_patient')}} as stg_patient
        on unique_visits.pat_key = stg_patient.pat_key
    inner join dialysis_type
        on dialysis_type.pat_key = unique_visits.pat_key
    inner join maintenance_dialysis_and_transplant
        on maintenance_dialysis_and_transplant.pat_key = unique_visits.pat_key
            and encounter_date between min_encounter_date and max_encounter_date
