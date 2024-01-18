with last_well as (
select
    patient_key,
    encounter_key as last_well_encounter_key,
    encounter_date as last_well_date,
    age_years as last_well_age_years,
    age_months as last_well_age_months,
    row_number() over(
        partition by patient_key
        order by encounter_date desc,
            appointment_date desc
    ) as row_num
from {{ref('stg_encounter_outpatient')}}
where primary_care_ind = 1
    and encounter_date < current_date
    and (well_visit_ind = 1
        -- recent arrived well visits not yet closed/billed
        or (appointment_status_id = 6
            and encounter_date >= (current_date - 5)
            and visit_type_id in (
                '1307',
                '4014',
                '4019',
                '2923',
                '2922',
                '2921',
                '2910',
                '2913',
                '1358',
                '1335',
                '2931',
                '2908',
                '2909',
                '1363',
                '1312',
                '1372',
                '1373',
                '4022',
                '4016',
                '2925',
                '2932',
                '2933',
                '2902',
                '2133',
                '2132',
                '2134',
                '2368',
                '2940',
                '9902',
                '4024',
                '1413',
                '1379',
                '2158',
                '4008',
                '2903',
                '9913',
                '2730',
                '3431',
                '3039',
                '852',
                '2731',
                '1378')
            ))
),
wells_by_fifteen as (
select
    patient_key,
    count(encounter_key) as num_wells_by_fifteen
from {{ref('stg_encounter_outpatient')}}
where primary_care_ind = 1
    and well_visit_ind = 1
    and age_months < 15
    and encounter_date < current_date
group by patient_key
),
wells_by_thirty as (
select
    patient_key,
    count(encounter_key) as num_wells_fifteen_thirty
from {{ref('stg_encounter_outpatient')}}
where primary_care_ind = 1
    and well_visit_ind = 1
    and age_months >= 15
    and age_months < 30
    and encounter_date < current_date
group by patient_key
),
next_well as (
select
    patient_key,
    encounter_key as next_well_encounter_key,
    encounter_date as next_well_date,
    age_years as next_well_age_years,
    row_number() over(
        partition by patient_key
        order by appointment_date asc
    ) as row_num
from {{ref('stg_encounter_outpatient')}}
where primary_care_ind = 1
    and appointment_status_id = 1 -- scheduled
    --lower(visit_type) like '%well%'
    and visit_type_id in (
                '1307',
                '4014',
                '4019',
                '2923',
                '2922',
                '2921',
                '2910',
                '2913',
                '1358',
                '1335',
                '2931',
                '2908',
                '2909',
                '1363',
                '1312',
                '1372',
                '1373',
                '4022',
                '4016',
                '2925',
                '2932',
                '2933',
                '2902',
                '2133',
                '2132',
                '2134',
                '2368',
                '2940',
                '9902',
                '4024',
                '1413',
                '1379',
                '2158',
                '4008',
                '2903',
                '9913',
                '2730',
                '3431',
                '3039',
                '852',
                '2731',
                '1378')
    and encounter_date >= current_date
)
select
    stg_primary_care_patients.patient_key,
    last_well.last_well_encounter_key,
    last_well.last_well_date,
    last_well.last_well_age_years,
    last_well.last_well_age_months,
    wells_by_fifteen.num_wells_by_fifteen,
    wells_by_thirty.num_wells_fifteen_thirty,
    next_well.next_well_encounter_key,
    next_well.next_well_date,
    next_well.next_well_age_years
from {{ref('stg_primary_care_patients')}} as stg_primary_care_patients
    left join last_well
        on last_well.patient_key = stg_primary_care_patients.patient_key
            and last_well.row_num = 1
    left join wells_by_fifteen
        on wells_by_fifteen.patient_key = stg_primary_care_patients.patient_key
    left join wells_by_thirty
        on wells_by_thirty.patient_key = stg_primary_care_patients.patient_key
    left join next_well
        on next_well.patient_key = stg_primary_care_patients.patient_key
            and next_well.row_num = 1
