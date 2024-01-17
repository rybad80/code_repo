with chop_oncology_encounters as (
    select distinct
        stg_encounter_outpatient_raw.pat_key,
        patient_all.deceased_ind
    from
        {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
        inner join {{ref('patient_all')}} as patient_all
            on stg_encounter_outpatient_raw.pat_key = patient_all.pat_key
    where
        stg_encounter_outpatient_raw.encounter_date between add_months(current_date, -120) and current_date
        and lower(specialty_name) = 'oncology'
        and stg_encounter_outpatient_raw.specialty_care_ind = 1
),

raw_radiation_course as (
    select
        strright('0000' || chopradiationcourse.chopmrn, 8) as mrn,
        chopradiationcourse.ariacourseserialnumber,
        chopradiationcourse.courseid,
        chopradiationcourse.coursedxdesc,
        chopradiationcourse.coursedxsubcategory,
        chopradiationcourse.firsttreatmentdate,
        chopradiationcourse.lasttreatmentdate,
        chopradiationcourse.treatmentintent,
        chopradiationcourse.iscomplete
    from
        {{source('ods',  'chopradiationcourse')}} as chopradiationcourse

),

radiation_course as (
    select
        raw_radiation_course.ariacourseserialnumber,
        patient_all.patient_name,
        patient_all.mrn,
        patient_all.pat_id,
        patient_all.dob,
        patient_all.current_age,
        patient_all.deceased_ind,
        raw_radiation_course.courseid,
        raw_radiation_course.coursedxdesc,
        raw_radiation_course.coursedxsubcategory,
        raw_radiation_course.firsttreatmentdate,
        raw_radiation_course.lasttreatmentdate,
        raw_radiation_course.treatmentintent,
        raw_radiation_course.iscomplete,
        patient_all.pat_key
    from
        raw_radiation_course
        inner join {{ref('patient_all')}} as patient_all
            on raw_radiation_course.mrn = patient_all.mrn
    order by
        patient_all.patient_name
),

radiation_dose as (
    select
        radiation_course.ariacourseserialnumber,
        chopradiationcoursedose.referencepointid,
        chopradiationcoursedose.delivereddosecgy,
        chopradiationcoursedose.deliveredfractions,
        cast(delivereddosecgy as int4) * cast(deliveredfractions as int4) as dosexfraction,
        radiation_course.pat_key
    from
        radiation_course
    inner join
        {{source('ods',  'chopradiationcoursedose')}} as chopradiationcoursedose
            on radiation_course.ariacourseserialnumber = chopradiationcoursedose.ariacourseserialnumber
),

main as (
    select
        radiation_course.ariacourseserialnumber as aria_course_serial_number,
        radiation_course.patient_name,
        radiation_course.pat_id,
        radiation_course.mrn,
        radiation_course.dob,
        round(radiation_course.current_age, 0) as current_age,
        radiation_course.deceased_ind,
        radiation_course.courseid as course_id,
        radiation_course.coursedxdesc as course_dx_desc,
        radiation_course.coursedxsubcategory as course_dx_subcategory,
        radiation_course.firsttreatmentdate as first_treatment_date,
        radiation_course.lasttreatmentdate as last_treatment_date,
        radiation_course.treatmentintent as treatment_intent,
        case when radiation_course.iscomplete then 1 else 0 end as is_complete_ind,
        radiation_dose.referencepointid as reference_point_id,
        case when lower(referencepointid) like '%tbi%' then 1 else 0 end as tbi_ind,
        radiation_dose.delivereddosecgy as delivered_dose_cgy,
        radiation_dose.deliveredfractions as delivered_fractions,
        radiation_dose.dosexfraction as dose_x_fraction,
--        case when chop_encounters.last_oncology_encounter_date is null then 0 else 1 end as onco_seen_ind,
--        chop_encounters.last_oncology_encounter_date,
        radiation_course.pat_key
    from
        radiation_course
        inner join radiation_dose
            on radiation_course.ariacourseserialnumber = radiation_dose.ariacourseserialnumber
        inner join chop_oncology_encounters
            on radiation_course.pat_key = chop_oncology_encounters.pat_key
),

elect_radiation_course as (
    select
        main.*,
        thyroid_surveillance_courseids.elect_high_risk_radiation_ind
    from
        main as main
    inner join {{ref('lookup_thyroid_surveillance_courseids')}} as thyroid_surveillance_courseids
    on main.course_id = thyroid_surveillance_courseids.courseid
    and thyroid_surveillance_courseids.elect_high_risk_radiation_ind = 1
    and main.deceased_ind = 0
),

all_course_first_last_rad_dates as (
    select
        pat_key,
        min(firsttreatmentdate) as all_course_first_treatment_date,
        max(lasttreatmentdate) as all_course_last_treatment_date
    from
        radiation_course
    group by
        pat_key

),

thyroid_first_last_rad_dates as (
    select
        pat_key,
        min(dob) as date_of_birth,
        min(first_treatment_date) as radiation_first_treatment_date,
        max(last_treatment_date) as radiation_last_treatment_date,
        (extract(year from min(first_treatment_date)) - extract(year from min(dob)))
          - case
                when add_months(min(dob), (extract(year from min(first_treatment_date))
                     - extract(year from min(dob))) * 12)
                    > min(first_treatment_date) then 1
                else 0
            end as first_radiation_age -- more accurate way of calculating age

    from
        elect_radiation_course
    group by
        pat_key

),

radiation as (
    select h.* from (
        select distinct
            elect_radiation_course.aria_course_serial_number,
            elect_radiation_course.course_id,
            elect_radiation_course.course_dx_desc,
            elect_radiation_course.course_dx_subcategory,
            elect_radiation_course.is_complete_ind,
            elect_radiation_course.reference_point_id,
            elect_radiation_course.tbi_ind,
            elect_radiation_course.delivered_dose_cgy,
            elect_radiation_course.delivered_fractions,
            elect_radiation_course.deceased_ind,
            1 as radiation_ind,
            elect_radiation_course.elect_high_risk_radiation_ind,
            elect_radiation_course.pat_key,
            elect_radiation_course.patient_name,
            elect_radiation_course.pat_id,
            elect_radiation_course.mrn,
            row_number() over(partition by elect_radiation_course.pat_key
                                order by elect_radiation_course.last_treatment_date desc) as radiation_num
        from
            elect_radiation_course
        ) as h
    where
        h.radiation_num = 1
),

final_table as (
    select
        radiation.pat_key,
        radiation.patient_name,
        radiation.mrn,
        radiation.pat_id,
        radiation.aria_course_serial_number,
--        radiation.age_at_first_radiation,
        radiation.course_id,
        radiation.course_dx_desc,
        radiation.course_dx_subcategory,
--        radiation.first_radiation_date,
--        radiation.last_radiation_date,
        radiation.is_complete_ind,
        radiation.reference_point_id,
        radiation.tbi_ind,
        radiation.delivered_dose_cgy,
        radiation.delivered_fractions,
        radiation.radiation_ind,
        radiation.elect_high_risk_radiation_ind,
        radiation.deceased_ind,
        all_course_first_last_rad_dates.all_course_first_treatment_date,
        all_course_first_last_rad_dates.all_course_last_treatment_date,
        thyroid_first_last_rad_dates.radiation_first_treatment_date,
        thyroid_first_last_rad_dates.radiation_last_treatment_date,
        thyroid_first_last_rad_dates.first_radiation_age
    from
        radiation
        left join all_course_first_last_rad_dates
            on radiation.pat_key = all_course_first_last_rad_dates.pat_key
        left join thyroid_first_last_rad_dates
            on radiation.pat_key = thyroid_first_last_rad_dates.pat_key
)

select
    patient_name,
    mrn,
    pat_id,
    all_course_first_treatment_date,
    all_course_last_treatment_date,
    radiation_first_treatment_date,
    radiation_last_treatment_date,
    first_radiation_age,
    case
        when is_complete_ind = 0 then 'Y'
        when is_complete_ind = 1 then 'N'
        end as currently_receiving_treatment,
    current_date as date_record_added
from
    final_table
where
    first_radiation_age < 19
