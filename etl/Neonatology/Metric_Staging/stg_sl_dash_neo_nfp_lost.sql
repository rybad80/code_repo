with nfp_pats as (
    select
        nfp_visits.pat_key,
        stg_patient.dob,
        case
            when stg_patient.gestational_age_complete_weeks >= 37
            then (current_date - date(stg_patient.dob)) / 30.5
            else (
                (current_date - date(stg_patient.dob))
                + stg_patient.gestational_age_complete_weeks * 7
                + stg_patient.gestational_age_remainder_days
            ) / 30.5 - 40.0 * 7.0 / 30.5  /* convert 40 weeks to months */
        end as current_corrected_age_in_months,
        nfp_visits_first.visit_location as first_nfp_visit_location,
        min(nfp_visits.corrected_age_in_months_at_encounter) as corrected_age_at_first_nfp_visit

    from
        {{ ref('stg_sl_dash_neo_nfp_visits') }} as nfp_visits
        inner join {{ ref('stg_patient') }} as stg_patient
            on stg_patient.pat_key = nfp_visits.pat_key
        inner join {{ ref('stg_sl_dash_neo_nfp_visits') }} as nfp_visits_first
            on nfp_visits_first.pat_key = nfp_visits.pat_key
                and nfp_visits_first.visit_number = 1

    where
        nfp_visits.appointment_status_id in (
            1, /* scheduled */
            2, /* completed */
            6 /* arrived */
        )

    group by
        nfp_visits.pat_key,
        stg_patient.dob,
        stg_patient.gestational_age_complete_weeks,
        stg_patient.gestational_age_remainder_days,
        nfp_visits_first.visit_location
)

select
    nfp_pats.pat_key,
    nfp_pats.first_nfp_visit_location,
    nfp_pats.dob,
    /* use 30.5 as `days per month` to match logic in nfp redcap */
    nfp_pats.dob + 30.5 * 28 as nfp_lost_two_years_age_out_date,
    nfp_pats.dob + 30.5 * 72 as nfp_lost_six_years_age_out_date,
    nfp_pats.corrected_age_at_first_nfp_visit,
    nfp_pats.current_corrected_age_in_months,
    /* Denominator =
        a. is in the NFP program -- had a visit
        and
        b. has `aged` out of nfp eligibility (either 2 year or 6 year) */
    case
        when nfp_pats.corrected_age_at_first_nfp_visit <= 28.0
            and nfp_pats.current_corrected_age_in_months > 28.0
        then 1
        else 0
    end as nfp_lost_two_years_denom,
    case
        when nfp_pats.corrected_age_at_first_nfp_visit <= 72.0
            and nfp_pats.current_corrected_age_in_months > 72.0
        then 1
        else 0
    end as nfp_lost_six_years_denom,
    min(
        case
            /* patient had a visit in time window (20 to 28 months for 2 year visit) -- NOT lost */
            when nfp_visits.corrected_age_in_months_at_encounter between 20.0 and 28.0 then 0
            /* no nfp visit in window, mark them as `lost` if they meet denominator criteria */
            else nfp_lost_two_years_denom
        end
    ) as nfp_lost_two_years_num,
    min(
        case
            /* patient had a visit in time window (30 to 72 months for final evaluation) -- NOT lost */
            when nfp_visits.corrected_age_in_months_at_encounter between 30.0 and 72.0 then 0
            /* no nfp visit in window, mark them as `lost` if they meet denominator criteria */
            else nfp_lost_six_years_denom
        end
    ) as nfp_lost_six_years_num

from
    nfp_pats
    inner join {{ ref('stg_sl_dash_neo_nfp_visits') }} as nfp_visits
        on nfp_visits.pat_key = nfp_pats.pat_key

where
    nfp_lost_two_years_denom + nfp_lost_six_years_denom > 0

group by
    nfp_pats.pat_key,
    nfp_pats.first_nfp_visit_location,
    nfp_pats.dob,
    nfp_pats.corrected_age_at_first_nfp_visit,
    nfp_pats.current_corrected_age_in_months
