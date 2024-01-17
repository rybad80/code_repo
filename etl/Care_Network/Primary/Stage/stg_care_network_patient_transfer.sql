/*
encounters where a patient's transfer out of the cn is documented -
based on standard code in cdw > care network. In patient-level metrics,
i want to exclude patients who have transferred out from the denominator.
to do this, need to determine "inactive" date intervals.
*/
with patient_transfer as ( --noqa: PRS
    select
        stg_encounter.patient_key,
        stg_encounter.pat_key,
        stg_encounter.encounter_date,
        99 as completed_ind

    from
        {{ ref('stg_encounter') }} as stg_encounter
    inner join {{ source('clarity_ods', 'pat_enc_rsn_visit') }} as pat_enc_rsn_visit
        on stg_encounter.csn = pat_enc_rsn_visit.pat_enc_csn_id

    where
        encounter_type_id = 70 --encounter type: telephone
        --reason name: transfer
        and pat_enc_rsn_visit.enc_reason_id = 268
        and lower(stg_encounter.intended_use_name) = 'primary care'

    group by
        stg_encounter.patient_key,
        stg_encounter.pat_key,
        stg_encounter.encounter_date
),

/*
concatenating transfer events and completed, billed office visits
so i can identify if a patient returned to the cn after transferring out.
if they returned, i want to be able to document their inactive period ended
as of the date of the return visit.
*/
combo as (
    select
        stg_care_network_patient_month_year.pat_key,
        stg_care_network_patient_month_year.patient_key,
        stg_care_network_patient_month_year.encounter_date,
        stg_care_network_patient_month_year.completed_ind

    from
        {{ ref('stg_care_network_patient_month_year')}} as stg_care_network_patient_month_year

    union all

    select
        pat_key,
        patient_key,
        encounter_date,
        completed_ind

    from
        patient_transfer
),

/*
found that sometimes patients have multiple transfer events that occur
consecutively  - that is, they have multiple transfer phone calls with no
return visit to the cn in between. We only want the first transfer event to
get the start date for the period of inactivity, so i'm flagging the consecutive
events to filter out in the next step.
*/
consecutive_flag as (
    select
        pat_key,
        patient_key,
        encounter_date,
        completed_ind,
        case
            when completed_ind = 99
                and (
                    lag(
                        patient_key
                    ) over (order by patient_key, encounter_date) = patient_key
                )
                and (
                    lag(
                        completed_ind
                    ) over (order by patient_key, encounter_date) = 99
                ) then 1
            else 0
        end as consecutive_flag

    from
        combo
),

/*
filtering out consecutive transfers and computing start and end dates
for inactive intervals. an inactive interval starts on the date the transfer
was documented and ends when a patient returns to the cn or on the current date
if the patient does not return.
*/
transfer_dates as (
    select
        pat_key,
        patient_key,
        encounter_date,
        completed_ind,
        consecutive_flag,
        case
            when completed_ind = 99 then encounter_date
        end as inactive_start_dt,
        /*
        defining inactive end date as the date of the next completed visit
        in a data set ordered by patient_key, date, and completed_ind.
        In some instances patients had a visit and transferred on the same day.
        Ordering on completed_ind here to ensure that the transfer
        (completed_ind = 99) is read as the most recent encounter
        - otherwise it's possible that it will look like the inactive period
        ended on the same day it began.
        */
        case
            when completed_ind = 99
                and lead(
                    patient_key
                ) over (
                    order by patient_key, encounter_date, completed_ind
                ) = patient_key
                then lead(
                    encounter_date
                ) over (order by patient_key, encounter_date, completed_ind)
            when completed_ind = 99
                and (
                    lead(
                        patient_key
                    ) over (
                        order by patient_key, encounter_date, completed_ind
                    ) != patient_key
                    or lead(
                        patient_key
                    ) over (
                        order by patient_key, encounter_date, completed_ind
                    ) is null
                )
                then current_date
        end as inactive_end_dt

    from
        consecutive_flag

    where
        consecutive_flag != 1
)

select
    pat_key,
    patient_key,
    inactive_start_dt,
    inactive_end_dt
from
    transfer_dates
where
    completed_ind = 99
