/* An ECMO Run is a period of continuous treatment.
This code uses the `Hour on ECMO` flowsheet row as the `source of truth` to indicate
that a patient is receiving ECMO treatment. This field was suggested by Jim Connelly and
Sue Williams from the ECMO team.

It is possible for a patient to have more than one ECMO run during a hospitalization.
Run Start = First `Hour on ECMO` Flowsheet recorded
Run End = Final `Hour on ECMO` Flowsheet recorded

If a patient does not recieve ECMO treatment for 12 hours or longer, the run is over.
If the patient subsequently recieves ECMO treatment again, that is considered a new run.
*/

with ecmo_hour_ts as (
    select distinct
        pat_key,
        visit_key,
        recorded_date
    from
        {{ ref('flowsheet_all') }}
    where
        flowsheet_id = 40060202 /* hour on ecmo */
        and meas_val is not null
),

ecmo_starts as (
    /* find ecmo starts
    These are datapoints for a given run in which there is no prior ecmo flowsheet row
    for at least 12 hours. we are using 12 hours as cutoff point */
    select
        current_ts.pat_key,
        current_ts.visit_key,
        current_ts.recorded_date as ecmo_start_datetime,
        lead(current_ts.recorded_date) over (
            partition by current_ts.visit_key
            order by current_ts.recorded_date
        ) as next_ecmo_start_ts

    from
        ecmo_hour_ts as current_ts
        left join ecmo_hour_ts as prior_ts
            on prior_ts.visit_key = current_ts.visit_key
                and prior_ts.recorded_date < current_ts.recorded_date
                and prior_ts.recorded_date >= current_ts.recorded_date
                - cast('12 hours' as interval)

    where
        /* next `ECMO Type` flowsheet row is > 12 hours OR does not exist */
        prior_ts.recorded_date is null
),

ecmo_run as (
    select
        ecmo_starts.pat_key,
        ecmo_starts.visit_key,
        ecmo_starts.ecmo_start_datetime,
        max(ecmo_hour_ts.recorded_date) as ecmo_end_datetime

    from
        ecmo_starts
        left join ecmo_hour_ts
            on ecmo_hour_ts.visit_key = ecmo_starts.visit_key
                and ecmo_hour_ts.recorded_date > ecmo_starts.ecmo_start_datetime
                and (ecmo_hour_ts.recorded_date < ecmo_starts.next_ecmo_start_ts
                    or ecmo_starts.next_ecmo_start_ts is null)

    group by
        ecmo_starts.pat_key,
        ecmo_starts.visit_key,
        ecmo_starts.ecmo_start_datetime
),

ecmo_types as (
    select distinct
        ecmo_run.pat_key,
        ecmo_run.ecmo_start_datetime,
        flowsheet_all.meas_val as cannulation_type
    from
        ecmo_run
        inner join {{ ref('flowsheet_all') }} as flowsheet_all
            on flowsheet_all.visit_key = ecmo_run.visit_key
                and flowsheet_all.recorded_date >= ecmo_run.ecmo_start_datetime
                and flowsheet_all.recorded_date <= ecmo_run.ecmo_end_datetime
    where
        flowsheet_id = 40060256
        and meas_val is not null
),

ecmo_types_concat as (
    select
        ecmo_types.pat_key,
        ecmo_types.ecmo_start_datetime,
        group_concat(ecmo_types.cannulation_type, ';') as cannulation_type
    from
        ecmo_types
    group by
        ecmo_types.pat_key,
        ecmo_types.ecmo_start_datetime
)

select
    ecmo_run.visit_key,
    coalesce(adt_bed.patient_name, first_adt_bed.patient_name) as patient_name,
    coalesce(adt_bed.mrn, first_adt_bed.mrn) as mrn,
    coalesce(adt_bed.dob, first_adt_bed.dob) as dob,
    coalesce(adt_bed.csn, first_adt_bed.csn) as csn,
    coalesce(adt_bed.encounter_date, first_adt_bed.encounter_date) as encounter_date,
    coalesce(
        adt_bed.hospital_admit_date,
        first_adt_bed.hospital_admit_date
    ) as hospital_admit_date,
    coalesce(
        adt_bed.hospital_discharge_date,
        first_adt_bed.hospital_discharge_date
    ) as hospital_discharge_date,
    ecmo_run.ecmo_start_datetime,
    /* if only one `ECMO Type` flowsheet recorded, then `ecmo_run.ecmo_end_datetime`
    would be null */
    coalesce(
        ecmo_run.ecmo_end_datetime,
        ecmo_run.ecmo_start_datetime
    ) as ecmo_end_datetime,
    ecmo_types_concat.cannulation_type,
    round(
        extract( --noqa: PRS
            epoch from (
                ecmo_run.ecmo_end_datetime - ecmo_run.ecmo_start_datetime
            )
        ) / 3600.0, /* (60 secs / minute) * (60 minutes / hour) = 3600 */
    2) as ecmo_run_time_hours,
    coalesce(adt_bed.bed_name, first_adt_bed.bed_name) as bed_at_ecmo_start,
    coalesce(adt_bed.room_name, first_adt_bed.room_name) as room_name_at_ecmo_start,
    coalesce(
        adt_bed.department_name,
        first_adt_bed.department_name
    ) as department_name_at_ecmo_start,
    coalesce(
        adt_bed.department_group_name,
        first_adt_bed.department_group_name
    ) as department_group_name_at_ecmo_start,
    coalesce(
        adt_bed.bed_care_group,
        first_adt_bed.bed_care_group
    ) as bed_care_group_at_ecmo_start,
    coalesce(adt_bed.pat_key, first_adt_bed.pat_key) as pat_key,
    coalesce(adt_bed.hsp_acct_key, first_adt_bed.hsp_acct_key) as hsp_acct_key

from
    ecmo_run
    left join {{ ref('adt_bed') }} as adt_bed
        on adt_bed.visit_key = ecmo_run.visit_key
            and adt_bed.enter_date <= ecmo_run.ecmo_start_datetime
            and coalesce(
                adt_bed.exit_date,
                current_date
            ) > ecmo_run.ecmo_start_datetime
    /* some patients have a first `ECMO Type` ts before ADT admission. We can use their
    first adt bed in those instances. */
    left join {{ ref('adt_bed') }} as first_adt_bed
        on first_adt_bed.visit_key = ecmo_run.visit_key
            and first_adt_bed.all_bed_order = 1
    left join ecmo_types_concat
        on ecmo_types_concat.pat_key = ecmo_run.pat_key
            and ecmo_types_concat.ecmo_start_datetime = ecmo_run.ecmo_start_datetime
