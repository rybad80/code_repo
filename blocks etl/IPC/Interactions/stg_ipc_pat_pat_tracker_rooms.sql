select
    patient_location_event.pat_loc_event_key,
    patient_location_event.pat_loc_finder_key,
    upper(coalesce(
        patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
    ) as waiting_room_name,
    patient_location_event.visit_key,
    patient_location_event.pat_key,
    date(patient_location_event.start_dt) as encounter_date,
    patient_location_event.start_dt as start_date,
    patient_location_event.end_dt as end_date
from
    {{source('cdw', 'patient_location_event')}} as patient_location_event
    inner join {{source('cdw', 'patient_location_finder')}} as patient_location_finder
        on patient_location_finder.pat_loc_finder_key = patient_location_event.pat_loc_finder_key
where
    (
        lower(coalesce(
            patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
        ) like '%exam%'
        or lower(coalesce(
            patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
        ) like '% ex %'
        or lower(coalesce(
            patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
        ) like '%wait%'
        or lower(coalesce(
            patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
        ) like '%room%'
        or lower(coalesce(
            patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
        ) like '%bed%'
        or lower(coalesce(
            patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
        ) like '%vitals%'
    )
    and lower(coalesce(
        patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
    ) not like '%assess%'
    and lower(coalesce(
        patient_location_finder.pat_loc_display_nm, patient_location_finder.pat_loc_record_nm)
        ) not like '%discharge%'
    and extract( --noqa: PRS
        days from patient_location_event.end_dt - patient_location_event.start_dt) < 1
    and patient_location_event.end_dt <= current_timestamp
