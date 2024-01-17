select
    kaps_claim_file.file_id,
    kaps_incident_1.incid_id as incident_id,
    kaps_claim_file.file_nm,
    kaps_event_person_affected.person_type,
    kaps_event_location.event_dt as event_date,
    kaps_event_location.event_site,
    kaps_event_location.event_building,
    kaps_incident_1.incid_type as event_type,
    kaps_incident_1.entered_by as incident_entered_by,
    kaps_claim_file.enter_dt as entered_date,
    kaps_event_location.event_prgm as event_program,
    replace(kaps_incident_1.event_desc, '&nbsp', '') as event_description,
    kaps_event_outcome.event_recurr as event_recurrence,
    kaps_event_outcome.sevr_recurr as severity_recurrence,
    kaps_event_outcome.sevr_lvl as severity_level,
    kaps_event_person_affected.person_mrn as mrn,
    kaps_event_person_affected.person_gender,
    kaps_event_person_affected.person_age_int
from
{{source('cdw', 'kaps_claim_file')}} as kaps_claim_file
left join {{source('cdw', 'kaps_incident_1')}} as kaps_incident_1
    on kaps_claim_file.file_id = kaps_incident_1.file_id
left join {{source('cdw', 'kaps_event_location')}} as kaps_event_location
    on kaps_incident_1.incid_id = kaps_event_location.incid_id
left join {{source('cdw', 'kaps_event_person_affected')}} as kaps_event_person_affected
    on kaps_incident_1.incid_id = kaps_event_person_affected.incid_id
left join {{source('cdw', 'kaps_event_outcome')}} as kaps_event_outcome
    on kaps_event_outcome.incid_id  = kaps_incident_1.incid_id
where
    kaps_event_location.event_dt is not null
