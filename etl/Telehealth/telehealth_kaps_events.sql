select
    kaps_claim_file.file_id,
    kaps_event_person_affected.person_type,
    kaps_event_location.event_dt as event_date,
    kaps_event_location.event_site,
    kaps_event_location.event_building,
    kaps_incident_1.incid_type as event_type,
    kaps_event_location.event_prgm as event_program,
    replace(kaps_incident_1.event_desc, '&nbsp', '') as event_description,
    kaps_claim_file.enter_dt as entered_date,
    current_date - kaps_claim_file.enter_dt as n_days_from_enter_date,
    current_date - kaps_event_location.event_dt as n_days_from_event_date,
    kaps_incident_1.incid_id as incident_id,
    kaps_event_person_affected.person_mrn as mrn
from
     {{source('cdw', 'kaps_claim_file')}} as kaps_claim_file
     left join {{source('cdw', 'kaps_incident_1')}} as kaps_incident_1
        on kaps_claim_file.file_id = kaps_incident_1.file_id
     left join {{source('cdw', 'kaps_event_location')}} as kaps_event_location
        on kaps_incident_1.incid_id = kaps_event_location.incid_id
     left join {{source('cdw', 'kaps_event_person_affected')}} as kaps_event_person_affected
        on kaps_incident_1.incid_id = kaps_event_person_affected.incid_id
where
    kaps_event_location.event_dt is not null
    and (
        lower(kaps_event_location.event_prgm) like 'telehealth urgent care'
          or lower(kaps_incident_1.event_desc) like 'telemed%'
          or lower(kaps_incident_1.event_desc) like 'telehealth%'
          or lower(kaps_incident_1.event_type) like '%telehealth%'
          or lower(kaps_incident_1.event_desc) like 'video visit%'
          or lower(kaps_incident_1.event_desc) like '%virtual%'
     )
