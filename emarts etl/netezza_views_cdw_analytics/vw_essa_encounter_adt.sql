select
    ve.visit_key as "Encounter Key",
    ve.visit_event_key as "ADT Event Key",
    dep.dept_key as "ADT Department Key",
    dep.dept_nm as "ADT Department Name",
    dep.dept_abbr as "ADT Department Abbreviation",
    rm.room_nm as "ADT Room Name",
    rm.room_key as "ADT Room Key",
    bd.bed_key as "ADT Bed Key",
    bd.bed_nm as "ADT Bed Name",
    adtevent.dict_nm as "ADT Event Type",
    adtsubevent.dict_nm as "ADT Event Type Status",
    adtpatcls.dict_nm as "ADT Patient Class",
    adtpatsvc.dict_nm as "ADT Service",
    adtbcls.dict_nm as "ADT Base Class",
    ve.eff_event_dt as "ADT Effective DateTime",
    date(ve.eff_event_dt) as "ADT Effective Date",
    date_part('HOUR'::"VARCHAR", ve.eff_event_dt) as "ADT Effective Time Hour",
    -- CASE
    --     WHEN (enc."Last Encounter Stay Indicator" = 'YES' ::"VARCHAR")
    --         and ((adtpatcls.dict_nm = 'Inpatient' ::"VARCHAR")
    --         or adtpatcls.dict_nm = 'Admit After Surgery' ::"VARCHAR"))
    --         or (adtpatcls.dict_nm = 'Admit After Surgery-IP' ::"VARCHAR")
    --         or adtpatcls.dict_nm = 'IP Deceased Organ Donor' ::"VARCHAR"))))
    --             THEN 'Hospital IP Encounter'::"VARCHAR"
    --     WHEN (enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR" AND adtpatcls.dict_nm = 'Outpatient'::"VARCHAR")
    --         OR (adtpatcls.dict_nm = 'Day Surgery'::"VARCHAR")
    --             THEN 'Hospital OP Encounter'::"VARCHAR"
    --     WHEN enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR" AND adtpatcls.dict_nm = 'Recurring Outpatient'::"VARCHAR"
    --             THEN 'Hospital OP Encounter'::"VARCHAR"
    --     WHEN enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR" AND adtpatcls.dict_nm = 'Emergency'::"VARCHAR"
    --             THEN 'Hospital ED Encounter'::"VARCHAR"
    --     WHEN (enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR" AND adtpatcls.dict_nm = 'Observation'::"VARCHAR")
    --         OR (adtpatcls.dict_nm = 'Admit After Surgery-OBS'::"VARCHAR")
    --             THEN 'Hospital OBS Encounter'::"VARCHAR"
    --     ELSE NULL::"VARCHAR"
    -- END AS "Encounter Stay Class",
    CASE
        WHEN ((enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR") and (((adtpatcls.dict_nm = 'Inpatient'::"VARCHAR") or (adtpatcls.dict_nm = 'Admit After Surgery' ::"VARCHAR")) or ((adtpatcls.dict_nm = 'Admit After Surgery-IP'::"VARCHAR") or (adtpatcls.dict_nm = 'IP Deceased Organ Donor'::"VARCHAR")))) THEN 'Hospital IP Encounter'::"VARCHAR"
        WHEN ((enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR") AND ((adtpatcls.dict_nm = 'Outpatient'::"VARCHAR") OR (adtpatcls.dict_nm = 'Day Surgery'::"VARCHAR"))) THEN 'Hospital OP Encounter'::"VARCHAR"
        WHEN ((enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR") AND (adtpatcls.dict_nm = 'Recurring Outpatient'::"VARCHAR")) THEN 'Hospital OP Encounter'::"VARCHAR"
        WHEN ((enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR") AND (adtpatcls.dict_nm = 'Emergency'::"VARCHAR")) THEN 'Hospital ED Encounter'::"VARCHAR"
        WHEN ((enc."Last Encounter Stay Indicator" = 'YES'::"VARCHAR") AND ((adtpatcls.dict_nm = 'Observation'::"VARCHAR") OR (adtpatcls.dict_nm = 'Admit After Surgery-OBS'::"VARCHAR"))) THEN 'Hospital OBS Encounter'::"VARCHAR"
        ELSE NULL::"VARCHAR"
    END AS "Encounter Stay Class",
    ve.real_event_dt as "ADT Event Datetime"
from
    {{ source('cdw', 'visit_event') }} as ve
    inner join {{ ref('vw_essa_encounter') }} as enc on enc."Encounter Key" = ve.visit_key
    left join {{ source('cdw', 'department') }} as dep on ve.dept_key = dep.dept_key
    left join {{ source('cdw', 'master_room') }} as rm on rm.room_key = ve.room_key
    left join {{ source('cdw', 'master_bed') }} as bd on bd.bed_key = ve.bed_key
    left join {{ source('cdw', 'cdw_dictionary') }} as adtevent on adtevent.dict_key = ve.dict_adt_event_key
    left join {{ source('cdw', 'cdw_dictionary') }} as adtsubevent on adtsubevent.dict_key = ve.dict_event_subtype_key
    left join {{ source('cdw', 'cdw_dictionary') }} as adtpatcls on adtpatcls.dict_key = ve.dict_pat_class_key
    left join {{ source('cdw', 'cdw_dictionary') }} as adtpatsvc on adtpatsvc.dict_key = ve.dict_pat_svc_key
    left join {{ source('cdw', 'cdw_dictionary') }} as adtbcls on adtbcls.dict_key = ve.dict_acct_basecls_key
where
    ve.create_by = 'CLARITY' ::"VARCHAR"
    and adtsubevent.dict_nm in (('Original'::"VARCHAR")::VARCHAR(500), ('Update'::"VARCHAR")::VARCHAR(500))
    and adtevent.dict_nm <> 'Census' ::"VARCHAR"
