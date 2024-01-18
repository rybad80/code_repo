with patient_match_logic as (
    select
        patient_match.pat_key,
        patient_match.src_sys_id
    from {{ source('cdw', 'patient_match') }} as patient_match
    where lower(patient_match.src_sys_nm) = 'muse'
    union all
    select
        patient.pat_key,
        patient_mismatch.src_sys_id
    from {{ source('cdw', 'patient_mismatch') }} as patient_mismatch
    inner join {{ source('cdw', 'patient') }} as patient
        on patient_mismatch.pat_mrn_id = patient.pat_mrn_id
    where lower(patient_mismatch.src_sys_nm) = 'muse'
        and patient_mismatch.match_pct = 88
        and patient_mismatch.src_sys_dob_null_ind = 1
),

indication_text as (
    select
        muse_tsttests.testid,
        muse_tstdiagnosisdetails.nthoccur,
        case when muse_tstdiagnosisdetails.statementnumber = 0
            then muse_tstdiagnosisdetails.statementtext
            else muse_cfgstatements.fulltext end as indication
    from
        {{ source('muse_ods', 'muse_tsttests') }} as muse_tsttests
    left join {{ source('muse_ods', 'muse_tstdiagnosisdetails') }} as muse_tstdiagnosisdetails
        on muse_tstdiagnosisdetails.testid = muse_tsttests.testid
    left join {{ source('muse_ods', 'muse_cfgstatements') }} as muse_cfgstatements
        on muse_tstdiagnosisdetails.statementnumber = muse_cfgstatements.statementnumber
            and muse_cfgstatements.testtype = 1
),

indication_order as (
    select
        testid,
        regexp_replace(group_concat('~' || lpad(nthoccur, 4, '0')
            || '~' || indication, ' '), '~[0-9]{4}~', '') as indication
    from indication_text
    group by testid
),

sq_ekg_study as (
    select
        muse_tsttests.testid as source_system_id,
        muse_cfgtesttypequalifiers.fullname as study_type,
        muse_tsttestdemographics.ordernumber as order_number,
        patient_match_logic.pat_key as patient_key,
        muse_cfglocations.fullname as location,
        to_char(muse_tsttestdemographics.acquisitiondatetime_dt, 'yyyymmdd') as study_date_key,
        case when muse_tsttestdemographics.acquisitiontech_fullname__last is null
            then muse_tsttestdemographics.acquisitiontech_fullname__first
            when muse_tsttestdemographics.acquisitiontech_fullname__first is null
                then muse_tsttestdemographics.acquisitiontech_fullname__last
            else
                trim(trim(isnull(muse_tsttestdemographics.acquisitiontech_fullname__last, '')) || ', '
                || trim(isnull(muse_tsttestdemographics.acquisitiontech_fullname__first, ''))) end as acquisition_tech,
        case when muse_tsttestdemographics.orderingmd_fullname__last is null
            then muse_tsttestdemographics.orderingmd_fullname__first
            when muse_tsttestdemographics.orderingmd_fullname__first is null
                then muse_tsttestdemographics.orderingmd_fullname__last
            else
            trim(trim(isnull(muse_tsttestdemographics.orderingmd_fullname__last, '')) || ', '
            || trim(isnull(muse_tsttestdemographics.orderingmd_fullname__first, ''))) end as ordering_physician,
        case when muse_tsttestdemographics.referringmd_fullname__last is null
            then muse_tsttestdemographics.referringmd_fullname__first
            when muse_tsttestdemographics.referringmd_fullname__first is null
                then muse_tsttestdemographics.referringmd_fullname__last
            else
            trim(trim(isnull(muse_tsttestdemographics.referringmd_fullname__last, '')) || ', '
            || trim(isnull(muse_tsttestdemographics.referringmd_fullname__first, ''))) end as referring_physician,
        case when muse_tsttestdemographics.overreader_fullname__last is null
            then muse_tsttestdemographics.overreader_fullname__first
            when muse_tsttestdemographics.overreader_fullname__first is null
                then muse_tsttestdemographics.overreader_fullname__last
            else
            trim(trim(isnull(muse_tsttestdemographics.overreader_fullname__last, '')) || ', '
            || trim(isnull(muse_tsttestdemographics.overreader_fullname__first, ''))) end as confirming_physician,
        replace(muse_tsttestdemographics.testreason, '|', ' ') as test_reason,
        replace(indication_order.indication, '|', ' ') as indication,
        to_char(muse_tsttestdemographics.acquisitiondatetime_dt, 'hh24miss') as study_time,
        case when muse_tsttestdemographics.confirmdatetime_dt is not null
            then 0 else 1 end as confirmed,
        case when muse_quenormaldiscard.testid is null
            then 0 else 1 end as discarded,
        muse_tsttests.testid || 'Muse' as ekg_study_id,
        'Muse' as source_system
    from {{ source('muse_ods', 'muse_tsttests') }} as muse_tsttests
    inner join {{ source('muse_ods', 'muse_tstpatientdemographics') }} as muse_tstpatientdemographics
         on muse_tsttests.testid = muse_tstpatientdemographics.testid
    inner join {{ source('muse_ods', 'muse_tsttestdemographics') }} as muse_tsttestdemographics
         on muse_tsttests.testid = muse_tsttestdemographics.testid
    inner join {{ source('muse_ods', 'muse_cfgtesttypequalifiers') }} as muse_cfgtesttypequalifiers
         on muse_tsttests.testtype = muse_cfgtesttypequalifiers.testtypequalifierid
    left join {{ source('muse_ods', 'muse_cfglocations') }} as muse_cfglocations
         on muse_cfglocations.locationid = muse_tsttestdemographics.location
    left join {{ source('muse_ods', 'muse_quenormaldiscard') }} as muse_quenormaldiscard
         on muse_tsttests.testid = muse_quenormaldiscard.testid
    left join {{ source('muse_ods', 'muse_hisorders') }} as muse_hisorders
         on muse_hisorders.placersordernumber = muse_tsttestdemographics.ordernumber
    left join indication_order on muse_tsttests.testid = indication_order.testid
    left join patient_match_logic on muse_tsttests.testid = patient_match_logic.src_sys_id
    where
        muse_tsttests.testid > 0-- negative are temporary copies
        and muse_tsttests.testtype in(1, 3, 4)-- ekg, hires, exercise
)

select
    cast(ekg_study_id as varchar(25)) as ekg_study_id,
    cast(study_date_key as integer) as study_date_key,
    cast(patient_key as bigint) as patient_key,
    cast(source_system_id as integer) as source_system_id,
    cast(source_system as varchar(20)) as source_system,
    cast(location as varchar(100)) as location,
    cast(acquisition_tech as varchar(100)) as acquisition_tech,
    cast(ordering_physician as varchar(100)) as ordering_physician,
    cast(referring_physician as varchar(100)) as referring_physician,
    cast(confirming_physician as varchar(100)) as confirming_physician,
    cast(order_number as varchar(22)) as order_number,
    cast(test_reason as varchar(64)) as test_reason,
    cast(indication as varchar(1000)) as indication,
    cast(study_time as varchar(8)) as study_time,
    cast(study_type as varchar(50)) as study_type
from sq_ekg_study
where
    patient_key is not null
    and discarded = 0
