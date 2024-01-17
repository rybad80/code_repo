with patients as (
    select distinct
        pat_key,
        'Enrolled' as status
    from
        {{source('cdw', 'patient_list_info')}} as patient_list_info
        inner join {{source('cdw', 'patient_list')}} as patient_list
            on patient_list.pat_lst_info_key = patient_list_info.pat_lst_info_key
    where
        lower(display_nm) = 'cardiac research registry - kim lin, md'
),
reporttext as (
    select
        testid,
        regexp_replace(
            group_concat('~' || lpad(nthoccur, 4, '0') || '~' || indication, ' '), '~[0-9]{4}~', ''
    ) as report_text
    from (
        select
            muse_tsttests.testid,
            case
                when muse_tstdiagnosisdetails.statementnumber = 0 then muse_tstdiagnosisdetails.statementtext
                else muse_cfgstatements.fulltext
            end as indication,
            nthoccur
        from
            {{source('ccis_ods', 'muse_tsttests')}} as muse_tsttests
            left join {{source('ccis_ods', 'muse_tstdiagnosisdetails')}} as muse_tstdiagnosisdetails
                on muse_tstdiagnosisdetails.testid = muse_tsttests.testid --noqa: L026,L028
            left join {{source('ccis_ods', 'muse_cfgstatements')}} as muse_cfgstatements
                on muse_tstdiagnosisdetails.statementnumber = muse_cfgstatements.statementnumber --noqa: L026,L028
                and muse_cfgstatements.testtype = 1 --noqa: L026,L028
    ) as sub --noqa: L025
    group by
        testid
)
select
    patient_match.pat_key,
    muse_tsttestdemographics.acquisitiondatetime_dt as acquisition_date,
    muse_tstrestingecgmeasurement.ventricularrate as ventricular_rate,
    muse_tstrestingecgmeasurement.p_rinterval as pr_interval,
    muse_tstrestingecgmeasurement.qrsduration as qrs_duration,
    muse_tstrestingecgmeasurement.q_tinterval as qt_interval,
    muse_tstrestingecgmeasurement.qtccalculation as qtc_calculation,
    reporttext.report_text,
    patients.status as study_status,
    muse_tsttestdemographics.testid
from
    {{source('ccis_ods', 'muse_tstpatientdemographics')}} as muse_tstpatientdemographics
    inner join {{source('ccis_ods', 'muse_tsttestdemographics')}} as muse_tsttestdemographics
        on muse_tstpatientdemographics.testid = muse_tsttestdemographics.testid
    inner join {{source('ccis_ods', 'muse_tsttests')}} as muse_tsttests
        on muse_tstpatientdemographics.testid = muse_tsttests.testid
    left join {{source('ccis_ods', 'muse_tstrestingecgmeasurement')}} as muse_tstrestingecgmeasurement
        on muse_tstrestingecgmeasurement.testid = muse_tsttestdemographics.testid
    left join {{source('ccis_ods', 'muse_tstecgmeasmatrix')}} as muse_tstecgmeasmatrix
        on muse_tstecgmeasmatrix.testid = muse_tsttestdemographics.testid
    left join {{source('ccis_ods', 'muse_quenormaldiscard')}} as muse_quenormaldiscard
        on muse_tstpatientdemographics.testid = muse_quenormaldiscard.testid
    left join reporttext
        on muse_tsttestdemographics.testid = reporttext.testid
    inner join {{source('cdw', 'patient_match')}} as patient_match
        on muse_tsttestdemographics.testid = patient_match.src_sys_id
        and lower(patient_match.src_sys_nm) = 'muse'
    inner join patients
        on patient_match.pat_key = patients.pat_key
where
    muse_quenormaldiscard.testid is null
