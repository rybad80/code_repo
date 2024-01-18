with sq_ekg_study_resting_measurements as (
    select
        systolicbp,
        diastolicbp,
        ventricularrate,
        atrialrate,
        p_rinterval,
        qrsduration,
        q_tinterval,
        qtccalculation,
        paxis,
        raxis,
        taxis,
        qrscount,
        qonset,
        qoffset,
        ponset,
        poffset,
        toffset,
        demographicbits,
        ecgsamplebase,
        ecgsampleexponent,
        options12sl,
        qtcfredericia,
        qtcframingham,
        qtc_rr,
        testid || 'Muse' as ekg_study_id
    from {{ source('muse_ods', 'muse_tstrestingecgmeasurement') }}
)

select
    cast(ekg_study.ekg_study_id as varchar(25)) as ekg_study_id,
    cast(systolicbp as integer) as systolicbp,
    cast(diastolicbp as integer) as diastolicbp,
    cast(ventricularrate as integer) as ventricularrate,
    cast(atrialrate as integer) as atrialrate,
    cast(p_rinterval as integer) as p_rinterval,
    cast(qrsduration as integer) as qrsduration,
    cast(q_tinterval as integer) as q_tinterval,
    cast(qtccalculation as integer) as qtccalculation,
    cast(paxis as integer) as paxis,
    cast(raxis as integer) as raxis,
    cast(taxis as integer) as taxis,
    cast(qrscount as integer) as qrscount,
    cast(qonset as integer) as qonset,
    cast(qoffset as integer) as qoffset,
    cast(ponset as integer) as ponset,
    cast(poffset as integer) as poffset,
    cast(toffset as integer) as toffset,
    cast(demographicbits as bigint) as demographicbits,
    cast(ecgsamplebase as integer) as ecgsamplebase,
    cast(ecgsampleexponent as integer) as ecgsampleexponent,
    cast(options12sl as bigint) as options12sl,
    cast(qtcfredericia as integer) as qtcfredericia,
    cast(qtcframingham as integer) as qtcframingham,
    cast(qtc_rr as integer) as qtc_rr
from sq_ekg_study_resting_measurements
inner join {{ ref('ekg_study') }} as ekg_study
     on sq_ekg_study_resting_measurements.ekg_study_id = ekg_study.ekg_study_id
