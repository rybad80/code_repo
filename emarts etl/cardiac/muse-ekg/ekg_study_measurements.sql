with sq_ekg_study_measurements as (
    select
        p_onset,
        p_offset,
        qrs_onset,
        qrs_offset,
        t_onset,
        t_offset,
        numqrscomplexes,
        qrs_duration,
        qt_interval,
        qtc_bazett,
        pr_interval,
        ventrate,
        avgrrinterval,
        testid || 'Muse' as ekg_study_id
    from {{ source('muse_ods', 'muse_tstecgmeasmatrix') }}
)

select
    cast(ekg_study.ekg_study_id as varchar(25)) as ekg_study_id,
    cast(p_onset as integer) as p_onset,
    cast(p_offset as integer) as p_offset,
    cast(qrs_onset as integer) as qrs_onset,
    cast(qrs_offset as integer) as qrs_offset,
    cast(t_onset as integer) as t_onset,
    cast(t_offset as integer) as t_offset,
    cast(numqrscomplexes as integer) as numqrscomplexes,
    cast(qrs_duration as integer) as qrs_duration,
    cast(qt_interval as integer) as qt_interval,
    cast(qtc_bazett as integer) as qtc_bazett,
    cast(pr_interval as integer) as pr_interval,
    cast(ventrate as integer) as ventrate,
    cast(avgrrinterval as integer) as avgrrinterval
from sq_ekg_study_measurements
inner join {{ ref('ekg_study') }} as ekg_study on sq_ekg_study_measurements.ekg_study_id = ekg_study.ekg_study_id
