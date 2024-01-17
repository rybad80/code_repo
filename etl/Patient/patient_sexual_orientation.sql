select
    {{
        dbt_utils.surrogate_key([
            'pat_sexual_orientation.pat_id',
            'pat_sexual_orientation.line',
        ])
    }} as patient_sexual_orientation_key,
    stg_patient.patient_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    pat_sexual_orientation.pat_id,
    pat_sexual_orientation.line,
    zc_sexual_orientation.name as sexual_orientation,
    zc_sexual_orientation.sexual_orientation_c as sexual_orientation_id
from
    {{ref('stg_patient')}} as stg_patient
    inner join {{source('clarity_ods','pat_sexual_orientation')}} as pat_sexual_orientation
        on stg_patient.pat_id = pat_sexual_orientation.pat_id
    inner join  {{source('clarity_ods','zc_sexual_orientation')}} as zc_sexual_orientation
        on pat_sexual_orientation.sexual_orientatn_c = zc_sexual_orientation.sexual_orientation_c
