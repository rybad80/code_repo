select 
    patient.pat_key,
    max(
        case 
            when lower(patient.lang) in ('english', 'spanish', 'arabic') then lower(patient.lang)
            when patient.lang is null then 'blank'
            else 'other'
        end
    ) as pat_pref_lang
from 
    {{ source('cdw', 'patient') }} as patient
group by
    patient.pat_key