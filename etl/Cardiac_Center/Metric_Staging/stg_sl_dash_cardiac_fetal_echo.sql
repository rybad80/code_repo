select distinct
    cardiac_study.cardiac_study_id as echo_fetal_study_id,
    cardiac_study.mrn,
    cardiac_study.pat_key,
    cardiac_study.study_date,
    case
        when fetal_heart_program.mother_mrn is not null
        then 'Fetal Echos w/ Significant Fetal CVD'
        else 'Fetal Echos w/o Significant Fetal CVD'
    end as drill_down,
    {{
        dbt_utils.surrogate_key([
            'echo_fetal_study_id'
        ])
    }} as primary_key,
    'cardiac_fetal_echo' as metric_id
from {{ ref('cardiac_study')}} as cardiac_study
    left join {{ ref('fetal_heart_program')}} as fetal_heart_program
    on cardiac_study.mrn = fetal_heart_program.mother_mrn
    and cardiac_study.study_date between add_months(fetal_heart_program.due_date, -9)
    and fetal_heart_program.due_date
where
    lower(cardiac_study.cardiac_type) = 'fetal echo'
    and lower(cardiac_study.study_location) = 'chop main'
