select
    flowsheet_lda.patient_name,
    flowsheet_lda.pat_key,
    flowsheet_lda.mrn,
    flowsheet_lda.ip_lda_id,
    flowsheet_lda.lda_types,
    flowsheet_lda.lda_description,
    flowsheet_lda.placement_instant,
    min(recorded_date) as first_documenation_date,
    flowsheet_lda.removal_instant,
    stg_patient.death_date,
    case when flowsheet_lda.removal_instant > stg_patient.death_date then stg_patient.death_date
        else flowsheet_lda.removal_instant
        end as removal_instant_fixed,
    coalesce(placement_instant, first_documenation_date) as placement_instant_fixed,
    case when placement_instant_fixed < '2018-07-01' then '2018-07-01 00:00:00' else placement_instant_fixed
        end as start_date,
    case when removal_instant_fixed > current_date then current_date else removal_instant_fixed end as end_date
from
    {{ref('flowsheet_lda')}} as flowsheet_lda
    inner join {{ref('stg_patient')}} as stg_patient on stg_patient.pat_key = flowsheet_lda.pat_key
where
    flowsheet_lda.recorded_date >= '2018-07-01' -- some documentation on the line post this date
    and ( -- Central Line Types
        upper(flowsheet_lda.lda_types) like '%PORT%'
        or upper(flowsheet_lda.lda_types) like '%PICC%'
        or upper(flowsheet_lda.lda_types) like '%CVC%'
        )
group by
    flowsheet_lda.patient_name,
    flowsheet_lda.pat_key,
    flowsheet_lda.mrn,
    flowsheet_lda.ip_lda_id,
    flowsheet_lda.placement_instant,
    flowsheet_lda.removal_instant,
    stg_patient.death_date,
    flowsheet_lda.lda_types,
    flowsheet_lda.lda_description
