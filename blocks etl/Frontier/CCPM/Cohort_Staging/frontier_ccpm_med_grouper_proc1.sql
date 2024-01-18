select
    mao.mrn,
    mao.medication_start_date,
    mao.medication_end_date,
    max(case
        when
        eg.epic_grouper_id = 121188
            and mao.medication_start_date >= '2021-01-01'
        then 1 else 0 end)
    as erx_onco_inv_ind,
    max(case
        when
        eg.epic_grouper_id = 121245
            and mao.medication_start_date >= '2021-01-01'
        then 1 else 0 end)
    as erx_onco_enz_inhibitors_ind,
    max(case
        when
        eg.epic_grouper_id = 121243
            and mao.medication_start_date >= '2021-01-01'
        then 1 else 0 end)
    as erx_onco_exclusion_meds_ind
from
    {{ source('cdw', 'epic_grouper_item') }} as eg
    left join {{ source('cdw', 'epic_grouper_medication') }} as em
        on eg.epic_grouper_key = em.epic_grouper_key
    left join {{ ref('medication_order_administration') }} as mao
        on em.med_key = mao.med_key
    left join {{ ref('stg_patient') }} as stg_patient
        on mao.mrn = stg_patient.mrn
where
    eg.epic_grouper_id in (121188, 121245, 121243)
    and visit_key is not null
    and medication_start_date >= '2021-01-01'
group by
    mao.mrn,
    mao.medication_start_date,
    mao.medication_end_date
