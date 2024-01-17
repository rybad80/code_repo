with cancer_dx as (
    select
        diagnosis.dx_key
    from
        {{source('cdw', 'epic_grouper_item')}} as epic_grouper_item
        inner join {{source('cdw', 'epic_grouper_diagnosis')}} as epic_grouper_diagnosis
            on epic_grouper_item.epic_grouper_key = epic_grouper_diagnosis.epic_grouper_key
        inner join {{source('cdw', 'diagnosis')}} as diagnosis
            on epic_grouper_diagnosis.dx_key = diagnosis.dx_key
    where lower(epic_grouper_nm) like '%oncology%'
    group by
        diagnosis.dx_key
),
anatomic as (
    select procedure_order_all.pat_key,
        procedure_order_all.encounter_date,
        proc_nm
    from {{ref ('procedure_order_all')}} as procedure_order_all
        inner join {{source('cdw', 'procedure')}} as procedure --noqa: L029
            on procedure_order_all.proc_key = procedure.proc_key
    where lower(proc_cat) = 'pathology chop'
        and procedure_order_all.encounter_date >= '2011-01-01'
),
consult as (
    select
        procedure_order_all.pat_key,
        procedure_order_all.encounter_date,
        proc_nm
    from {{ref ('procedure_order_all')}} as procedure_order_all
        inner join {{source('cdw','procedure')}} as procedure --noqa: L029
            on procedure_order_all.proc_key = procedure.proc_key
    where
        lower(proc_nm) like '%consult to general surgery%'
        and procedure_order_all.encounter_date >= '2011-01-01'
),

critera_ind as (
    select
        stg_encounter.visit_key,
        max(case when cancer_dx.dx_key is not null then 1 else 0 end) as cancer_dx_ind,
        max(case when anatomic.pat_key is not null then 1 else 0 end) as anatomical_ind,
        max(case when consult.pat_key is not null then 1 else 0 end) as consult_ind
    from
        {{ref ('stg_encounter')}} as stg_encounter
        --for cancer dx
        left join {{ref ('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on diagnosis_encounter_all.visit_key = stg_encounter.visit_key
        left join cancer_dx
            on diagnosis_encounter_all.dx_key = cancer_dx.dx_key
        --for anatomical
        left join anatomic
            on anatomic.pat_key = stg_encounter.pat_key
                and anatomic.encounter_date = stg_encounter.encounter_date
        --for consult
        left join consult
            on consult.pat_key = stg_encounter.pat_key
                and consult.encounter_date = stg_encounter.encounter_date
    group by
        stg_encounter.visit_key
),

inpatient_encounters as (
    select
        stg_encounter.visit_key,
        1 as inpatient_encounter_ind
    from
        {{ref ('stg_encounter')}} as stg_encounter
        --for inpatient encounters
        left join {{ref('provider_encounter_care_team')}} as provider_encounter_care_team
            on stg_encounter.visit_key = provider_encounter_care_team.visit_key
        left join {{source('clarity_ods', 'ept_care_teams')}} as ept_care_teams
            on stg_encounter.csn = ept_care_teams.pat_enc_csn_id
    where
        (--region ip encounters criteria
        -- long-term treatment 
        (provider_encounter_care_team.provider_care_team_start_date
            != provider_encounter_care_team.provider_care_team_end_date
        or provider_encounter_care_team.provider_care_team_end_date is null
        )
        -- oncology providers
        and (provider_encounter_care_team.provider_care_team_group_category = 'oncology')
        )
        or ept_care_teams.care_teams_id = '521' --record id for oncology vascular
        --end region
    group by
        stg_encounter.visit_key
)

select
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.patient_name,
    stg_encounter.visit_key,
    stg_encounter.encounter_date as visit_date,
    stg_encounter.department_name,
    stg_encounter.visit_type,
    stg_encounter.encounter_type,
    stg_encounter.appointment_status,
    --EDGE definition: The age, in years, of the patient at the time of the visit,
    stg_encounter.age_years,
    stg_encounter_chop_market.chop_market,
    cancer_dx_ind,
    anatomical_ind,
    consult_ind,
    case when cancer_dx_ind = 1
    --and (anatomical_ind = 1 or consult_ind = 1) removing due to new criteria
    then 1 else 0 end as first_chop_dx_ind
from
    {{ref ('stg_encounter')}} as stg_encounter
    left join {{ref('stg_encounter_chop_market')}} as stg_encounter_chop_market
        on stg_encounter.visit_key = stg_encounter_chop_market.visit_key
    left join critera_ind
        on stg_encounter.visit_key = critera_ind.visit_key
    --for inpatient encounters
    left join inpatient_encounters
        on stg_encounter.visit_key = inpatient_encounters.visit_key
where
    (
    (--region op encounters criteria
    -- visits in onco clinics
    stg_encounter.department_id in
    (101001118,  --'bgr oncology day hosp'
    101001027, --'vnj hem onc day hosp'
    101001016, --'kop hem onc day hosp'
    89356016,  --'wood oncology day hosp'
    101001139, --'bgr onco holding'
    101001082, --'vnj oncology holding'
    101001083, --'kop oncology holding'
    89395025,  --'neuro-oncology'
    101001073, -- 'pcam radiation oncology'
    101001619, -- 'telehlth onco day hosp'
    101016057, --'BGR BH ONCOLOGY'
    101003024 --'KOPH HEM ONC DAY HOSP' as of 07/01/2021
    )
    and (--valid encounter type + appointment status
    (
    stg_encounter.encounter_type_id in
    (50,   --appointment
    3,   --hospital encounter
    160, --care coordination
    101 --office visit
    )
    and stg_encounter.appointment_status_id in
    (2,  --completed
    6 --arrived
    )
    )
    or (
    stg_encounter.encounter_type_id in
    (52,  --anesthesia
    53  --anesthesia event
    )
    and stg_encounter.appointment_status_id = -2 -- not applicable
    )
    )
    --endregion
    )
    or inpatient_encounter_ind = 1
    )
    --remove future encounters
    and visit_date < current_date
    and stg_encounter.encounter_date >= '2011-01-01'
