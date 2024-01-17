with cohort as (
select
     diagnosis_encounter_all.visit_key
from
    {{ ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
    inner join {{ source('cdw', 'epic_grouper_diagnosis')}} as epic_grouper_diagnosis
        on epic_grouper_diagnosis.dx_key = diagnosis_encounter_all.dx_key
    inner join {{ source('cdw', 'epic_grouper_item')}} as dx_grouper
        on dx_grouper.epic_grouper_key = epic_grouper_diagnosis.epic_grouper_key
where
    lower(dx_grouper.epic_grouper_nm) = 'chop icd diabetes registry'
    and diagnosis_encounter_all.encounter_date <= current_date
group by
     diagnosis_encounter_all.visit_key
),

active_flowsheets as (
    select pat_key
    from {{ ref('diabetes_icr_active_flowsheets_historical')}}
    group by pat_key
)
--lead last visit date by provider type
select
    enc.pat_key,
    enc.visit_key,
    enc.mrn,
    enc.patient_name,
    enc.dob,
    enc.encounter_type as enc_type,
    enc.age_years, --at the time of the visit
    enc.encounter_date as endo_vis_dt, --current encounter date
--last encounter:
    lead(enc.encounter_date) over (partition by enc.pat_key order by enc.encounter_date desc) as last_endo_vis_dt,
--last md visit:
    case when lower(provider.prov_type) = 'physician' and lower(enc_type) = 'office visit'
        then last_value(enc.encounter_date)
            over (partition by enc.pat_key, provider.prov_type order by enc.encounter_date)
        end as last_md_vis_dt, --most recent md visit
    case when lower(provider.prov_type) = 'physician' and lower(provider.prov_type) = 'office visit'
        then row_number()
            over (partition by enc.pat_key, provider.prov_type order by enc.encounter_date desc) end as md_rn,
--last np visit:
    case when lower(provider.prov_type) in ('nurse practitioner') and lower(provider.prov_type) = 'office visit'
        then last_value(enc.encounter_date)
            over (partition by enc.pat_key, provider.prov_type order by enc.encounter_date)
        end as last_np_vis_dt,
    case when lower(provider.prov_type) in ('nurse practitioner') and lower(provider.prov_type) = 'office visit'
        then row_number()
            over (partition by enc.pat_key, provider.prov_type order by enc.encounter_date desc) end as np_rn,
--last education visit: same logic as qv healthy planet
    case when lower(enc.visit_type) in ('advanced pump class',
                                'ahm class',
                                'ahm t1y1 class',
                                'diabetes edu less than 30 mins',
                                'diabetes education',
                                'diabetes education t1y1',
                                'insulin start',
                                'saline start',
                                'pre technology',
                                'pump class',
                                'safety skills class',
                                'upgrade pump',
                                'cgms initiation',
                                'cgms interpretation')
            or (lower(enc.visit_type) in ('video visit diabetes') and lower(provider.prov_type) = 'office visit'
                and lower(provider.prov_type) in ('dietician', 'registered nurse')) --video cde visit
        then last_value(enc.encounter_date) over (partition by enc.pat_key order by enc.encounter_date)
        end as last_edu_vis_dt,
    case when lower(enc.visit_type) in ('advanced pump class',
                                'ahm class',
                                'ahm t1y1 class',
                                'diabetes edu less than 30 mins',
                                'diabetes education',
                                'diabetes education t1y1',
                                'insulin start',
                                'saline start',
                                'pre technology',
                                'pump class',
                                'safety skills class',
                                'upgrade pump',
                                'cgms initiation',
                                'cgms interpretation')
            or (lower(enc.visit_type) in ('video visit diabetes') and lower(enc_type) = 'office visit'
                and lower(provider.prov_type) in ('dietician', 'registered nurse'))
        then row_number() over (partition by enc.pat_key order by enc.encounter_date desc) end as edu_rn,
        --current encounter information:
    initcap(provider.full_nm) as provider_nm, --current encounter
    enc.department_name as dept_nm, --current encounter
    provider.prov_type, --no primary provider_type attribute in enc block
    enc.visit_type as visit_type_nm,
    coalesce(stg_encounter_outpatient_raw.specialty_care_ind, 0) as specialty_care_ind,
    case when stg_encounter_telehealth.visit_key is not null then 1 else 0 end as telehealth_ind,
    enc.appointment_status as appt_stat,
    dense_rank() over (partition by enc.pat_key order by enc.encounter_date desc) as enc_rn
    --start from most recent one  
from
    {{ref('stg_encounter')}} as enc
    left join {{ ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
        on stg_encounter_outpatient_raw.visit_key = enc.visit_key
    inner join active_flowsheets
        on active_flowsheets.pat_key = enc.pat_key
    inner join cohort
        on cohort.visit_key = enc.visit_key
    inner join {{ source('cdw', 'provider')}} as provider
        on provider.prov_key = enc.prov_key
    inner join {{ source('cdw', 'epic_grouper_department')}} as epic_grouper_department
        on epic_grouper_department.dept_key = enc.dept_key
    inner join {{ source('cdw', 'epic_grouper_item')}} as dep_grouper
        on dep_grouper.epic_grouper_key = epic_grouper_department.epic_grouper_key
    left join {{ref('stg_encounter_telehealth')}} as stg_encounter_telehealth
        on stg_encounter_telehealth.visit_key = enc.visit_key

where
    lower(dep_grouper.epic_grouper_nm) = 'chop dep endocrinology'
--    icr flowsheets has launched since 2012, including all historical encounters
--    and enc.encounter_date >= '2011-01-01'
    and upper(enc.appointment_status) not in ('canceled', 'cancelled', 'no show', 'left without seen')
    and lower(enc.encounter_type) not in ('email correspondence',
                                'letter (out)',
                                'mychart encounter',
                                'scanning encounter',
                                'telephone',
                                'error',
                                'canceled',
                                'no show',
                                'wait list',
                                'bpa',
                                'orders only',
                                'patient outreach',
                                'refill',
                                'mobile')
