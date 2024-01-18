with viral_codes as (
    --ICD10 viral categories
    select
        diagnosis.dx_key,
        1 as viral_dx
    from
        {{source('cdw','diagnosis')}} as diagnosis
        inner join {{ref('lookup_diagnosis_icd10')}} as lookup_diagnosis_icd10
            on diagnosis.icd10_cd = lookup_diagnosis_icd10.icd10_code
    where
        lookup_diagnosis_icd10.category in ('B34', 'J06', 'J21', 'A08', 'R50', 'J02',
                                            'R11', 'J05', 'J11', 'H10', 'B08',
                                            'B09', 'B30', 'K12', 'J12', 'A87', 'A98')
),

cohort as (
    select
        visit_key
    from
        {{ref('encounter_inpatient')}}
    where
        coalesce(hospital_discharge_date, current_date) >= '2014-01-01'
),

viral_diagnosis as (
    --diagnosed with viral infection at admission or anytime during stay
    select
        cohort.visit_key,
        max(case when viral_codes.viral_dx = 1 then 1 else 0 end) as viral_encounter_ind,
        max(
            case when viral_codes.viral_dx = 1
            and visit_diagnosis.dict_dx_sts_key not in (321, 322) then 1 else 0 end
        ) as viral_admission_ind
    from
        cohort
        left join {{source('cdw','visit_diagnosis')}} as visit_diagnosis
            on cohort.visit_key = visit_diagnosis.visit_key
        left join viral_codes
            on viral_codes.dx_key = visit_diagnosis.dx_key
    group by
        cohort.visit_key
),

or_raw as (
    select
        surgery_encounter.visit_key,
        surgery_encounter.or_key,
        surgery_encounter.log_id,
        surgery_encounter.case_key,
        surgery_encounter.surgery_date,
        surgery_encounter.service as surgery_service,
        surgery_encounter.location_group,
        surgery_encounter_timestamps.in_room_date,
        rank() over (partition by surgery_encounter.visit_key
                        order by surgery_encounter.surgery_date,
                        case
                            when lower(surgery_encounter.service) = 'anesthesia'
                            then 1
                            else 0
                        end, or_key) as ord
    from
        cohort
        inner join {{ref('surgery_encounter')}} as surgery_encounter
            on cohort.visit_key = surgery_encounter.visit_key
        inner join {{ref('surgery_encounter_timestamps')}} as surgery_encounter_timestamps
            on surgery_encounter_timestamps.or_key = surgery_encounter.or_key
),

or_first_booked as (
select
    or_raw.or_key,
    min(or_case_audit_history.audit_act_dt) as first_booked_date
    from
        or_raw
        inner join {{source('cdw','or_case_audit_history')}} as or_case_audit_history
            on or_case_audit_history.or_case_key = or_raw.case_key
        inner join {{source('cdw','dim_or_audit_action')}} as dim_or_audit_action
            on dim_or_audit_action.dim_or_audit_act_key = or_case_audit_history.dim_or_audit_act_key
    where
        lower(dim_or_audit_action.or_audit_act_nm) = 'scheduled'
    group by
        or_raw.or_key
),

surgery as (
      select
        or_raw.visit_key,
        or_raw.or_key,
        or_first_booked.first_booked_date,
        in_room_date,
        surgery_date,
        surgery_service,
        or_raw.location_group,
        case  -- any surg during entire IP stay
            when or_raw.in_room_date is not null
            then 1
            else 0
        end as surgery_encounter_ind,
        case  -- SCHEDULED surgery within 24hrs of operation
            when (extract(epoch from in_room_date - first_booked_date)) / 3600 <= 24
            then 1
            else 0
        end as emergent_surgery_ind,
        rank() over (partition by or_raw.visit_key
                      order by surgery_date, case
                                              when lower(surgery_service) = 'anesthesia'
                                            then 1
                                            else 0 end, or_key) as ord
      from
        or_raw
          left join or_first_booked
            on or_first_booked.or_key = or_raw.or_key
),

ed_dx_grp as (--region major ED diagnosis grouper
    select distinct
        diagnosis.dx_key,
        coalesce(lookup_ed_icd10_groupers.major_group_desc, 'Not Categorized') as ed_dx_group,
        coalesce(lookup_ed_icd10_groupers.subgroup_desc, 'Not Categorized') as ed_dx_subgroup
    from
        {{ source('cdw', 'diagnosis') }} as diagnosis
        left join {{ ref('lookup_ed_icd10_groupers') }} as lookup_ed_icd10_groupers
            on lookup_ed_icd10_groupers.icd10_code = regexp_replace(diagnosis.icd10_cd, '\.', '')
    where
        diagnosis.icd10_ind = 1
        and diagnosis.seq_num = 1
        and lower(diagnosis.dx_stat) != 'deleted'

),

ed_stg as (--ED stage
    select distinct
        cohort.visit_key,
        visit_diagnosis.seq_num,
        visit_diagnosis.dx_key,
        coalesce(ed_dx_group, 'Not Categorized') as ed_dx_group,
        coalesce(ed_dx_subgroup, 'Not Categorized') as ed_dx_subgroup,
        rank() over (partition by cohort.visit_key order by
                        case
                          when  ed_dx_group is null
                          then 4
                          when   lower(ed_dx_group) = 'not categorized'
                          then 3
                          when   lower(ed_dx_group) = 'other'
                          then 2
                          else 1
                        end,
                        case
                           when  (dict_dx_sts_key = 313
                                       or (dict_dx_sts_key in (314, 317, 319)
                                        and visit_diagnosis.seq_num = 1)
                                    )
                           then dict_dx_sts_key else 100000
                        end,
                        visit_diagnosis.seq_num,
                        dx_key) as ord
    from  cohort
        inner join {{ref('stg_encounter_ed')}} as stg_encounter_ed
            on stg_encounter_ed.visit_key = cohort.visit_key
        left join {{source('cdw','visit_diagnosis')}} as visit_diagnosis
            on visit_diagnosis.visit_key = stg_encounter_ed.visit_key
            and (visit_diagnosis.dict_dx_sts_key = 313
                    or (visit_diagnosis.dict_dx_sts_key in (314, 317, 319)
                        and visit_diagnosis.seq_num = 1)
                )
        left join ed_dx_grp
            on ed_dx_grp.dx_key = visit_diagnosis.dx_key
    where
        stg_encounter_ed.ed_patients_seen_ind = 1

),

ed as (--ED patients one row
    select
        ed_stg.visit_key,
        ed_stg.ed_dx_group,
        ed_stg.ed_dx_subgroup
    from
        ed_stg
    where
        ed_stg.ord = 1
)

select
    encounter_inpatient.pat_key,
    encounter_inpatient.visit_key,
    stg_patient.dob,
    case
        when (surgery.surgery_encounter_ind = 0 or surgery.surgery_encounter_ind is null)
            and lower(encounter_inpatient.admission_type) = 'elective'
        then 1
        when surgery.surgery_encounter_ind = 1
            and surgery.emergent_surgery_ind = 0
        then 1
        else 0
    end as elective_ind,
    encounter_inpatient.admission_source,
    encounter_inpatient.inpatient_admit_date,
    encounter_inpatient.admission_department,
    encounter_inpatient.admission_department_center_abbr,
    encounter_inpatient.admission_service,
    case
        when encounter_inpatient.admission_service in
                                    ('General Surgery', 'Neurosurgery', 'Oral and Maxillofacial Surgery',
                                      'Orthopedics', 'Otolaryngology', 'Plastic Surgery', 'Trauma',
                                      'Trauma PICU', 'Trauma Surgery', 'Urology') then 1
        else 0
    end as surgical_admission_service_ind,
    encounter_inpatient.ed_ind,
    encounter_inpatient.hospital_admit_date,
    encounter_inpatient.hospital_discharge_date,
    encounter_inpatient.age_years,
    encounter_inpatient.age_days,
    stg_patient.ethnicity,
    case
        when stg_encounter_chop_market.chop_market = 'international'
            then 'International'
        else stg_patient.mailing_state
      end as mailing_state,
    case
        when stg_encounter_chop_market.chop_market = 'international'
            then 'International'
        else stg_patient.county
      end as county,
    substr(stg_patient.mailing_zip, 1, 5) as mailing_zip,
    case
        when lower(stg_encounter_chop_market.chop_market) in ('other', 'unknown') then 'National'
        else stg_encounter_chop_market.chop_market
      end as chop_market,
    case
        when stg_encounter_chop_market.chop_market = 'international'
        then 1 else 0
    end as international_ind,
    ed.ed_dx_group,
    ed.ed_dx_subgroup,
    coalesce(surgery.surgery_encounter_ind, 0) as surgery_encounter_ind,
    coalesce(surgery.emergent_surgery_ind, 0) as emergent_surgery_ind,
    case
        when (extract(epoch from surgery.in_room_date - encounter_inpatient.hospital_admit_date)) / 3600 <= 24
        then 1
        else 0
    end as surgery_admission_ind, -- surgery within 24hrs after ip admission
    case
        when surgery_admission_ind = 1
        then surgery.location_group
    end as surgery_admission_location,
    viral_diagnosis.viral_encounter_ind,
    viral_diagnosis.viral_admission_ind

from cohort
    inner join {{ref('encounter_inpatient')}} as encounter_inpatient
        on encounter_inpatient.visit_key = cohort.visit_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = encounter_inpatient.pat_key
    left join ed
        on ed.visit_key = cohort.visit_key
    left join surgery
        on surgery.visit_key = cohort.visit_key
        and surgery.ord = 1
    left join viral_diagnosis
        on viral_diagnosis.visit_key = cohort.visit_key
    left join {{ref('stg_encounter_chop_market')}} as stg_encounter_chop_market
        on stg_encounter_chop_market.visit_key = cohort.visit_key
