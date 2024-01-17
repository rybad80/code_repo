with patient_info as ( --noqa: PRS
    select
        stg_patient.pat_key,
        stg_patient.patient_name,
        stg_patient.mrn,
        stg_patient.dob,
        case when lower(stg_patient.sex) = 'm' then 1
            when lower(stg_patient.sex) = 'f' then 2
            when lower(stg_patient.sex) = 'u' then 9
            else 3
        end as sex, --3 stands for 'other'    
        case
            when stg_patient.current_age >= 3 then stg_patient.current_age  --years
            when stg_patient.current_age >= 0.08 and stg_patient.current_age < 3
                then stg_patient.current_age * 12 --months
            else stg_patient.current_age * 365 --days
        end as age,
        case
            when stg_patient.current_age >= 3 then 1 --years
            when stg_patient.current_age >= 0.08 and stg_patient.current_age < 3 then 2 --months
            else 3 --days
        end as ageunit,
        cast(stg_patient.mailing_address_line1 as varchar(3500)) as res_address1,
        cast(stg_patient.mailing_address_line2 as varchar(3500)) as res_address2,
        cast(stg_patient.mailing_city as varchar(50))            as res_city,
        cast(stg_patient.mailing_state as varchar(255))          as res_state,
        cast(stg_patient.mailing_zip as varchar(60))             as res_zip,
        cast(stg_patient.county as varchar(255))                 as res_county,
        cast(patient.home_ph as varchar(192))                    as res_phone,
        case
            when lower(stg_patient.race) = 'black or african american'
            then 1 else 0
        end as race_black,
        case
            when lower(stg_patient.race) = 'white'
            then 1 else 0
        end as race_white,
        case
            when lower(stg_patient.race) = 'native hawaiian or other pacific islander'
            then 1 else 0
        end as race_nhpi,
        case
            when lower(stg_patient.race) = 'asian'
            then 1 else 0
        end as race_asian,
        case
            when lower(stg_patient.race) = 'american indian or alaska native'
            then 1 else 0
        end as race_aian,
        case
            when lower(stg_patient.race) = 'other'
            then 1 else 0
        end as race_other,
        case
            when lower(stg_patient.race) not in ('black or african american',
                                            'white',
                                            'native hawaiian or other pacific islander',
                                            'asian',
                                            'american indian or alaska native',
                                            'other',
                                            'refused',
                                            null ) then 1 else 0
        end as race_spec,
        case
            when lower(stg_patient.race) in ('refused', null)
            then 1 else 0
        end as race_unk,
        case when lower(stg_patient.ethnicity) = 'hispanic or latino' then 1 else 0 end as ethnicity,
        stg_patient.deceased_ind,
        patient.death_dt,
        max(case when smoke.mrn is not null then 1 else 0 end) as smoking_ind,
        max(case when xsmoke.mrn is not null then 1 else 0 end) as former_smoking_ind,
        max(case when preg.mrn is not null then 1 else 0 end) as preg_ind
    from
        {{ref('stg_patient')}} as stg_patient
        inner join {{ref('stg_outbreak_flu_pui_cohort')}} as stg_outbreak_flu_pui_cohort
            on  stg_patient.pat_key = stg_outbreak_flu_pui_cohort.pat_key
        inner join {{source('cdw', 'patient')}} as patient
            on  stg_patient.pat_key = patient.pat_key
        left join {{ref('diagnosis_encounter_all')}} as smoke
            on stg_patient.pat_key = smoke.pat_key
            and lower(smoke.icd10_code) in ('f17.200', 'f17.210', 'z72.0')
            and smoke.encounter_date >= current_date - interval '3 months'
            and smoke.visit_diagnosis_ind = 1
        left join {{ref('diagnosis_encounter_all')}} as xsmoke
            on stg_patient.pat_key = xsmoke.pat_key
            and lower(xsmoke.icd10_code) in ('z87.891')
            and xsmoke.encounter_date >= current_date - interval '3 months'
            and xsmoke.visit_diagnosis_ind = 1
        left join {{ref('diagnosis_encounter_all')}} as preg
            on stg_patient.pat_key = preg.pat_key
            and lower(preg.icd10_code) in ('z34.90')
            and preg.encounter_date >= current_date - interval '1 month'
            and preg.visit_diagnosis_ind = 1
    group by
        stg_patient.pat_key,
        stg_patient.patient_name,
        stg_patient.mrn,
        stg_patient.dob,
        stg_patient.sex,
        stg_patient.current_age,
        stg_patient.mailing_address_line1,
        stg_patient.mailing_address_line2,
        stg_patient.mailing_city,
        stg_patient.mailing_state,
        stg_patient.mailing_zip,
        stg_patient.county,
        patient.home_ph,
        stg_patient.race,
        stg_patient.ethnicity,
        stg_patient.deceased_ind,
        patient.death_dt
),

symptoms as (
    select
        stg_outbreak_flu_pui_cohort.pat_key,
        stg_outbreak_flu_pui_cohort.test_type,
        min(encounter_date) as onset_dt
    from
        {{ref('stg_outbreak_flu_pui_cohort')}} as stg_outbreak_flu_pui_cohort
        inner join {{ref('diagnosis_encounter_all')}} as diagnosis_encounter_all
            on diagnosis_encounter_all.pat_key = stg_outbreak_flu_pui_cohort.pat_key
        inner join {{source('cdw', 'diagnosis')}} as dx
            on diagnosis_encounter_all.dx_key = dx.dx_key
    where
        (
            lower(dx.icd10_cd) in (
                'b34.9', --viral infection, unspecified
                'j05.0', --acute obstructive laryngitis [croup]
                'j06.9', --acute upper respiratory infection, unspecified
                'j21.9', --acute bronchiolitis, unspecified
                'r05', --cough
                'r50.9', --fever, unspecified
                'j45.21',  --mild intermittent asthma with (acute) exacerbation
                'j45.31', --mild persistent asthma with (acute) exacerbation
                'z11.59', -- encounter for screening for other viral diseases
                'j11.1' --  influenza due to unidentified influenza virus with other respiratory manifestations
            )
            or lower(dx.dx_nm) like '%flu%like%'
        )
        and diagnosis_encounter_all.visit_diagnosis_ind = 1
        and diagnosis_encounter_all.encounter_date
            between (min_specimen_taken_date - interval '30 days') --naqa:PRS
            and (min_specimen_taken_date + interval '30 days')   --naqa:PRS
    group by
        stg_outbreak_flu_pui_cohort.pat_key,
        stg_outbreak_flu_pui_cohort.test_type
),

inpatient as (
    select
        stg_outbreak_flu_pui_cohort.pat_key,
        encounter_inpatient.visit_key,
        encounter_inpatient.hospital_admit_date as adm1_dt,
        encounter_inpatient.primary_dx,
        encounter_inpatient.primary_dx_icd,
        encounter_inpatient.admission_department,
        encounter_inpatient.icu_ind as icu_yn,
        encounter_inpatient.hospital_discharge_date as dis1_dt,
        min_specimen_taken_date - hospital_admit_date as days_from_test,
        row_number() over (
            partition by encounter_inpatient.pat_key order by min_specimen_taken_date - hospital_admit_date
        ) as admit_number     -- pick most-recent admission relative to covid-19 test
    from
        {{ref('encounter_inpatient')}} as encounter_inpatient
            inner join {{ref('stg_outbreak_flu_pui_cohort')}} as stg_outbreak_flu_pui_cohort
            on encounter_inpatient.pat_key = stg_outbreak_flu_pui_cohort.pat_key
    where
        encounter_inpatient.hospital_admit_date
            between (min_specimen_taken_date - interval '30 days') --noqa: PRS
            and (min_specimen_taken_date + interval '30 days')   --noqa: PRS
)

select distinct
    patient_info.pat_key,
    stg_outbreak_flu_pui_cohort.test_type,
    patient_info.patient_name,
    patient_info.mrn,
    stg_outbreak_flu_pui_cohort.current_status,
    null as case_cdcreport_dt,
    case when stg_outbreak_flu_pui_cohort.current_status = 3
        then patient_info.res_address1 else null
    end as res_address1,
    case when stg_outbreak_flu_pui_cohort.current_status = 3
         then patient_info.res_address2 else null
    end as res_address2,
    case when stg_outbreak_flu_pui_cohort.current_status = 3
         then patient_info.res_city else null
    end as res_city,
    patient_info.res_state,
    case when stg_outbreak_flu_pui_cohort.current_status = 3
        then patient_info.res_zip else null
    end as res_zip,
    patient_info.res_county,
    case when stg_outbreak_flu_pui_cohort.current_status = 3
        then patient_info.res_phone else null
    end as res_phone,
    patient_info.ethnicity,
    patient_info.sex,
    patient_info.race_asian,
    patient_info.race_aian,
    patient_info.race_black,
    patient_info.race_nhpi,
    patient_info.race_white,
    patient_info.race_unk,
    patient_info.race_other,
    patient_info.race_spec,
    patient_info.dob,
    patient_info.age,
    patient_info.ageunit,
    date(stg_outbreak_flu_pui_cohort.min_specimen_taken_date) as collected_dt,
    case
        when stg_outbreak_flu_pui_cohort.current_status = 3
        then date(stg_outbreak_flu_pui_cohort.min_specimen_taken_date)
    end as pos_spec_dt,
    case
        when stg_outbreak_flu_pui_cohort.current_status = 3
        and stg_outbreak_flu_pui_cohort.min_specimen_taken_date is null
        then 1
    end as pos_spec_unk,
    null as pos_spec_na,
    case when stg_outbreak_pui_diagnosis.pneumonia_ind = 1 then 1 else 0 end as pna_yn,
    case when stg_outbreak_pui_diagnosis.ards_ind = 1 then 1 else 0 end as acuterespdistress_yn,
    null as diagother,
    case when stg_outbreak_pui_diagnosis.chronic_liver_yn = 1 then 1 else 0 end as chronic_liver_yn,
    case when abxchest_yn = 1 then 1 else 0 end as abxchest_yn,
    case when symptoms.pat_key is not null then 1 else 0 end as sympstatus,
    symptoms.onset_dt,
    case when symptoms.pat_key is not null and symptoms.onset_dt is null then 1 end as onset_unk,
    case when symptoms.pat_key is not null then 9 end as symp_res_dt,
    case when symptoms.pat_key is not null then 9 end as symp_res_yn,
    case when inpatient.pat_key is not null then 1 else 0 end as hosp_yn,
    inpatient.adm1_dt,
    inpatient.dis1_dt,
    inpatient.icu_yn,
    inpatient.primary_dx,
    inpatient.primary_dx_icd,
    inpatient.admission_department,
    case when inpatient.pat_key is null then null
 when patient_info.death_dt between inpatient.adm1_dt and inpatient.dis1_dt
        then 'Fatal'
 else 'Survived'
    end as inpatient_outcome,
    stg_outbreak_pui_flowsheet.mechvent_yn,
    stg_outbreak_pui_flowsheet.max_mv - stg_outbreak_pui_flowsheet.min_mv as mechvent_dur,
    stg_outbreak_pui_flowsheet.ecmo_yn,
    case
        when patient_info.deceased_ind = 1 then 1
        when patient_info.deceased_ind is null then 9
        else 0
    end as death_yn,
    patient_info.death_dt,
    case when patient_info.deceased_ind = 1 and patient_info.death_dt is null then 1 end as death_unk,
    0 as collect_ptinterview,
    1 as collect_medchart,
    case when stg_outbreak_pui_flowsheet.fever_yn = 1 then 1 else 0 end as fever_yn,
    case when stg_outbreak_pui_diagnosis.sfever_yn = 1
                or stg_outbreak_pui_flowsheet.sfever_yn = 1
                then 1 else 0
    end as sfever_yn,
    case when stg_outbreak_pui_diagnosis.chills_yn = 1
                or stg_outbreak_pui_flowsheet.chills_yn = 1
                then 1 else 0
    end as chills_yn,
    case when stg_outbreak_pui_diagnosis.myalgia_yn = 1
                or stg_outbreak_pui_flowsheet.myalgia_yn = 1
                then 1 else 0
    end as myalgia_yn,
    case when stg_outbreak_pui_diagnosis.runnose_yn = 1
                or stg_outbreak_pui_flowsheet.runnose_yn = 1
                then 1 else 0
    end as runnose_yn,
    case when stg_outbreak_pui_diagnosis.sthroat_yn = 1
                or stg_outbreak_pui_flowsheet.sthroat_yn = 1
                then 1 else 0
    end as sthroat_yn,
    case when stg_outbreak_pui_diagnosis.cough_yn = 1
                or stg_outbreak_pui_flowsheet.cough_yn = 1
                then 1 else 0
    end as cough_yn,
    case when stg_outbreak_pui_diagnosis.sob_yn = 1
                or stg_outbreak_pui_flowsheet.sob_yn = 1
                then 1 else 0
    end as sob_yn,
    case when stg_outbreak_pui_diagnosis.nauseavomit_yn = 1
                or stg_outbreak_pui_flowsheet.nauseavomit_yn = 1
                then 1 else 0
    end as nauseavomit_yn,
    case when stg_outbreak_pui_diagnosis.headache_yn = 1
                or stg_outbreak_pui_flowsheet.headache_yn = 1
                then 1 else 0
    end as headache_yn,
    case when stg_outbreak_pui_diagnosis.abdom_yn = 1
                or stg_outbreak_pui_flowsheet.abdom_yn = 1
                then 1 else 0
    end as abdom_yn,
    case when stg_outbreak_pui_diagnosis.diarrhea_yn = 1
                or stg_outbreak_pui_flowsheet.diarrhea_yn = 1
                then 1 else 0
    end as diarrhea_yn,
    case when stg_outbreak_pui_diagnosis.medcond_yn = 1 then 1 else 0 end as medcond_yn,
    case when stg_outbreak_pui_diagnosis.cld_yn = 1 then 1 else 0 end as cld_yn,
    case when stg_outbreak_pui_diagnosis.diabetes_yn = 1 then 1 else 0 end as diabetes_yn,
    case when stg_outbreak_pui_diagnosis.cvd_yn = 1 then 1 else 0 end as cvd_yn,
    case when stg_outbreak_pui_diagnosis.renaldis_yn = 1 then 1 else 0 end as renaldis_yn,
    case when stg_outbreak_pui_diagnosis.liverdis_yn = 1 then 1 else 0 end as liverdis_yn,
    case when stg_outbreak_pui_immunocompromised.reason is not null then 1 else 0 end as immsupp_yn,
    case when stg_outbreak_pui_diagnosis.neuro_yn = 1 then 1 else 0 end as neuro_yn,
    patient_info.preg_ind as pregnant_yn,
    patient_info.smoking_ind as smoke_curr_yn,
    patient_info.former_smoking_ind as smoke_former_yn,
    stg_outbreak_pui_resp_diag.resp_fluA_ag, -- noqa: L014
    stg_outbreak_pui_resp_diag.resp_fluB_ag, -- noqa: L014
    stg_outbreak_pui_resp_diag.resp_fluA_pcr, -- noqa: L014
    stg_outbreak_pui_resp_diag.resp_fluB_pcr, -- noqa: L014
    stg_outbreak_pui_resp_diag.resp_fluA_rapid_pcr, -- noqa: L014
    stg_outbreak_pui_resp_diag.resp_fluB_rapid_pcr, -- noqa: L014
    stg_outbreak_pui_resp_diag.resp_rsv,
    stg_outbreak_pui_resp_diag.resp_hm,
    stg_outbreak_pui_resp_diag.resp_pi,
    stg_outbreak_pui_resp_diag.resp_adv,
    stg_outbreak_pui_resp_diag.resp_rhino,
    stg_outbreak_pui_resp_diag.resp_cov,
    stg_outbreak_pui_resp_diag.resp_mp,
    stg_outbreak_pui_resp_diag.resp_rcp,
    stg_outbreak_pui_flowsheet.oth_resp_support,
    stg_outbreak_pui_resp_diag.covid_19
from
    patient_info
    inner join {{ref('stg_outbreak_flu_pui_cohort')}} as stg_outbreak_flu_pui_cohort
        on patient_info.pat_key = stg_outbreak_flu_pui_cohort.pat_key
    left join inpatient
        on stg_outbreak_flu_pui_cohort.pat_key = inpatient.pat_key
        and inpatient.admit_number = 1
    left join symptoms
        on stg_outbreak_flu_pui_cohort.pat_key = symptoms.pat_key
        and stg_outbreak_flu_pui_cohort.test_type = symptoms.test_type
    left join {{ref('stg_outbreak_pui_resp_diag')}} as stg_outbreak_pui_resp_diag
        on stg_outbreak_flu_pui_cohort.pat_key = stg_outbreak_pui_resp_diag.pat_key
        and stg_outbreak_pui_resp_diag.outbreak_type = stg_outbreak_flu_pui_cohort.test_type
    left join {{ref('stg_outbreak_pui_diagnosis')}} as stg_outbreak_pui_diagnosis
        on stg_outbreak_flu_pui_cohort.pat_key = stg_outbreak_pui_diagnosis.pat_key
        and stg_outbreak_pui_diagnosis.outbreak_type = stg_outbreak_flu_pui_cohort.test_type
    left join {{ref('stg_outbreak_pui_flowsheet')}} as stg_outbreak_pui_flowsheet
        on stg_outbreak_flu_pui_cohort.pat_key = stg_outbreak_pui_flowsheet.pat_key
        and stg_outbreak_pui_flowsheet.outbreak_type = stg_outbreak_flu_pui_cohort.test_type
    left join {{ref('stg_outbreak_pui_immunocompromised')}} as stg_outbreak_pui_immunocompromised
        on stg_outbreak_flu_pui_cohort.pat_key = stg_outbreak_pui_immunocompromised.pat_key
        and stg_outbreak_pui_immunocompromised.outbreak_type = stg_outbreak_flu_pui_cohort.test_type
