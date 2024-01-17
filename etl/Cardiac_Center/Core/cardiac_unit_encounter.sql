/*Description: encounter-level data for CICU and CCU encounters, sourced primarily from PC4 and PAC3 registries.
Output is one row per encounter.

Author: Rob Olsen

Last Edited: 1/15/2020

*/

/*this first CTE pulls CICU + CCU encounters from PC4. PC4 used to track both CICU + CCU encounter until
Jan 2019, at which point PC4 became exclusively CICU, and PAC3 became CCU*/

with pc4 as (
    select
        case when registry_pc4_encounter.card_unit_adm_ind = 1 then 'CCU' else 'CICU' end as department_name,
        'PC4' as registry,
        registry_pc4_encounter.pat_key,
        registry_pc4_encounter.r_enc_key as enc_key,
        registry_pc4_encounter.r_hsp_vst_key  as hsp_vst_key,
        registry_pc4_encounter.r_cicu_strt_dt as department_admit_date,
        registry_pc4_encounter.r_cicu_phys_end_dt as department_discharge_date,
        /*currently options for unscheduled + unplanned,
        scheduled + planned. this combines into just planned/unplanned*/
        case
            when lower(registry_pc4_encounter.sched_cicu_enc_desc) in ('unscheduled', 'unplanned')
                then 'UNPLANNED'
            when lower(registry_pc4_encounter.sched_cicu_enc_desc) in ('scheduled', 'planned')
                then 'PLANNED'
        end as planned_encounter,
        registry_pc4_encounter.cicu_adm_src as admit_source_facility,
        registry_pc4_encounter.cicu_adm_src_desc as admit_source_department,
        registry_pc4_encounter.enc_rsn_desc as encounter_reason,
        registry_pc4_encounter.cicu_disp as disposition_facility,
        registry_pc4_encounter.cicu_disp_desc as disposition_department
    from
        {{source('cdw', 'registry_pc4_encounter')}} as registry_pc4_encounter
    where
        registry_pc4_encounter.cur_rec_ind = 1
),
/*CCU encounters from PAC3.*/

pac3 as (
    select
        'CCU' as department_name,
        'PAC3' as registry,
        registry_pac3_encounter.pat_key,
        registry_pac3_encounter.r_enc_key as enc_key,
        registry_pac3_encounter.r_hsp_vst_key as hsp_vst_key,
        registry_pac3_encounter.r_ccu_strt_dt as department_admit_date,
        registry_pac3_encounter.r_ccu_end_dt as department_discharge_date,
        null as planned_encounter,
        /*PAC3 concatenates admission and disposition facility into one field -- the following code
        parses out facility and department*/
        case when registry_pac3_encounter.ccu_adm_src like '%-%'
            then trim(trailing from substr( --noqa: L028
                registry_pac3_encounter.ccu_adm_src, 1,
                instr(registry_pac3_encounter.ccu_adm_src, '-') - 1)
            )
            else registry_pac3_encounter.ccu_adm_src end as admit_source_facility,
        case when registry_pac3_encounter.ccu_adm_src like '%-%'
            then trim(leading from substr( --noqa: L028
                    registry_pac3_encounter.ccu_adm_src,
                    instr(registry_pac3_encounter.ccu_adm_src, '-') + 1))
        end as admit_source_department,
        registry_pac3_encounter.enc_rsn_desc as encounter_reason,
        case when registry_pac3_encounter.pac_dispo like '%-%'
            then trim(trailing from substr( --noqa: L028
                registry_pac3_encounter.pac_dispo, 1,
                instr(registry_pac3_encounter.pac_dispo, '-') - 1))
            else registry_pac3_encounter.pac_dispo
        end as disposition_facility,
        case when registry_pac3_encounter.pac_dispo like '%-%'
            then trim(leading from substr( --noqa: L028
                registry_pac3_encounter.pac_dispo,
                instr(registry_pac3_encounter.pac_dispo, '-') + 1))
        end as disposition_department
    from
        {{source('cdw', 'registry_pac3_encounter')}} as registry_pac3_encounter
    where
        registry_pac3_encounter.cur_rec_ind = 1
),

cardiac_encounter_all as (
    select
        department_name,
        registry,
        pat_key,
        enc_key,
        hsp_vst_key,
        department_admit_date,
        department_discharge_date,
        planned_encounter,
        admit_source_facility,
        admit_source_department,
        encounter_reason,
        disposition_facility,
        disposition_department
    from pc4
    union all
    select
        department_name,
        registry,
        pat_key,
        enc_key,
        hsp_vst_key,
        department_admit_date,
        department_discharge_date,
        planned_encounter,
        admit_source_facility,
        admit_source_department,
        encounter_reason,
        case when disposition_facility = 'CURRENT HOSP' then 'CURRENT HOSPITAL'
            else disposition_facility end as disposition_facility,
        disposition_department
    from pac3
),

    /*helper CTE for bounceback indicators. this flags CCU encounters where
        1) admission facility was current hospital,
        2) admission department was CICU, and
        3) either returned to CICU and that CICU encounter was unplanned, or returned to
        NICU or PICU regardless of planned/unplanned
        (registries don't track planned/unplanned status for other ICU's

    Time-specific indicators (e.g. 24, 48 hour bounceback) are created below.
    these will operate similarly to readmission rates in the sense that all 7 day
    readmissions are counted as 14 day readmissions, all 7 or 14 day readmissions
    are also counted as 30 day readmissions, etc.*/

bounceback_encounters as (
    select
        cardiac_encounter_all.registry,
        cardiac_encounter_all.enc_key,
        case
            when lower(cardiac_encounter_all.department_name) = 'ccu' -- currently in CCU
                and lower(cardiac_encounter_all.admit_source_facility) = 'current hospital' -- CHOP only
                and lower(cardiac_encounter_all.admit_source_department) = 'cicu' -- came from CICU
        /*either returned to CICU and that CICU encounter was unplanned, or returned to
        NICU or PICU regardless of planned/unplanned
        (registries don't track planned/unplanned status for other ICU's)*/
                and ((lower(cardiac_encounter_all.disposition_department) like '%cicu%'
                and lower(lead(cardiac_encounter_all.planned_encounter) over (
                    partition by cardiac_encounter_all.hsp_vst_key
                    order by cardiac_encounter_all.department_admit_date)
                    ) = 'unplanned')
                    or (lower(cardiac_encounter_all.disposition_department) like '%nicu%'
                        or (lower(cardiac_encounter_all.disposition_department) like '%picu%')
                    ))
            then 1
            else 0
        end as bounceback_ind
    from cardiac_encounter_all
)

select
    cardiac_encounter_all.enc_key,
    cardiac_encounter_all.registry,
    stg_patient.pat_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.sex,
    cardiac_encounter_all.hsp_vst_key,
    registry_hospital_visit.visit_key,
    registry_hospital_visit.r_admit_dt as hospital_admit_date,
    registry_hospital_visit.r_disch_dt as hospital_discharge_date,
    cardiac_encounter_all.department_name,
    cardiac_encounter_all.department_admit_date,
    cardiac_encounter_all.department_discharge_date,
    cardiac_encounter_all.planned_encounter,
    cardiac_encounter_all.encounter_reason,
    round((
        extract(epoch from cardiac_encounter_all.department_discharge_date
            - cardiac_encounter_all.department_admit_date) / 60.0 / 60.0), 2
    ) as department_los_hours,
    round((
        extract(epoch from cardiac_encounter_all.department_discharge_date
            - cardiac_encounter_all.department_admit_date) / 60.0 / 60.0 / 24.0), 2
    ) as department_los_days,
    cardiac_encounter_all.admit_source_facility,
    cardiac_encounter_all.admit_source_department,
    disposition_facility,
    cardiac_encounter_all.disposition_department,
    case
        when bounceback_encounters.bounceback_ind = 1
        and department_los_hours <= 24
        then 1
        else 0
    end as ccu_bounceback_24_ind,
    case
        when bounceback_encounters.bounceback_ind = 1
        and department_los_hours <= 48
        then 1
        else 0
    end as ccu_bounceback_48_ind,
    case
        when bounceback_encounters.bounceback_ind = 1
        and department_los_hours <= 72
        then 1
        else 0
    end as ccu_bounceback_72_ind,
    row_number() over (
        partition by cardiac_encounter_all.hsp_vst_key
        order by cardiac_encounter_all.department_admit_date
    ) as visit_seq
from
    cardiac_encounter_all
    inner join {{source('cdw', 'registry_hospital_visit')}} as registry_hospital_visit
        on cardiac_encounter_all.hsp_vst_key = registry_hospital_visit.r_hsp_vst_key
        and registry_hospital_visit.cur_rec_ind = 1
    inner join {{ref('stg_patient')}} as stg_patient
        on cardiac_encounter_all.pat_key = stg_patient.pat_key
    inner join bounceback_encounters
        on cardiac_encounter_all.enc_key = bounceback_encounters.enc_key
        and cardiac_encounter_all.registry = bounceback_encounters.registry
