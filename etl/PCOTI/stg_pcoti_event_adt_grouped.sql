with visit_admission_service as (
    select
        encounter_inpatient.pat_key,
        encounter_inpatient.visit_key,
        lower(encounter_inpatient.admission_service) as admission_service
    from
        {{ ref('encounter_inpatient') }} as encounter_inpatient
),

adt_grouped as (
    select
        stg_pcoti_adt_raw.pat_key,
        stg_pcoti_adt_raw.visit_key,
        visit_admission_service.admission_service,
        stg_pcoti_adt_raw.campus_name,
        stg_pcoti_adt_raw.department_group_name,
        max(stg_pcoti_adt_raw.bed_care_group) as bed_care_group,
        max(stg_pcoti_adt_raw.final_dept_key) as final_dept_key,
        max(stg_pcoti_adt_raw.final_department_name) as final_department_name,
        min(stg_pcoti_adt_raw.enter_date) as event_start_date,
        max(stg_pcoti_adt_raw.exit_date_or_current_date) as event_end_date
    from
        {{ ref('stg_pcoti_adt_raw') }} as stg_pcoti_adt_raw
        left join visit_admission_service
            on stg_pcoti_adt_raw.pat_key = visit_admission_service.pat_key
            and stg_pcoti_adt_raw.visit_key = visit_admission_service.visit_key
    group by
        stg_pcoti_adt_raw.pat_key,
        stg_pcoti_adt_raw.visit_key,
        stg_pcoti_adt_raw.bed_care_change_seq,
        visit_admission_service.admission_service,
        stg_pcoti_adt_raw.campus_name,
        stg_pcoti_adt_raw.department_group_name
)

select
    adt_grouped.pat_key,
    adt_grouped.visit_key,
    case
        ------------------------------------------------------------------------
        -- ED
        when adt_grouped.bed_care_group in ('ED', 'KOPH ED') then 'Location - ED'
        ------------------------------------------------------------------------
        -- FLOOR
        -- if location is ITCU and admitting service is gen peds, consider that
        -- a floor location; else, consider non-floor
        when
            adt_grouped.bed_care_group = 'PHL MED/SUR'
            and adt_grouped.department_group_name = 'ITCU'
            and adt_grouped.admission_service = 'general pediatrics'
            then 'Location - Floor'
        when
            adt_grouped.bed_care_group = 'PHL MED/SUR'
            and adt_grouped.department_group_name = 'ITCU'
            and adt_grouped.admission_service != 'general pediatrics'
            then 'Location - ITCU'
        when
            adt_grouped.bed_care_group = 'PHL MED/SUR'
            and adt_grouped.department_group_name like 'CCU%'
            then 'Location - CCU'
        when
            adt_grouped.bed_care_group = 'PHL MED/SUR'
            and adt_grouped.department_group_name = 'Rehab OVF'
            then 'Location - Rehab'
        -- fallback rule: all med/surg not otherwise specified
        when adt_grouped.bed_care_group in ('KOPH MED/SUR', 'PHL MED/SUR') then 'Location - Floor'
        ------------------------------------------------------------------------
        -- ICU
        when
            regexp_like(adt_grouped.bed_care_group, '^(?:PHL|KOPH) ICU')
            and regexp_like(adt_grouped.department_group_name, '^PICU|^NICU|^CICU')
            then 'Location - ICU ('
                || regexp_extract(adt_grouped.department_group_name, '^PICU|^NICU|^CICU')
                || ')'
        when
            adt_grouped.bed_care_group like 'PHL ICU%'
            and adt_grouped.department_group_name like 'PCU%'
            then 'Location - PCU'
        ------------------------------------------------------------------------
        -- OTHER
        when adt_grouped.bed_care_group = 'PERIOP' then 'Location - Periop'
        when adt_grouped.bed_care_group = 'PHL PED OBS' then 'Location - Observation'
        when adt_grouped.bed_care_group = 'PHL REHAB' then 'Location - Rehab'
        when adt_grouped.bed_care_group = 'PHL SDU' then 'Location - SDU'
        when
            adt_grouped.bed_care_group = 'PROCEDURAL'
            and adt_grouped.department_group_name = 'CPRU'
            then 'Location - CPRU'
        else 'Location - Other'
    end as event_type_name,
    case
        ------------------------------------------------------------------------
        -- ED
        when adt_grouped.bed_care_group in ('ED', 'KOPH ED') then 'LOC_ED'
        ------------------------------------------------------------------------
        -- FLOOR
        -- if location is ITCU and admitting service is gen peds, consider that
        -- a floor location; else, consider non-floor
        when
            adt_grouped.bed_care_group = 'PHL MED/SUR'
            and adt_grouped.department_group_name = 'ITCU'
            and adt_grouped.admission_service = 'general pediatrics'
            then 'LOC_FLOOR'
        when
            adt_grouped.bed_care_group = 'PHL MED/SUR'
            and adt_grouped.department_group_name = 'ITCU'
            and adt_grouped.admission_service != 'general pediatrics'
            then 'LOC_ITCU'
        when
            adt_grouped.bed_care_group = 'PHL MED/SUR'
            and adt_grouped.department_group_name like 'CCU%'
            then 'LOC_CCU'
        when
            adt_grouped.bed_care_group = 'PHL MED/SUR'
            and adt_grouped.department_group_name = 'Rehab OVF'
            then 'LOC_REHAB'
        -- fallback rule: all med/surg not otherwise specified
        when adt_grouped.bed_care_group in ('KOPH MED/SUR', 'PHL MED/SUR') then 'LOC_FLOOR'
        ------------------------------------------------------------------------
        -- ICU
        when
            regexp_like(adt_grouped.bed_care_group, '^(?:PHL|KOPH) ICU')
            and regexp_like(adt_grouped.department_group_name, '^PICU|^NICU|^CICU')
            then 'LOC_ICU_' || regexp_extract(adt_grouped.department_group_name, '^PICU|^NICU|^CICU')
        when
            adt_grouped.bed_care_group like 'PHL ICU%'
            and adt_grouped.department_group_name like 'PCU%'
            then 'LOC_PCU'
        ------------------------------------------------------------------------
        -- OTHER
        when adt_grouped.bed_care_group = 'PERIOP' then 'LOC_PERIOP'
        when adt_grouped.bed_care_group = 'PHL PED OBS' then 'LOC_OBS'
        when adt_grouped.bed_care_group = 'PHL REHAB' then 'LOC_REHAB'
        when adt_grouped.bed_care_group = 'PHL SDU' then 'LOC_SDU'
        when
            adt_grouped.bed_care_group = 'PROCEDURAL'
            and adt_grouped.department_group_name = 'CPRU'
            then 'LOC_CPRU'
        else 'LOC_OTHER'
    end as event_type_abbrev,
    adt_grouped.final_dept_key as dept_key,
    adt_grouped.final_department_name as department_name,
    adt_grouped.department_group_name,
    adt_grouped.bed_care_group,
    adt_grouped.campus_name,
    adt_grouped.event_start_date,
    adt_grouped.event_end_date
from
    adt_grouped
