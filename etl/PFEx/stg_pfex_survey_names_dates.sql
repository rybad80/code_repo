select
    stg_pfex_deduplicate.survey_key,
    case
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'as0101',
            'as0101e')
            then 'Day Surgery'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'as0102',
            'as0102e')
            then 'Day Surgery - CPRU'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            /*inpatient pediatric (pre-10/1/19)*/
            'ch0101u',
            'ch0101ue',
            'ch0102ue',
            /*inpatient pediatric (post-10/1/19)*/
            'pd0101',
            'pd0101e',
            'pd0102e')
            then 'Inpatient Pediatric'
        /*ch0102ue/pd0102e = 'inpatient pediatric - GPS'*/
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'iz0101u',
            'iz0101ue')
            /*'SDU inpatient only'*/
            then 'SDU'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'iz0102u',
            'iz0102ue')
            /*'SDU inpatient and labor delivery'*/
            then 'SDU'
when lower(stg_pfex_deduplicate.survey_line_id) in (
            'md0101',
            'md0101e',
            'md0103e',
            'mt0101ce')
            and lower(stg_encounter.intended_use_name) = 'primary care'
            /*All primary care depts and 3550 and PRIMARY CARE SOUDERTON*/
            /*md0103e =GPS Medical Practice, mt0101ce=telehealth*/
            then 'Primary Care'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'md0101',
            'md0101e',
            'md0103e',
            'mt0101ce')
            and stg_encounter.department_id != '101012066' -- KARABOTS CHILD DEV
            and lower(stg_encounter.specialty_name) not in(
                'speech',
                'physical therapy',
                'occupational therapy',
                'behavioral health services',
                'urgent care')
            then 'Specialty Care'
        /* md0101/md0101e/md0103e depts that are *not* primary
        care depts or 3550, mt0101ce=telehealth removing 4
        specialties due to raw file error in 4/2020 that send
        outpatient service depts the mt0101ce survey */
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'md0102',
            'md0102e',
            'mt0102ce')
            then 'Adult Specialty Care'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'nc0101',
            'nc0101e')
            then 'NICU'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'on0101',
            'on0101e')
            then 'Outpatient Oncology'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'ou0101',
            'ou0101e',
            'ov0101',
            'ov0101e')
            then 'Outpatient Services'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'oy0101',
            'oy0101e',
            'bt0101',
            'bt0101e')
            then 'Outpatient Behavioral Health'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'pe0101',
            'pe0101e')
            then 'Pediatric ED'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'rh0101',
            'rh0101e')
            then 'Inpatient Rehabilitation'
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'uc0101',
            'uc0101e',
            'ut0101e')
            then 'Urgent Care'
        when lower(stg_pfex_deduplicate.survey_line_id) = 'hh0101e'
            then 'Home Care'
        /*Sedation Survey Line added 03-11-2022*/
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'as0103',
            'as0103e')
            and stg_encounter.department_id != '101003049'
            then 'Sedation'
        else lower(stg_pfex_deduplicate.survey_line_id)
    end as survey_line_name,
    case
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'as0101', 'as0101e',   /*day surgery*/
            'as0102', 'as0102e',   /*day surgery - CPRU*/
            'ch0101u', 'ch0101ue', /*inpatient pediatric (pre-10/1/19)*/
            'ch0102ue',            /*inpatient pediatric - GPS (pre-10/1/19)*/
            'pd0101', 'pd0101e',   /*inpatient pediatric (pre-10/1/19)*/
            'pd0102e',             /*inpatient pediatric - GPS (post-10/1/19)*/
            'iz0101u', 'iz0101ue', /*SDU inpatient only*/
            'iz0102u', 'iz0102ue', /*SDU inpatient and labor delivery*/
            'nc0101', 'nc0101e',   /*NICU*/
            'pe0101', 'pe0101e',   /*pediatric ED*/
            'uc0101', 'uc0101e',   /*urgent care*/
            'as0103', 'as0103e')   /*Sedation added 03-14-2022*/
            then stg_encounter.hospital_discharge_date
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            /*medical practice, mt=telehealth*/
            'md0101', 'md0101e',
            'mt0101ce',
            /*adult specialty care, mt=telehealth*/
            'md0102', 'md0102e',
            'mt0102ce',
            'md0103e', /*medical pracice - GPS*/
            'on0101', 'on0101e', /*outpatient oncology*/
            /*outpatient services, ov=telehealth*/
            'ou0101', 'ou0101e',
            'ov0101', 'ov0101e',
            /*outpatient behavioral health, bt = telehealth*/
            'oy0101', 'oy0101e',
            'bt0101', 'bt0101e',
            /*ut=telehealth urgent care*/
            /*ut is separated from uc b/c dsch_dt not filled in visit table*/
            'ut0101e')
            then stg_encounter.appointment_date
        when lower(stg_pfex_deduplicate.survey_line_id) in (
            'rh0101',
            'rh0101e',
            'hh0101',
            'hh0101e')  /*inpatient rehabilitation and home care*/
            then coalesce(stg_encounter.hospital_discharge_date, stg_encounter.appointment_date)
        else null
    end as visit_date
from
    {{ref('stg_pfex_deduplicate')}} as stg_pfex_deduplicate
-- left join {{source('cdw', 'department')}} as department
--     on stg_pfex_deduplicate.dept_key = department.dept_key
left join {{ref('stg_encounter')}} as stg_encounter
    on stg_pfex_deduplicate.visit_key = stg_encounter.visit_key
-- left join {{source('cdw', 'visit')}} as visit
--     on visit.visit_key = stg_encounter.visit_key
