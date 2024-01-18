select
    'INTUSSUSCEPTION' as cohort,
    null as subcohort,
    ed.visit_key,
    ed.pat_key

from
    {{ ref('stg_encounter_ed') }} as ed

    inner join
        {{ ref('diagnosis_encounter_all') }} as dx
        on ed.visit_key = dx.visit_key

    inner join
        {{ source('cdw', 'procedure_order') }} as us_proc_ord -- ultrasound
        on ed.visit_key = us_proc_ord.visit_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as us_proc_ord_stat
        on us_proc_ord.dict_ord_stat_key = us_proc_ord_stat.dict_key
    inner join
        {{ source('cdw', 'procedure') }} as us_proc
        on us_proc_ord.proc_key = us_proc.proc_key

    inner join
        {{ source('cdw', 'procedure_order') }} as fl_proc_ord -- fluoro enema
        on ed.visit_key = fl_proc_ord.visit_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as fl_proc_ord_stat
        on fl_proc_ord.dict_ord_stat_key = fl_proc_ord_stat.dict_key
    inner join
        {{ source('cdw', 'procedure') }} as fl_proc
        on fl_proc_ord.proc_key = fl_proc.proc_key

where
    year(ed.encounter_date) >= year(current_date) - 5

    and dx.icd10_code = 'K56.1'

    and fl_proc.cpt_cd = '74283'
    and fl_proc_ord_stat.src_id != 4 -- ignore cancelled orders

    and us_proc.proc_id in (
        85641,   -- US ABD FOR INTUSSUSCEPTION
        85637,   -- US ABD FOR BOWEL
        119825,  -- US ABDOMEN COMPLETE OUTSIDE EXAM SECOND READ
        119827,  -- US ABDOMEN LIMITED BOWEL OUTSIDE EXAM SECOND READ
        119859   -- US ABDOMEN SINGLE ORGAN OUTSIDE EXAM SECOND READ
    )
    and fl_proc_ord_stat.src_id != 4 -- ignore cancelled orders

group by ed.visit_key, ed.pat_key
