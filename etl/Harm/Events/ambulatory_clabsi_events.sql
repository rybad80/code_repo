with
infection_class as (

select
    infection_surveillance_class.inf_surv_key,
    max(case when infection_surveillance_class.inf_surv_cls_nm = 'Central-line associated' then 1
        else 0 end) as cla_class_ind,
    max(case when infection_surveillance_class.inf_surv_cls_nm in
        ('BSI (bloodstream infection)', 'Bloodstream Infection') then 1
        else 0 end) as bsi_class_ind,
    max(case when infection_surveillance_class.inf_surv_cls_nm like '%MBI-LCBI%' then 1
        else 0 end) as mbi_ind,
    max(
        case
            when infection_surveillance_class.inf_surv_cls_ansr like '%perm_broviac%' then 'Broviac   '
            when infection_surveillance_class.inf_surv_cls_ansr like '%perm_hickman%' then 'Hickman   '
            when infection_surveillance_class.inf_surv_cls_ansr like '%perm_port%' then 'Port-a-cath   '
            when infection_surveillance_class.inf_surv_cls_ansr like '%perm_tunneled%' then 'Tunneled   '
            when infection_surveillance_class.inf_surv_cls_ansr like '%perm_other%' then 'Permanent-Other   '
            else '' end
            )
    || max(
        case
            when infection_surveillance_class.inf_surv_cls_ansr like '%temp_femoral%' then 'Femoral   '
            when infection_surveillance_class.inf_surv_cls_ansr like '%temp_peripheral%'
                then 'Peripherally Inserted(PICC)   '
            when infection_surveillance_class.inf_surv_cls_ansr like '%temp_other%'
                then 'Temporary-Other   '
            else '' end
        )
    || max(
        case
            when infection_surveillance_class.inf_surv_cls_ansr like '%umb_arterial%' then 'Arterial   '
            when infection_surveillance_class.inf_surv_cls_ansr like '%umb_venous%' then 'Venous   '
            when infection_surveillance_class.inf_surv_cls_ansr like '%umb_other%' then 'Umbilical-Other   '
        else '' end
        )
    || max(
        case
            when upper(infection_surveillance_class.inf_surv_cls_nm) like '%PERMANENT%IMPLANTED%'
                then 'Permanent   '
            when upper(infection_surveillance_class.inf_surv_cls_nm) like '%TEMPORARY%INDWELLING%'
                then 'Temporary   '
            when upper(infection_surveillance_class.inf_surv_cls_nm) like '%UMBILICAL%' then 'Umbilical   '
            else 'Not Indicated' end
        )
    as line_type
from
    {{source('cdw', 'infection_surveillance_class')}} as infection_surveillance_class
group by
    infection_surveillance_class.inf_surv_key
),

bugsy_lda as (

select
    (assocd_lda.registry_data_id + 300000) as inf_surv_id,
    assocd_lda.assocd_lda_id,
    max(flowsheet_lda.lda_types) as lda_types
from
    {{source('clarity_ods', 'assocd_lda')}} as assocd_lda
    left join {{ref('flowsheet_lda')}} as flowsheet_lda on flowsheet_lda.ip_lda_id = assocd_lda.assocd_lda_id
where
    assocd_lda.line = 1
group by
    assocd_lda.registry_data_id,
    assocd_lda.assocd_lda_id
),

theradoc_visit as (

select
    infection_surveillance_visit.inf_surv_key,
    stg_encounter.visit_key,
    stg_department_all.dept_key as theradoc_dept_key,
    stg_department_all.department_name as theradoc_dept_nm,
    stg_department_all.specialty_name as theradoc_specialty
from
    {{source('cdw', 'infection_surveillance_visit')}} as infection_surveillance_visit
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = infection_surveillance_visit.visit_key
    inner join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.dept_key = stg_encounter.dept_key
where
    infection_surveillance_visit.visit_rank = 1
group by
    infection_surveillance_visit.inf_surv_key,
    stg_encounter.visit_key,
    stg_department_all.dept_key,
    stg_department_all.department_name,
    stg_department_all.specialty_name
),

bugsy_assoc_line as (

select
    bugsy_custom_infection_classes.c54_td_ica_surv_id as inf_surv_id,
    1 as line_associated_ind
from
    {{source('cdw', 'bugsy_custom_infection_classes')}} as bugsy_custom_infection_classes
where
    (bugsy_custom_infection_classes.centralline = 'Y'
    or bugsy_custom_infection_classes.permcentralline = 'Y'
    or bugsy_custom_infection_classes.tempcentralline = 'Y'
    or bugsy_custom_infection_classes.lda_associated_yn = 'Y')
group by
    bugsy_custom_infection_classes.c54_td_ica_surv_id
),

organism as (

select distinct
    infection_surveillance_micro.inf_surv_key,
    coalesce(
    regexp_extract(infection_surveillance_micro.organism_nm,
        '(.*)(?=Ammended susceptibility|if Cefazolin|This organism|Minimum inhibitory|If reported|Infectious Disease|Infectious disease|The preferred|There is insufficient)' --noqa: L016
        ), infection_surveillance_micro.organism_nm)
        as organism_name_shortened
from
    {{source('cdw', 'infection_surveillance_micro')}} as infection_surveillance_micro
where
    infection_surveillance_micro.organism_nm not in ( -- Not meaningful
         'Blood culture - Blood culture: Blood Culture received by Lab.',
         'Blood culture - Blood culture: No growth to date.',
         'Positive blood culture '
        )
),

organism_distinct as (

select
    inf_surv_key,
    group_concat(organism_name_shortened) as organism
from
    organism
group by
    inf_surv_key
),

abstration_q as (

select
    (cur_abst_quesr_answers.registry_data_id + 300000) as inf_surv_id,
    max(case when cur_abst_quesr_answers.question_id = '142390'
        then cast(cur_abst_quesr_answers.formatted_answer as varchar(20)) end) as gi_subcohort,
    max(case when cur_abst_quesr_answers.question_id = '147389'
        and cur_abst_quesr_answers.formatted_answer = 'Yes' then 1 else 0 end) as mbi_ind
from
    {{source('clarity_ods', 'cur_abst_quesr_answers')}} as cur_abst_quesr_answers
where
    cur_abst_quesr_answers.question_id  in (
         '142390', --GI sub-division
         '147389') --MBI
group by
    cur_abst_quesr_answers.registry_data_id
)

select
    infection_surveillance.inf_surv_id,
    stg_patient.mrn,
    stg_patient.patient_name,
    infection_surveillance.inf_dt as infection_date,
    infection_surveillance.conf_dt as confirmation_date,
    case when
        stg_department_all.department_name = 'UNKNOWN' then theradoc_visit.theradoc_dept_nm
        else stg_department_all.department_name
        end as department,
    case -- Fix BH Depts
        when department = 'BGR BH ONCOLOGY' then 'ONCOLOGY'
        when department = 'BGR BH GASTROENTERLGY' then 'GASTROENTEROLOGY'
        when department = 'BGR BH METABOLISM' then 'METABOLISM'
        when stg_department_all.specialty_name = 'UNKNOWN' then theradoc_visit.theradoc_specialty
        else stg_department_all.specialty_name end as specialty,
    case
        when department like '%DIALYSIS%' then 'DIALYSIS'
        when specialty in ('GI/NUTRITION', 'GASTROENTEROLOGY', 'CLINICAL NUTRITION')
            and abstration_q.gi_subcohort in ('Non-IRP', 'IRP') then upper(abstration_q.gi_subcohort)
        when specialty in ('GI/NUTRITION', 'GASTROENTEROLOGY', 'CLINICAL NUTRITION')
            and coalesce(abstration_q.gi_subcohort, 'N/A') in ('N/A') then 'GI'
        when specialty in ('HEMATOLOGY ONCOLOGY', 'RADIATION ONCOLOGY') then 'ONCOLOGY'
        else coalesce(specialty, 'UNKNOWN')
        end as display_specialty,
    infection_surveillance.inf_acq_type as infection_acquired_type,
    infection_surveillance.pres_on_admit_ind as present_on_admit_ind,
    coalesce(abstration_q.mbi_ind, infection_class.mbi_ind, 0) as mbi_lcbi_ind,
    organism_distinct.organism,
    coalesce(bugsy_lda.lda_types, infection_class.line_type) as line_type,
    infection_surveillance.work_status,
    infection_surveillance.create_by,
    infection_surveillance.pat_key,
    infection_surveillance.inf_surv_key,
    case when infection_surveillance.dept_key = 0 then theradoc_visit.theradoc_dept_key
        else infection_surveillance.dept_key
        end as dept_key
from
    {{source('cdw', 'infection_surveillance')}} as infection_surveillance
    inner join {{ref('stg_patient')}} as stg_patient on stg_patient.pat_key = infection_surveillance.pat_key
    left join infection_class on infection_class.inf_surv_key = infection_surveillance.inf_surv_key
    left join bugsy_assoc_line on bugsy_assoc_line.inf_surv_id = infection_surveillance.inf_surv_id
    left join {{ref('stg_department_all')}} as stg_department_all
        on stg_department_all.dept_key = infection_surveillance.dept_key
    left join theradoc_visit on theradoc_visit.inf_surv_key = infection_surveillance.inf_surv_key
    left join bugsy_lda on bugsy_lda.inf_surv_id = infection_surveillance.inf_surv_id
    left join organism_distinct on organism_distinct.inf_surv_key = infection_surveillance.inf_surv_key
    left join abstration_q on abstration_q.inf_surv_id = infection_surveillance.inf_surv_id
where
    (
        (infection_surveillance.create_by = 'THERADOC'
        and infection_surveillance.pres_on_admit_ind = 1
        and infection_surveillance.inf_acq_type = 'HEALTHCARE-ASSOCIATED'
        and infection_class.bsi_class_ind = 1
        and infection_class.cla_class_ind = 1
        )
    or (infection_surveillance.create_by = 'BUGSY'
        and infection_surveillance.inf_acq_type = 'COMMUNITY-ACQUIRED' -- Class = Not HAI
        and infection_class.bsi_class_ind = 1
        and bugsy_assoc_line.line_associated_ind = 1
        )
    )
    and date(infection_surveillance.inf_dt) >= '2018-07-01'
    and infection_surveillance.work_status in
        ('COMPLETE', 'PENDING') --Pending from old Theradoc cases
