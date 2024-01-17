select
    procedure_order_clinical.proc_ord_key,
    procedure_order_clinical.cpt_code,
    procedure_order_clinical.procedure_name,
    procedure_order_clinical.procedure_group_name,
    procedure_order_clinical.procedure_subgroup_name,
    procedure_order_clinical.procedure_order_type,
    procedure_order_clinical.orderset_name,
    procedure_order_clinical.order_status,
    procedure_order_clinical.department_name,
    procedure_order_clinical.ordering_provider_name,
    procedure_order_clinical.ordering_provider_key,
    procedure_order_clinical.mrn,
    procedure_order_clinical.patient_name,
    procedure_order_clinical.csn,
    procedure_order_clinical.encounter_date,
    procedure_order_clinical.placed_date,
    procedure_order_clinical.pat_key,
    procedure_order_clinical.visit_key,
    procedure_order_clinical.dept_key
from
    {{ref('procedure_order_clinical')}} as procedure_order_clinical
where
    ((
    (lower(procedure_order_clinical.procedure_name) like '%behav%'
        and procedure_order_clinical.procedure_id not in (431415, 80908))
     -- Excludes "BRIEF EMOTIONAL/BEHAVIORAL ASSESSMENT" and RESTRAINT CARE AND TREAT: NON-BEHAVIORAL MG
        or (lower(procedure_order_clinical.procedure_name) like '%psych%'
            and lower(procedure_order_clinical.procedure_name) not like '%antipsych%')
        or procedure_order_clinical.procedure_id in (
            78184,     -- CONSULT TO SOCIAL WORK (CHOP) 100201.001
            111575,    -- CONSULT TO SOCIAL WORK (NONCHOP) 100201
            80912,     -- VISUAL & ARMS LENGTH 
            80914,     -- VISUAL OBSERVATION 500RES7
            96662,     -- CARE OF THE SUICIDAL PATIENT PROCEDURE 500NUR1336
            96664,     -- SEARCHING PATIENTS AT RISK FOR SELF-HARM 500NUR1337
            97283)     -- IP SUICIDE TEACHING 500NUR1338
    and procedure_order_clinical.order_status in ('Completed', 'Sent', 'Resulted'))
    -- or RESTRAINTS BEHAVIOR MANAGEMENT PT < 9YRS or PT > 9 YRS
        or procedure_order_clinical.procedure_id in (80910, 80918))
    and procedure_order_clinical.encounter_date >= '2018-01-01'
order by
    procedure_order_clinical.encounter_date,
    procedure_order_clinical.placed_date,
    procedure_order_clinical.patient_name
