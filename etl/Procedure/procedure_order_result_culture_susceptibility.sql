with procedure_order_result_culture as (
    select
        procedure_order_result_clinical.procedure_order_result_key,
        procedure_order_result_clinical.proc_ord_key,
        procedure_order_result_clinical.patient_name,
        procedure_order_result_clinical.mrn,
        procedure_order_result_clinical.dob,
        procedure_order_result_clinical.csn,
        procedure_order_result_clinical.encounter_date,
        procedure_order_result_clinical.procedure_order_id,
        procedure_order_result_clinical.order_specimen_source,
        procedure_order_result_clinical.specimen_taken_date,
        procedure_order_result_clinical.procedure_name,
        procedure_order_result_clinical.result_seq_num,
        procedure_order_result_clinical.result_date,
        procedure_order_result_clinical.result_component_name,
        procedure_order_result_clinical.result_component_id,
        procedure_order_result_clinical.result_value,
        procedure_order_result_clinical.result_status,
        procedure_order_result_clinical.result_lab_status,
        case
            when
                procedure_order_result_clinical.result_value is null
                or lower(procedure_order_result_clinical.result_value) like '%no growth%'
                or lower(procedure_order_result_clinical.result_value) like '%test cancelled%'
                or lower(procedure_order_result_clinical.result_value) like '%culture received%lab%'
                or (lower(procedure_order_result_clinical.result_value) like '%normal%flora%'
                    and lower(procedure_order_result_clinical.result_value) not like '%except%'
                )
                or lower(procedure_order_result_clinical.result_value) like '%no %isolated%'
                or lower(procedure_order_result_clinical.result_value) like 'negative%'
            then 0 else 1
        end as culture_growth_ind,
        case
            when lower(procedure_order_result_clinical.result_component_name) like '%blood%' then 'blood'
            when lower(procedure_order_result_clinical.result_component_name) like '%urine%' then 'urine'
            when lower(procedure_order_result_clinical.result_component_name) like '%resp%' then 'respiratory'
            when lower(procedure_order_result_clinical.result_component_name) like '%abdominal%' then 'abdominal'
            when lower(procedure_order_result_clinical.result_component_name) like '%wound%' then 'wound'
            when lower(procedure_order_result_clinical.result_component_name) like '%skin%' then 'skin'
            when lower(procedure_order_result_clinical.result_component_name) like '%body%'
             and lower(procedure_order_result_clinical.order_specimen_source) like '%pleural%' then 'pleural'
            when lower(procedure_order_result_clinical.result_component_name) like '%body%'
             and lower(procedure_order_result_clinical.order_specimen_source) like '%peritoneal%' then 'peritoneal'
            when lower(procedure_order_result_clinical.order_specimen_source) like '%cerebrospinal%'
             or lower(procedure_order_result_clinical.order_specimen_source) like 'csf%' then 'csf'
            else 'other'
        end as culture_type,
        procedure_order_result_clinical.visit_key,
        procedure_order_result_clinical.pat_key
    from
        {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
    where
        lower(procedure_order_result_clinical.result_component_name) like '%culture%'
)
select
    procedure_order_result_culture.procedure_order_result_key,
    coalesce(procedure_order_susceptibility.seq_num, 0) as susceptibility_seq_num,
    procedure_order_result_culture.patient_name,
    procedure_order_result_culture.mrn,
    procedure_order_result_culture.dob,
    procedure_order_result_culture.csn,
    procedure_order_result_culture.encounter_date,
    procedure_order_result_culture.procedure_order_id,
    procedure_order_result_culture.order_specimen_source,
    procedure_order_result_culture.specimen_taken_date,
    procedure_order_result_culture.result_date,
    procedure_order_result_culture.procedure_name,
    procedure_order_result_culture.result_seq_num,
    procedure_order_result_culture.result_component_name,
    procedure_order_result_culture.result_component_id,
    procedure_order_result_culture.result_status,
    procedure_order_result_culture.result_lab_status,
    procedure_order_result_culture.result_value,
    procedure_order_result_culture.culture_growth_ind,
    procedure_order_result_culture.culture_type,
    master_result_organism.organism_nm as organism_name,
    dict_antibiotic.dict_nm as antibiotic,
    dict_suscept.dict_nm as susceptibility,
    procedure_order_susceptibility.suscept_value,
    procedure_order_result_culture.proc_ord_key,
    procedure_order_result_culture.pat_key,
    procedure_order_result_culture.visit_key
from
    procedure_order_result_culture
    left join {{source('cdw', 'procedure_order_susceptibility')}} as procedure_order_susceptibility
        on procedure_order_susceptibility.proc_ord_key = procedure_order_result_culture.proc_ord_key
    left join {{source('cdw', 'cdw_dictionary')}} as dict_antibiotic
        on dict_antibiotic.dict_key = procedure_order_susceptibility.dict_antibiotic_key
    left join {{source('cdw', 'cdw_dictionary')}} as dict_suscept
        on dict_suscept.dict_key = procedure_order_susceptibility.dict_suscept_key
    left join {{source('cdw', 'master_result_organism')}} as master_result_organism
        on master_result_organism.organism_key = procedure_order_susceptibility.organism_key
where
    -- result must refer to organism or be a no-growth result to maintain match
    procedure_order_result_culture.culture_growth_ind = 0
        or regexp_extract(lower(procedure_order_result_culture.result_value),
        lower(master_result_organism.organism_nm), 1, 1) is not null
