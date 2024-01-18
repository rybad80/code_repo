{{ config(meta = {
    'critical': true
}) }}

with cohort_labs as (
    select
        procedure_order_clinical.proc_ord_key,
        procedure_order_clinical.patient_name,
        procedure_order_clinical.mrn,
        procedure_order_clinical.csn,
        procedure_order_clinical.procedure_id,
        procedure_order_clinical.procedure_order_id,
        procedure_order_clinical.placed_date,
        procedure_order_clinical.specimen_taken_date,
        procedure_order_clinical.result_date,
        procedure_order_clinical.procedure_name,
        procedure_order_clinical.department_name,
        dim_department.intended_use_name, 
        procedure_order_clinical.pat_key,
        procedure_order_clinical.visit_key,
        procedure_order_clinical.procedure_order_type,
        max(
            case when master_question.quest_id in('123667', '123525') then order_question.ansr end
        ) as order_indication
    from
        {{ref('procedure_order_clinical')}} as procedure_order_clinical
        inner join {{ref('outbreak_master_covid_tests')}} as outbreak_master_covid_tests
            on outbreak_master_covid_tests.procedure_id = procedure_order_clinical.procedure_id
        left join {{source('cdw', 'order_question')}} as order_question
            on order_question.ord_key = procedure_order_clinical.proc_ord_key
        left join {{source('cdw', 'master_question')}} as master_question
            on master_question.quest_key = order_question.quest_key
        left join {{ref('dim_department')}} as dim_department 
            on procedure_order_clinical.dept_key = dim_department.dept_key 
    where
        outbreak_master_covid_tests.pcr_ind = 1
        and lower(procedure_order_clinical.order_status) not in ('canceled', 'not applicable')
        and procedure_order_clinical.procedure_order_type not in ('Parent Order', 'Future Order')
        and lower(procedure_order_clinical.procedure_group_name) = 'lab'
        and {{ limit_dates_for_dev(ref_date = 'procedure_order_clinical.placed_date') }}
    group by
        procedure_order_clinical.proc_ord_key,
        procedure_order_clinical.patient_name,
        procedure_order_clinical.mrn,
        procedure_order_clinical.csn,
        procedure_order_clinical.procedure_id,
        procedure_order_clinical.procedure_order_id,
        procedure_order_clinical.placed_date,
        procedure_order_clinical.specimen_taken_date,
        procedure_order_clinical.result_date,
        procedure_order_clinical.procedure_name,
        procedure_order_clinical.department_name,
        dim_department.intended_use_name,
        procedure_order_clinical.pat_key,
        procedure_order_clinical.visit_key,
        procedure_order_clinical.procedure_order_type
),

lab_results as (
    select
        cohort_labs.proc_ord_key,
        procedure_order_result_clinical.result_seq_num,
        max(
            case when dict_abnorm.dict_nm = 'Abnormal' then 1 else 0 end
        ) as abnormal_result_ind,
        coalesce(group_concat(lpad(result_seq_num, 2) || ' ' || result_value, ''), rslt_cmt) as result_value
    from
        cohort_labs
        inner join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
            on procedure_order_result_clinical.proc_ord_key = cohort_labs.proc_ord_key
        inner join {{ref('outbreak_master_covid_tests')}} as outbreak_master_covid_tests
            on outbreak_master_covid_tests.result_component_id = procedure_order_result_clinical.result_component_id --noqa: L016
        left join {{source('cdw', 'procedure_order_result_comment')}} as procedure_order_result_comment
            on procedure_order_result_comment.proc_ord_key = cohort_labs.proc_ord_key
            and procedure_order_result_comment.seq_num = 1
            and procedure_order_result_comment.cmt_num = 1
        left join {{source('cdw', 'procedure_order_result')}} as procedure_order_result
            on procedure_order_result.proc_ord_key = procedure_order_result_clinical.proc_ord_key
            and procedure_order_result.seq_num = procedure_order_result_clinical.result_seq_num
        left join {{source('cdw', 'cdw_dictionary')}} as dict_abnorm
            on dict_abnorm.dict_key = procedure_order_result.dict_abnorm_cd_key
    where
        outbreak_master_covid_tests.pcr_ind = 1
        and {{ limit_dates_for_dev(ref_date = 'procedure_order_result_clinical.placed_date') }}
    group by
        cohort_labs.proc_ord_key,
        procedure_order_result_clinical.result_seq_num,
        procedure_order_result_comment.rslt_cmt
)

select
    cohort_labs.proc_ord_key,
    lab_results.result_seq_num,
    cohort_labs.patient_name,
    patient.first_nm,
    patient.last_nm,
    cohort_labs.mrn,
    cohort_labs.csn,
    encounter_all.age_years,
    cohort_labs.procedure_id,
    cohort_labs.procedure_order_id,
    cohort_labs.placed_date,
    cohort_labs.procedure_order_type,
    cohort_labs.specimen_taken_date,
    lab_results.abnormal_result_ind,
    cohort_labs.order_indication,
    lab_results.result_value,
    case
        when lower(lab_results.result_value) like '%invalid%'
            or lower(lab_results.result_value) like '%inconclusive%'
            then 0 -- Inconclusive/invalid test
        when lab_results.abnormal_result_ind = 1
            then 3 -- Presumptive case (positive local test), confirmatory testing pending
        when lab_results.abnormal_result_ind in (0, -2)
        and (
        (lower(lab_results.result_value) not like '%invalid%'
            or lower(lab_results.result_value) not like '%inconclusive%')
        and lab_results.result_value is not null
        ) then 2 -- PUI tested negative
        else 1 -- PUI testing pending
    end as current_status,
    cohort_labs.result_date,
    cohort_labs.procedure_name,
    cohort_labs.department_name,
    cohort_labs.intended_use_name, 
    encounter_all.provider_name as encounter_provider,
    encounter_all.department_name as visit_dept,
    encounter_all.encounter_type,
    encounter_all.sex,
    encounter_all.patient_class,
    encounter_all.patient_address_zip_code,
    encounter_all.payor_group,
    stg_patient.county,
    stg_patient.race,
    stg_patient.ethnicity,
    stg_patient.race_ethnicity,
/*
    Main Clinical Lab Drive Thru Providers
    VOORHEES SPECIALTY CARE [  532764]
    BUCKS COUNTY SPECIALTY CARE [  532742]
    ROBERTS CENTER [  532741]
    BRANDYWINE VALLEY SPECIALTY CARE [  532760]
    Lab Provider, Site Three  [532743]
    WOODLAND REC CENTER    [532765]
    Lab Provider,Site Eight    [532799]
    Lab Provider,Site Seven    [532798]
    Drive Thru Providers, live 5/22
    Roberts Ctr Drive Up Test (532855)
    Bucks Co Drive Up Test  (532856)
    Brandywine Vly Drive Up Test (532857)
    Voorhees Drive Up Testing  (532858)
    Wood Rec Ctr Drive Up Test (532859)
    Mill Creek Rec Ctr Drive Up Test (532860)
    Kop Drive Up Testing   (532861)
    Wissinoming Prk Drive Up Test (532862)
    Pop Up Mobile Testing (532863)
    JJS COVID TESTING (532864)
    Phila Campus Covid Testing (532865)
*/
    case when encounter_all.provider_id in (
        '532760',
        '532742',
        '532741',
        '532764',
        '532743',
        '532765',
        '532799',
        '532798',
        '532855',
        '532856',
        '532857',
        '532858',
        '532859',
        '532860',
        '532861',
        '532862',
        '532863',
        '532864',
        '532865')
        then 1 else 0
    end as drive_thru_ind,
-- ROBERTS CENTER [  532741, 532855]
    case when encounter_all.provider_id in ('532741', '532855') then 1 else 0 end as roberts_drive_thru_ind,
-- BUCKS COUNTY SPECIALTY CARE [  532742]
    case when encounter_all.provider_id in ('532742', '532856') then 1 else 0 end as bucks_drive_thru_ind,
    cohort_labs.pat_key,
    cohort_labs.visit_key,
    case when account.acct_type = 'CHOP Occupational Health'
      then 1
      else 0
          end as occ_health_acct_ind,
    case when stg_outbreak_covid_employee_manual.false_positive_ind = 1
        then 1 else 0 end as false_positive_manual_review_ind
from
    cohort_labs
    inner join {{ref('encounter_all')}} as encounter_all
        on cohort_labs.visit_key = encounter_all.visit_key
    inner join {{source('cdw', 'visit')}} as visit
        on visit.visit_key = cohort_labs.visit_key
    inner join {{source('cdw', 'account')}} as account --noqa: L029
        on account.acct_key = visit.acct_key
    inner join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = cohort_labs.pat_key
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_key = cohort_labs.pat_key
    left join lab_results
        on lab_results.proc_ord_key = cohort_labs.proc_ord_key
    left join {{source('manual','stg_outbreak_covid_employee_manual')}} as stg_outbreak_covid_employee_manual
        on stg_outbreak_covid_employee_manual.proc_ord_id = cohort_labs.procedure_order_id
