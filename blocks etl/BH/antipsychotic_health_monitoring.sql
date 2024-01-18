with base  as (
select
    medication_order_administration.visit_key,
    medication_order_administration.pat_key,
    medication_order_administration.mrn,
    --used because historical meds do not have an ordering provider.case
    case when
            medication_order_administration.authorizing_provider_name = 'HISTORICAL MEDICATION' then
            initcap(encounter_all.provider_name)
        else medication_order_administration.authorizing_provider_name
    end as med_provider_names,
    medication_order_administration.authorizing_prov_key,
    encounter_all.provider_name as encounter_provider,
    medication_order_administration.ordering_provider_name,
    medication_order_administration.encounter_date,
    medication_order_administration.medication_name,
    medication_order_administration.medication_order_name,
    medication_order_administration.order_status,
    medication_order_administration.pharmacy_class,
    medication_order_administration.pharmacy_sub_class,
    case when encounter_all.department_name = '12 NORTHWEST' then 'PB Main BHIP'
        when encounter_all.department_name = '4 EAST CSH' then '4 East CSH'
        when encounter_all.department_name = 'ATL DEVELOPMENTAL PEDS' then 'ATL Dev Peds'
        when encounter_all.department_name = 'ATL BH DAY HOSPITAL' then 'ATL BH Day Hospital'
        when encounter_all.department_name = 'ATL BEHAVIORAL HEALTH' then 'ATL BH'
        when encounter_all.department_name = 'BGR BH PHAB' then 'BGR PHAB'
        when encounter_all.department_name like 'BGR BEHAVIO%' then 'BGR BH'
        when encounter_all.department_name like 'CSH%' then 'CSH BH'
        when encounter_all.department_name like 'HAVERFORD%' then 'Haverford BH'
        when encounter_all.department_name like 'KOP BEHAV%' then 'KOP BH'
        when encounter_all.department_name = 'BUC DEVELOPMENTAL PEDS' then 'BUC Dev Peds'
        when encounter_all.department_name = 'KOP DEVELOPMENTAL PEDS' then 'KOP Dev Peds'
        when encounter_all.department_name = 'MAIN BEHAVIORAL HEALTH' then 'Main BH'
        when encounter_all.department_name = 'MKT 3440 BH AUTISM' then 'Autism'
        when encounter_all.department_name = 'MKT 3550 BH EATING DIS' then 'Eating Disorder'
        when encounter_all.department_name = 'MKT 3550 DEVELOP PEDS' then '3550 Dev Peds'
        when encounter_all.department_name like 'MKT 4601 BEHAVIORAL%' then '4601 BH'
        when encounter_all.department_name like '%EATING DIS%' then 'Eating Disorder'
        when encounter_all.department_name = 'PB MAIN BHIP' then 'PB Main BHIP'
        when encounter_all.department_name = 'SOUTH PHILA BH' then 'South Philly BH'
        when encounter_all.department_name = 'VNJ ADOL SPC CARE' then 'VNJ Adol Spc Care'
        when encounter_all.department_name = 'VNJ BEHAVIORAL HEALTH' then 'VNJ BH'
        when encounter_all.department_name = 'VNJ DEVELOPMENTAL PEDS' then 'VNJ Dev Peds'
        else initcap(encounter_all.department_name) end as department_name,
    encounter_all.encounter_type,
    bh_departments.program as bh_program,
    bh_departments.division as bh_division,
    --Want to limit to most recent order for each patient
    row_number() over(
        partition by
            medication_order_administration.pat_key
        order by medication_order_administration.encounter_date desc
    ) as rn
from  {{ref('medication_order_administration')}} as medication_order_administration
inner join {{ref('encounter_all')}} as encounter_all on
        encounter_all.visit_key = medication_order_administration.visit_key
inner join {{ref('bh_departments')}} as bh_departments on
        bh_departments.department_name = medication_order_administration.ordering_department
where
    medication_order_administration.order_status in ('Sent', 'NOT APPLICABLE')
    and lower(medication_order_administration.pharmacy_class) = 'antipsychotics'
    and generic_medication_name not like '%lithium%'
    and generic_medication_name not like 'PROCHLOR%'
    and encounter_all.encounter_date >= '2018-01-01'
)


select
    base.visit_key,
    base.pat_key,
    base.mrn,
    base.encounter_date,
    bh_hm_plan.hm_plan_name,
    bh_hm_plan.hm_topic_name,
    bh_hm_plan.hmt_status,
    bh_hm_plan.hm_ideal_return_dt,
    bh_hm_plan.baseline_ind,
    base.med_provider_names,
    base.authorizing_prov_key,
    base.encounter_provider,
    base.ordering_provider_name,
    base.medication_name,
    base.medication_order_name,
    base.order_status,
    base.pharmacy_class,
    base.pharmacy_sub_class,
    base.department_name,
    base.encounter_type,
    base.bh_program,
    base.bh_division
from
{{ref('bh_hm_plan')}} as bh_hm_plan
left join base as base on
    bh_hm_plan.pat_key = base.pat_key
where base.rn = 1 and bh_hm_plan.hm_plan_id = 6
