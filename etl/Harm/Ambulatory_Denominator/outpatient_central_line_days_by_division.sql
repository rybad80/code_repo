with dept_list as (

select
    stg_department_all.dept_key,
    stg_department_all.department_name,
    stg_department_all.specialty_name,
    case
        when stg_department_all.department_name like '%DIALYSIS%' then 'DIALYSIS'
        when stg_department_all.specialty_name in ('GI/NUTRITION', 'GASTROENTEROLOGY') then 'GI'
        when stg_department_all.specialty_name in ('HEMATOLOGY ONCOLOGY', 'RADIATION ONCOLOGY') then 'ONCOLOGY'
        else stg_department_all.specialty_name
        end as display_specialty
from
    {{ ref('stg_department_all') }} as stg_department_all
where
    stg_department_all.specialty_name in (
        'GASTROENTEROLOGY',
        'GI/NUTRITION',
        'ONCOLOGY',
        'HEMATOLOGY ONCOLOGY',
        'RADIATION ONCOLOGY'
        )
    or stg_department_all.department_name like '%DIALYSIS%'
group by
    stg_department_all.dept_key,
    stg_department_all.department_name,
    stg_department_all.specialty_name
),

care_coord_rsn as (

select
    stg_encounter.visit_key,
    max(case when master_reason_for_visit.rsn_id = 10000 then 1 else 0 end)
        as error_enc, --'Erroneous encounter-disregard'
    max(case when master_reason_for_visit.rsn_id = 1043 then 1 else 0 end)
        as irp_enc, --'IRP Home PN'
    max(case when master_reason_for_visit.rsn_id = 1044 then 1 else 0 end)
        as non_irp_enc --'Non-IRP Home PN'
from
    {{ref('stg_encounter')}} as stg_encounter
    left join {{ source('cdw', 'visit_reason') }} as visit_reason
        on visit_reason.visit_key = stg_encounter.visit_key
    left join {{ source('cdw', 'master_reason_for_visit') }} as master_reason_for_visit
        on master_reason_for_visit.rsn_key = visit_reason.rsn_key
where
    encounter_type_id = 160
    and stg_encounter.encounter_date < current_date
    and stg_encounter.encounter_date > '2018-07-01'
group by
    stg_encounter.visit_key
),

applicable_visits as (

select
    outpatient_central_line_days.patient_name,
    outpatient_central_line_days.pat_key,
    outpatient_central_line_days.month_dt,
    outpatient_central_line_days.last_day_of_month,
    stg_encounter.encounter_date,
    dept_list.department_name,
    dept_list.display_specialty,
    case when dept_list.display_specialty = 'GI' then 1 else 0 end as gi_ind,
    case when
        stg_encounter.visit_type_id in (
        '6886', --'FOL UP INTESTINAL REHAB'
        '6885' --'NEW INTESTINAL REHAB'
        ) or care_coord_rsn.irp_enc = 1 then 1
        else 0 end as irp_visit_ind,
    case when care_coord_rsn.non_irp_enc = 1 then 1 else 0 end as non_irp_visit_ind,
    max(case when dept_list.display_specialty = 'ONCOLOGY' then 1 else 0 end)
        over (partition by outpatient_central_line_days.pat_key, outpatient_central_line_days.month_dt)
        as onco_pat_that_month
from
    {{ref('outpatient_central_line_days')}} as outpatient_central_line_days
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.pat_key = outpatient_central_line_days.pat_key
    inner join dept_list on dept_list.dept_key = stg_encounter.dept_key
    left join {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
        on stg_encounter_outpatient_raw.visit_key = stg_encounter.visit_key
    left join care_coord_rsn on care_coord_rsn.visit_key = stg_encounter.visit_key
where
    (
        (stg_encounter_outpatient_raw.specialty_care_ind = 1
            and stg_encounter_outpatient_raw.visit_type not like '%SEC OP%'
            and stg_encounter_outpatient_raw.visit_type not like '%SECOND OP%'
            and stg_encounter_outpatient_raw.visit_type not like '%2ND OP%'
            and stg_encounter_outpatient_raw.visit_type not like 'LAB%'
        )
        or (stg_encounter.encounter_type_id = 160 --'Care Coordination'
            and coalesce(care_coord_rsn.error_enc, 0) = 0)
     )
    and stg_encounter.encounter_date < current_date
    and outpatient_central_line_days.month_dt = date_trunc('month', stg_encounter.encounter_date)
    -- encounter prior to line being removed
    and stg_encounter.encounter_date <= outpatient_central_line_days.last_day_of_month
),

division_attribution as (

select
    outpatient_central_line_days.patient_name,
    outpatient_central_line_days.pat_key,
    outpatient_central_line_days.mrn,
    outpatient_central_line_days.month_dt,
    outpatient_central_line_days.op_line_days,
    applicable_visits.display_specialty,
    max(applicable_visits.irp_visit_ind) as irp_subcohort_ind,
    max(applicable_visits.non_irp_visit_ind) as non_irp_subcohort_ind
from
    {{ref('outpatient_central_line_days')}} as outpatient_central_line_days
    inner join applicable_visits on applicable_visits.pat_key = outpatient_central_line_days.pat_key
where
    ( --The visit counts the patient as being part of that division for that month, +2 more
        (applicable_visits.display_specialty in ('GI', 'ONCOLOGY')
        and outpatient_central_line_days.month_dt between
                    date_trunc('month', applicable_visits.encounter_date)
                    and date_trunc('month', applicable_visits.encounter_date) + cast('2 months' as interval)
        )
    or (--For Dialysis you need a visit that month
        applicable_visits.display_specialty = 'DIALYSIS'
        and outpatient_central_line_days.month_dt = date_trunc('month', applicable_visits.encounter_date)
        )
    )
    -- Exclude the GI visits when patient visits Onco in that month
    and (applicable_visits.onco_pat_that_month + applicable_visits.gi_ind) < 2
group by
    outpatient_central_line_days.patient_name,
    outpatient_central_line_days.pat_key,
    outpatient_central_line_days.mrn,
    outpatient_central_line_days.month_dt,
    outpatient_central_line_days.op_line_days,
    applicable_visits.display_specialty
)

select
    patient_name,
    pat_key,
    mrn,
    month_dt,
    op_line_days,
    case
        when display_specialty = 'GI' and irp_subcohort_ind = 1 then 'IRP'
        when display_specialty = 'GI' and non_irp_subcohort_ind = 1 then 'NON-IRP'
        when display_specialty = 'GI' and non_irp_subcohort_ind = 0 and irp_subcohort_ind = 0 then 'GI-OTHER'
        else display_specialty
        end as display_specialty
from
    division_attribution
