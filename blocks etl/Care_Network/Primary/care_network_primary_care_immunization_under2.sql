with immun_billed_office_visit as (

    select
        encounter_primary_care.pat_key,
        encounter_primary_care.visit_key,
        encounter_primary_care.dept_key,
        encounter_primary_care.dob,
		visit.contact_dt_key,
		visit.visit_prov_key

       from
        {{ref('encounter_primary_care')}} as encounter_primary_care
        inner join {{source('cdw', 'visit')}} as visit
            on encounter_primary_care.visit_key = visit.visit_key
           inner join {{source('cdw', 'patient')}} as patient
            on encounter_primary_care.pat_key = patient.pat_key
           inner join {{source('cdw', 'master_date')}} as master_date
            on encounter_primary_care.encounter_date = master_date.full_dt

       where
        (extract(day from master_date.full_dt - encounter_primary_care.dob) / 30) <= 23
        and (extract(day from master_date.full_dt - encounter_primary_care.dob) / 30) >= 15
        and encounter_primary_care.dob >= to_date('12/01/2013', 'mm/dd/yyyy')
        and encounter_primary_care.dob + (365 * 2) <= current_date - 1
        and encounter_primary_care.office_visit_ind = '1'
        and visit.los_proc_cd like '99%'
        and visit.los_proc_cd != '99211.999'
),

--At least 2 well visits in first 12 months
immun_well_visit as (

    select
        encounter_primary_care.pat_key

    from
        {{ref('encounter_primary_care')}} as encounter_primary_care
        inner join {{source('cdw', 'visit')}} as visit
            on encounter_primary_care.visit_key = visit.visit_key
           inner join {{source('cdw', 'patient')}} as patient
            on encounter_primary_care.pat_key = patient.pat_key
           inner join {{source('cdw', 'master_date')}} as master_date
            on encounter_primary_care.encounter_date = master_date.full_dt

    where
        encounter_primary_care.well_visit_ind = 1
        and encounter_primary_care.dob >= to_date('12/01/2013', 'mm/dd/yyyy')
        and encounter_primary_care.dob + (365 * 2) <= current_date - 1
        and (extract(day from master_date.full_dt - encounter_primary_care.dob) / 30) <= 12
        and encounter_primary_care.office_visit_ind = '1'

    group by
        encounter_primary_care.pat_key

    having
        count(*) >= 2
),

--Excluded Patients based on Problem Lists
problem_list_ex as (

    select
		patient.pat_key

       from
        {{source('cdw', 'patient')}} as patient
           inner join {{source('cdw', 'patient_problem_list')}} as patient_problem_list
            on patient.pat_key = patient_problem_list.pat_key
           inner join {{source('cdw', 'diagnosis')}} as diagnosis
            on patient_problem_list.dx_key = diagnosis.dx_key
           inner join {{source('cdw', 'cdw_dictionary')}} as dict_prob_stat
            on dict_prob_stat.dict_key = patient_problem_list.dict_prob_stat_key

	where
        diagnosis.dx_cd like 'Z28.%'
        and diagnosis.dx_cd not like 'Z28.3%'
        and diagnosis.dx_cd != 'Z28.21'
        and (patient_problem_list.rslvd_dt is null or patient_problem_list.rslvd_dt > patient.dob + (365 * 2))
        and patient_problem_list.noted_dt < patient.dob + (365 * 2)
        and dict_prob_stat.dict_nm != 'DELETED'
),

--Excluded Patients based on Antigen Allergies
allergy_ex as (

    select
        patient.pat_key

	from
        {{source('cdw', 'patient')}} as patient
        inner join {{source('cdw', 'patient_allergy')}} as patient_allergy
            on patient.pat_key = patient_allergy.pat_key
        inner join {{source('cdw', 'allergen')}} as allergen
            on allergen.algn_key = patient_allergy.algn_key
        left join {{ref ('lookup_care_network_allergen_patterns')}} as lookup_care_network_allergen_patterns
            on lookup_care_network_allergen_patterns.description = 'allergen'
            and lower(allergen.algn_nm) like lookup_care_network_allergen_patterns.pattern

	where
        patient_allergy.noted_dt < patient.dob + (365 * 2)
        and lookup_care_network_allergen_patterns.pattern is not null

	group by
        patient.pat_key
),


rotavirus_15wks as (

	select
        patient_immunization.pat_key

	from
        {{source('cdw', 'patient_immunization')}} as patient_immunization
        inner join {{source('cdw', 'patient')}} as patient
            on patient_immunization.pat_key = patient.pat_key
        inner join {{source('cdw', 'immunization')}} as immunization
            on patient_immunization.immun_key = immunization.immun_key
        inner join {{source('cdw', 'cdw_dictionary')}} as dict_immun_stat
            on dict_immun_stat.dict_key = patient_immunization.dict_immun_stat_key

	where
        lower(immun_nm) like '%rotavirus%'
        and (patient_immunization.immun_dt) <= (patient.dob + (15 * 7))-- started before 15 weeks
        and dict_immun_stat.dict_nm != 'DELETED'

	group by
        patient_immunization.pat_key
),

--Excludes the patients manually entered by the user via Redcap Survey
excluded_pat as (

    select
        master_redcap_question.mstr_redcap_quest_key,
        master_redcap_question.element_label,
        redcap_detail.record,
        substr(
            coalesce(master_redcap_element_answr.element_desc, redcap_detail.value),
            1,
            250
        ) as answer

    from
        {{source('cdw', 'redcap_detail')}} as redcap_detail
        left join {{source('cdw', 'master_redcap_project')}} as master_redcap_project
            on master_redcap_project.mstr_project_key = redcap_detail.mstr_project_key
        left join {{source('cdw', 'master_redcap_question')}} as master_redcap_question
            on master_redcap_question.mstr_redcap_quest_key = redcap_detail.mstr_redcap_quest_key
        left join {{source('cdw', 'master_redcap_element_answr')}} as master_redcap_element_answr
            on master_redcap_element_answr.mstr_redcap_quest_key = redcap_detail.mstr_redcap_quest_key
            and redcap_detail.value = master_redcap_element_answr.element_id

    where
        redcap_detail.cur_rec_ind = 1
        and lower(project_nm) = 'exclude_patient' --project id = 41
),

ex_pat_info as (

    select
        record as record_id,
        max(case when lower(element_label) = 'pat_mrn_id' then answer end) as pat_mrn_id,
        max(case when lower(element_label) = 'metrics name' then answer end) as metrics_name

    from
        excluded_pat

    group by
        record
),

manual_pat_exclude as (

    select
        patient.pat_key,
        ex_pat_info.pat_mrn_id,
        ex_pat_info.metrics_name

    from
        ex_pat_info
        inner join {{source('cdw', 'patient')}} as patient
            on patient.pat_mrn_id = ex_pat_info.pat_mrn_id

    where
        lower(metrics_name) = 'immunization'
),

--last immun visit
imm_last_visit as (

    select
        pat_key,
        visit_key as last_visit_key,
        visit_prov_key as last_visit_prov_key,
        dept_key as last_visit_dept_key,
        contact_dt_key as last_contact_dt_key,
        rank() over (partition by pat_key order by contact_dt_key desc) as rnk_last_visit

    from
        immun_billed_office_visit
),

final as (

    select
        immun_billed_office_visit.pat_key,
        imm_last_visit.last_visit_key as visit_key,
        max(imm_last_visit.last_visit_prov_key) as visit_prov_key,
        max(imm_last_visit.last_visit_dept_key) as dept_key,
        max(imm_last_visit.last_contact_dt_key) as contact_dt_key,
        max(case when
            (well_immun_dta_count >= 4
             and well_immun_hep_b_imm_count >= 3
             and well_immun_ipv_count >= 3
             and well_immun_mmr_count >= 1
             and well_immun_pcv_count >= 3
             and well_immun_vzv_count >= 1
             and well_hep_a_count >= 1
             and well_immun_hib_count >= 3
             and (
                 case
                     when
                         rotavirus_15wks.pat_key is not null
                         and well_immun_rot_count + well_rot_immun_c1_count > 0
                       then well_immun_rot_count
                       else 2
                  end >= 2
                  or case
                      when
                          rotavirus_15wks.pat_key is not null
                          and well_immun_rot_count + well_rot_immun_c1_count > 0
                       then well_rot_immun_c1_count
                    else 3
                  end  >= 3
                  or case
                      when
                          rotavirus_15wks.pat_key is not null
                          and well_immun_rot_count + well_rot_immun_c1_count > 0
                          and well_rot_immun_c1_count > 0
                       then well_rot_immun_c1_count + well_immun_rot_count
              else null end >= 3)
            )
            then 1 else 0
           end) as immunization_ind

    from
        immun_billed_office_visit
        inner join immun_well_visit
            on immun_well_visit.pat_key = immun_billed_office_visit.pat_key
        inner join imm_last_visit
            on imm_last_visit.last_visit_key = immun_billed_office_visit.visit_key
            and imm_last_visit.rnk_last_visit = 1
        left join {{source('cdw', 'registry_wellness_over_2ys_hx')}} as registry_wellness_over_2ys_hx
            on registry_wellness_over_2ys_hx.pat_key = immun_billed_office_visit.pat_key
        left join problem_list_ex
            on immun_billed_office_visit.pat_key = problem_list_ex.pat_key
        left join allergy_ex
            on immun_billed_office_visit.pat_key = allergy_ex.pat_key
        left join rotavirus_15wks
            on immun_billed_office_visit.pat_key = rotavirus_15wks.pat_key
        left join manual_pat_exclude
            on manual_pat_exclude.pat_key = immun_billed_office_visit.pat_key

    where
        manual_pat_exclude.pat_key is null
        and problem_list_ex.pat_key is null
        and allergy_ex.pat_key is null
        and dm_date <= dob + cast('25 month' as interval)

    group by
        immun_billed_office_visit.pat_key,
        last_visit_key
)

select
    final.pat_key,
	final.visit_key,
    master_date.full_dt as contact_dt,
	provider.prov_id,
    provider.full_nm as provider_name,
    department.dept_id,
    department.dept_nm as department_name,
    final.immunization_ind
from
    final
    inner join {{source('cdw', 'master_date')}} as master_date
            on final.contact_dt_key = master_date.dt_key
    left join {{source('cdw', 'department')}} as department
            on final.dept_key = department.dept_key
    left join {{source('cdw', 'provider')}} as provider
            on final.visit_prov_key = provider.prov_key
