with employee_workday as (
    select

        emp_key,
        emp_id,
        ad_login,
        comp_key

    from

        {{source('cdw','employee')}}

    where

        create_by = 'WORKDAY'
),
employee_common as (
    select

        emp_key,
        emp_id,
        comp_key,
        ad_login

    from

        {{source('cdw','employee')}}
),
main as (
    select

        client_id,
        case
            when length(trim(client_name)) = 0 then null
            else upper(trim(client_name))
        end as client_nm,
        employer_id,
        case
            when length(trim(employer_name)) = 0 then null
            else upper(trim(employer_name))
        end as employer_nm,
        participant_id,
        case
            when
                (
                    length(trim(corporate_hr_id)) = 0 or corporate_hr_id is null
                ) then '0'
            else corporate_hr_id
        end as o_corporate_hr_id,
        case
            when
                (
                    length(trim(corporate_hr_id)) = 0 or corporate_hr_id is null
                ) then 0
            else 100
        end as o_comp_key,
        0 as o_wd_comp_key,
        case
            when length(trim(phone)) = 0 then null
            else upper(trim(phone))
        end as ph_num,
        case
            when length(trim(occupation)) = 0 then null
            else upper(trim(occupation))
        end as occupation,
        case
            when length(trim(facility)) = 0 then null
            else upper(trim(facility))
        end as emp_facility,
        case
            when length(trim(location)) = 0 then null
            else upper(trim(location))
        end as loc,
        case
            when length(trim(division)) = 0 then null
            else upper(trim(division))
        end as division,
        case
            when length(trim(population_type)) = 0 then null
            else upper(trim(population_type))
        end as pop_type,
        case
            when length(trim(accreditation)) = 0 then null
            else upper(trim(accreditation))
        end as accreditation,
        badge_no as badge_num,
        case
            when length(trim(job_category)) = 0 then null
            else upper(trim(job_category))
        end as job_cat,
        to_date(last_updated_date, 'MM/DD/YYYY') as src_upd_dt,
        emp_survey_id,
        survey_id,
        case
            when length(trim(survey_code)) = 0 then null
            else upper(trim(survey_code))
        end as survey_cd,
        case
            when length(trim(survey_status)) = 0 then null
            else upper(trim(survey_status))
        end as survey_stat,
        case
            when length(trim(survey_last_updated_by)) = 0 then null
            else upper(trim(survey_last_updated_by))
        end as survey_upd_by,
        100 as o_comp_key_survey_last_upd_by,
        case
            when length(trim(survey_last_updated_date)) = 0 then null
            else to_date(survey_last_updated_date, 'MM/DD/YYYY')
        end as survey_upd_dt,
        case
            when
                (
                    incident_date is null or incident_time is null or incident_date = 'null' or incident_time = 'null'
                ) then null
            else to_timestamp(incident_date || ' ' || incident_time, 'MM/DD/YYYY HH24:MI')
        end as incident_dt,
        case
            when length(trim(injury_desc)) = 0 then null
            else upper(trim(injury_desc))
        end as injury_desc,
        case
            when length(trim(time_began_work)) = 0 or time_began_work = 'null' then null
            else upper(trim(time_began_work))
        end as work_start_time,
        case
            when length(trim(work_location)) = 0 then null
            else upper(trim(work_location))
        end as work_loc,
        case
            when length(trim(incident_location)) = 0 then null
            else upper(trim(incident_location))
        end as incident_loc,
        case
            when length(trim(incident_desc)) = 0 then null
            else upper(trim(incident_desc))
        end as incident_desc,
        case
            when length(trim(body_part)) = 0 then null
            else upper(trim(body_part))
        end as body_part,
        case
            when length(trim(observations)) = 0 then null
            else upper(trim(observations))
        end as observations,
        case
            when length(trim(influencing_conditions)) = 0 then null
            else upper(trim(influencing_conditions))
        end as influen_condi,
        case
            when length(trim(incident_cause)) = 0 then null
            else upper(trim(incident_cause))
        end as incident_cause,
        work_related,
        case
            when
                (
                    length(trim(work_related)) = 0 or work_related is null
                ) then '-2'
            when upper(trim(work_related)) = 'YES' then '1'
            when upper(trim(work_related)) = 'NO' then '0'
            else '-1'
        end as work_related_ind,
        case
            when length(trim(witnesses)) = 0 then null
            else upper(trim(witnesses))
        end as witnesses,
        case
            when length(trim(comments)) = 0 then null
            else upper(trim(comments))
        end as cmt,
        case
            when length(trim(device_involved)) = 0 then null
            else upper(trim(device_involved))
        end as device_involved,
        case
            when length(trim(device_mfg_model)) = 0 then null
            else upper(trim(device_mfg_model))
        end as device_mfg_model,
        case
            when
                (
                    length(trim(supervisor_informed)) = 0 or supervisor_informed is null
                ) then '-2'
            when upper(trim(supervisor_informed)) = 'YES' then '1'
            when upper(trim(supervisor_informed)) = 'NO' then '0'
            else '-1'
        end as supervisor_informed_ind,
        case
            when length(trim(supervisor_on_duty)) = 0 then null
            else upper(trim(supervisor_on_duty))
        end as supervisor_on_duty,
        case
            when (date_reported is null or time_reported = 'null' or date_reported = 'null' ) then null
            else
                to_timestamp(
                    date_reported || ' ' || case
                        when (length(trim(time_reported)) = 0 or time_reported is null) then '00:00'
                        else time_reported
                    end, 'MM/DD/YYYY HH24:MI'
                )
        end as report_dt,
        case
            when length(trim(supervisor_comments)) = 0 then null
            else upper(trim(supervisor_comments))
        end as supervisor_cmt,
        case
            when
                (
                    length(trim(seen_by_medical_provider)) = 0 or seen_by_medical_provider is null
                ) then '-2'
            when upper(trim(seen_by_medical_provider)) = 'YES' then '1'
            when upper(trim(seen_by_medical_provider)) = 'NO' then '0'
            else '-1'
        end as seen_by_med_prov_ind,
        case
            when length(trim(date_of_service)) = 0 then null
            else to_timestamp(date_of_service, 'MM/DD/YYYY')
        end as svc_dt,
        case
            when length(trim(provider)) = 0 then null
            else upper(trim(provider))
        end as prov_info,
        case
            when
                (
                    length(trim(medical_tx_desired)) = 0 or medical_tx_desired is null
                ) then '-2'
            when upper(trim(medical_tx_desired)) = 'YES' then '1'
            when upper(trim(medical_tx_desired)) = 'NO' then '0'
            else '-1'
        end as med_tx_desired_ind,
        case
            when
                (
                    length(trim(patient_involved)) = 0 or patient_involved is null
                ) then '-2'
            when upper(trim(patient_involved)) = 'YES' then '1'
            when upper(trim(patient_involved)) = 'NO' then '0'
            else '-1'
        end as pat_involved_ind,
        case
            when length(trim(patient_id)) = 0 then null
            else upper(trim(patient_id))
        end as pat_id,
        case
            when length(trim(signature)) = 0 then null
            else upper(trim(signature))
        end as signature,
        case
            when length(trim(marital_status)) = 0 then null
            else upper(trim(marital_status))
        end as marital_stat,
        dependent_count as dependent_cnt,
        case
            when length(trim(safeguards_desc)) = 0 then null
            else upper(trim(safeguards_desc))
        end as safeguards_desc,
        case
            when
                (
                    length(trim(safegards_used)) = 0 or safegards_used is null
                ) then '-2'
            when upper(trim(safegards_used)) = 'YES' then '1'
            when upper(trim(safegards_used)) = 'NO' then '0'
            else '-1'
        end as safeguards_used_ind,
        provider_2,
        case
            when length(trim(provider_2)) = 0 then null
            else upper(trim(provider_2))
        end as sec_prov_nm,
        current_timestamp as create_dt,
        'AXION' as create_by,
        current_timestamp as upd_dt,
        'AXION' as upd_by

    from

        {{source('readyset_ods', 'chop_self_rept_incident_survey')}}
)
select
    coalesce(
        case
            when ec1.emp_key >= 0 then ec1.emp_key
            else ew2.emp_key
        end, -1
    ) as emp_key,
    coalesce(
        case
            when ec2.emp_key >= 0 then ec2.emp_key
            else ew1.emp_key
        end, -1
    )as survey_upd_by_emp_key,
    client_id,
    pat_id,
    employer_id,
    emp_survey_id,
    participant_id,
    survey_id,
    client_nm,
    employer_nm,
    ph_num,
    occupation,
    emp_facility,
    loc,
    division,
    pop_type,
    accreditation,
    badge_num,
    job_cat,
    survey_cd,
    survey_stat,
    survey_upd_by,
    injury_desc,
    work_loc,
    incident_loc,
    incident_desc,
    body_part,
    observations,
    influen_condi,
    incident_cause,
    witnesses,
    cmt,
    device_involved,
    device_mfg_model,
    supervisor_on_duty,
    supervisor_cmt,
    prov_info,
    sec_prov_nm,
    signature,
    marital_stat,
    dependent_cnt,
    safeguards_desc,
    work_start_time,
    incident_dt,
    report_dt,
    src_upd_dt,
    survey_upd_dt,
    svc_dt,
    med_tx_desired_ind,
    pat_involved_ind,
    safeguards_used_ind,
    seen_by_med_prov_ind,
    supervisor_informed_ind,
    work_related_ind,
    create_dt,
    create_by,
    upd_dt,
    upd_by

from

main

left join employee_workday as ew1 on main.survey_upd_by = ew1.ad_login
    and main.o_wd_comp_key = ew1.comp_key
left join employee_common as ec2 on main.survey_upd_by = ec2.ad_login
    and main.o_comp_key_survey_last_upd_by = ec2.comp_key
left join employee_common as ec1 on main.o_corporate_hr_id = ec1.emp_id
    and main.o_comp_key = ec1.comp_key
left join employee_workday as ew2 on main.o_corporate_hr_id = ew2.emp_id
    and main.o_wd_comp_key = ew2.comp_key
