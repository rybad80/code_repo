with nephrology_historical_visit_count as (
    -- for each patient/date, pull count of nephrology office visits in year leading up to historical_date.
    select
        active_registry_historical.pat_key,
        active_registry_historical.historical_date,
        sum(case when stg_encounter_outpatient_raw.encounter_date
            between date(active_registry_historical.historical_date - interval '1 year')
            and date(active_registry_historical.historical_date)
            then 1 else 0 end) as last_year_visit_count
    from
        {{ref('stg_glean_active_registry_historical')}} as active_registry_historical
        inner join {{ref('stg_encounter_outpatient_raw')}} as stg_encounter_outpatient_raw
            on active_registry_historical.pat_key = stg_encounter_outpatient_raw.pat_key
        inner join  {{source('cdw', 'department')}} as department
            on stg_encounter_outpatient_raw.dept_key = department.dept_key
    where
        lower(department.specialty) = 'nephrology'
        and stg_encounter_outpatient_raw.specialty_care_ind = 1 -- completed office visit
    group by
        active_registry_historical.pat_key,
        active_registry_historical.historical_date
),

-- for each historical date, include one row for each patient who meets GLEAN cohort criteria as of that date.
-- include columns for registry metric values/sde metric values,
-- and create indicators to calculate summary metrics by week.
fact_chop_glean_historical_cohort_metrics as ( --noqa
    select distinct
        active_registry_historical.pat_key,
        stg_patient.sex,
        'CHOP' as facility, -- to support aggregation of CHOP data with other institutions' data.
        active_registry_historical.historical_date,
        round(extract(epoch from active_registry_historical.historical_date - stg_patient.dob) / 31557600.0, 2)
            as age_years_historical, -- approx. age as of historical date
        age_years_historical * 12 as age_months_historical, --apprx age in months as of historical date of interest
        -- registry and sde metric values
        registry_metrics_current_historical.last_neph_visit,
        last_neph_provider.full_nm as last_neph_prov,
        registry_metrics_current_historical.next_neph_appt,
        last_primary_dx.dx_nm as gd_primary_dx_last_neph_visit,
        registry_metrics_current_historical.remission_status,
        registry_metrics_current_historical.remission_status_date,
        registry_metrics_current_historical.urine_protein,
        registry_metrics_current_historical.last_urinalysis_3yr,
        registry_metrics_current_historical.admission_count_past_30_days,
        registry_metrics_current_historical.ip_days_past_30_days,
        registry_metrics_current_historical.revisit_7_day_acute_3_month,
        registry_metrics_current_historical.last_covid_19_vaccine,
        registry_metrics_current_historical.most_recent_flu_vaccine,
        registry_metrics_current_historical.most_recent_pneumovax,
        registry_metrics_current_historical.second_most_recent_pneumovax,
        registry_metrics_current_historical.most_recent_prevnar_13,
        registry_metrics_current_historical.second_most_recent_prevnar_13,
        registry_metrics_current_historical.third_most_recent_prevnar_13,
        registry_metrics_current_historical.fourth_most_recent_prevnar_13,
        last_neph_department.dept_nm as last_nephrology_department_name,
        sde_metrics_current_historical.phenotype,
        sde_metrics_current_historical.kidney_biopsy_date,
        sde_metrics_current_historical.kidney_biopsy_result,
        sde_metrics_current_historical.genetic_testing_performed,
        sde_metrics_current_historical.imm_rec_rev,
        sde_metrics_current_historical.tb,
        sde_metrics_current_historical.rd_counseling,
        sde_metrics_current_historical.patient_family_education,
        'CHOP' as site_name,
        -- indicators calculated from metric values 
        case
            when registry_metrics_current_historical.most_recent_flu_vaccine is not null
            then 1 else 0 end as flu_ind,
        case
            when registry_metrics_current_historical.most_recent_pneumovax is not null
            then 1 else 0 end as pneumovax_first_ind,
        case
            when registry_metrics_current_historical.second_most_recent_pneumovax is not null
            then 1 else 0 end as pneumovax_second_ind,
        pneumovax_first_ind + pneumovax_second_ind as n_pneumovax,
        case
            when registry_metrics_current_historical.most_recent_prevnar_13 is not null
            then 1 else 0 end as prevnar_first_ind,
        case
            when registry_metrics_current_historical.second_most_recent_prevnar_13 is not null
            then 1 else 0 end as prevnar_second_ind,
        case
            when registry_metrics_current_historical.third_most_recent_prevnar_13 is not null
            then 1 else 0 end as prevnar_third_ind,
        case
            when registry_metrics_current_historical.fourth_most_recent_prevnar_13 is not null
            then 1 else 0 end as prevnar_fourth_ind,
        prevnar_first_ind + prevnar_second_ind + prevnar_third_ind + prevnar_fourth_ind as n_prevnar,
        case
            when registry_metrics_current_historical.most_recent_prevnar_13
                <= add_months(date(stg_patient.dob), 24)
            then 1 else 0 end as first_prevnar_before_2_ind,
        case
            when registry_metrics_current_historical.second_most_recent_prevnar_13
                <= add_months(date(stg_patient.dob), 24)
            then 1 else 0 end as second_prevnar_before_2_ind,
        case
            when registry_metrics_current_historical.third_most_recent_prevnar_13
                <= add_months(date(stg_patient.dob), 24)
            then 1 else 0 end as third_prevnar_before_2_ind,
        case
            when registry_metrics_current_historical.fourth_most_recent_prevnar_13
                <= add_months(date(stg_patient.dob), 24)
            then 1 else 0 end as fourth_prevnar_before_2_ind,
        first_prevnar_before_2_ind + second_prevnar_before_2_ind + third_prevnar_before_2_ind
                + fourth_prevnar_before_2_ind as n_prevnar_before_age_2,
        case when lower(registry_metrics_current_historical.remission_status) like '%relapse%'
            and month(date(registry_metrics_current_historical.remission_status_date))
                = month(date(active_registry_historical.historical_date))
            then 1 else 0 end as relapse_ind,
        case
            when lower(registry_metrics_current_historical.urine_protein) like '%negative%'
            or lower(registry_metrics_current_historical.urine_protein) like '%trace%'
            then 1 else 0 end as urine_protein_remisson_ind,
        case
            when registry_metrics_current_historical.urine_protein is not null then 1 else 0
            end as urine_protein_complete_ind,
        case
            when lower(registry_metrics_current_historical.remission_status) like '%remission%'
            then 1 else 0 end as smartform_remission_ind,
        case
            when date(registry_metrics_current_historical.last_urinalysis_3yr)
                >= coalesce(date(registry_metrics_current_historical.remission_status_date), date('1900-01-01'))
                then urine_protein_remisson_ind
            when date(registry_metrics_current_historical.remission_status_date)
                > coalesce(date(registry_metrics_current_historical.last_urinalysis_3yr), date('1900-01-01'))
                then smartform_remission_ind
            else null end as remission_all_ind,
        case when sde_metrics_current_historical.phenotype is not null then 1 else 0 end as smartform_used_ind,
        --covid vaccine eligibility based on patient age as of historical date.
        -- Age-based eligibility provided by project team 8.2022
        case
            when registry_metrics_current_historical.last_covid_19_vaccine is not null
                and active_registry_historical.historical_date >= '2020-12-11'
                and age_years_historical >= 16 then 1
            when registry_metrics_current_historical.last_covid_19_vaccine is not null
                and active_registry_historical.historical_date >= '2021-05-10'
                and age_years_historical >= 12 then 1
            when registry_metrics_current_historical.last_covid_19_vaccine is not null
                and active_registry_historical.historical_date >= '2021-10-29'
                and age_years_historical >= 5 then 1
            when registry_metrics_current_historical.last_covid_19_vaccine is not null
                and active_registry_historical.historical_date >= '2022-06-19'
                and age_months_historical >= 6 then 1
            else 0 end as covid_vacc_ind,
        case
            when active_registry_historical.historical_date >= '2020-12-11' and age_years_historical >= 16 then 1
            when active_registry_historical.historical_date >= '2021-05-10' and age_years_historical >= 12 then 1
            when active_registry_historical.historical_date >= '2021-10-29' and age_years_historical >= 5 then 1
            when active_registry_historical.historical_date >= '2022-06-19' and age_months_historical >= 6 then 1
            else 0 end as covid_vacc_eligible_ind,
        case
            when lower(sde_metrics_current_historical.phenotype) like '%new onset%'
                then 1 else 0 end as new_onset_ind,
        case
            when lower(sde_metrics_current_historical.imm_rec_rev) = 'yes' and new_onset_ind = 1
                then 1 else 0 end as imm_rec_rev_ind,
        case
            when sde_metrics_current_historical.tb is not null and new_onset_ind = 1
                then 1 else 0 end as tb_ind,
        case
            when lower(sde_metrics_current_historical.rd_counseling) = 'yes' and new_onset_ind = 1
                then 1 else 0 end as rd_counseling_ind,
        case
            when lower(sde_metrics_current_historical.patient_family_education) = 'yes' and new_onset_ind = 1
                then 1 else 0 end as pat_fam_edu_ind,
        case
            when kidney_biopsy_date is not null then 1 else 0 end as kidney_biopsy_done_ind,
        case
            when lower(genetic_testing_performed) like '%yes%' then 1 else 0 end as genetic_testing_done_ind,
        case
            when revisit_7_day_acute_3_month = 1 then 1 else 0 end as revisit_7_day_acute_3_month_ind,
        -- prevnar/pneumovax/pneumococcal up-to-date criteria
        case
            when n_prevnar >= 4 then 1
            when age_years_historical >= 6 and n_prevnar >= 1 then 1
            when age_years_historical >= 6 and n_prevnar < 1 then 0
            -- if patient is age <2, they need 4 pcv 13 to be up to date
            when age_years_historical < 2 and n_prevnar >= 4 then 1
            -- if patient is age 2-6 and have received 4 pcv 13 before age 2, they are up to date
            when age_years_historical >= 2 and age_years_historical < 6 and n_prevnar_before_age_2 >= 4 then 1
            -- if patient is age 2-6 and have received 3 pcv 13 before age 2,
            -- they need one more pcv 13 to be up to date
            when age_years_historical >= 2 and age_years_historical < 6 and n_prevnar_before_age_2 = 3 then 0
            -- if patient is age 2-6 and have received 0, 1, or 2 pcv 13 before age 2,
            -- they need two more pcv13 after age 2 to be up to date
            when age_years_historical >= 2 and age_years_historical < 6 and n_prevnar_before_age_2 <= 2
                and n_prevnar < n_prevnar_before_age_2 + 2 then 0
            when age_years_historical >= 2 and age_years_historical < 6 and n_prevnar_before_age_2 <= 2
                and n_prevnar >= n_prevnar_before_age_2 + 2 then 1
            else null end as pcv_13_up_to_date_ind,
        case
            when pcv_13_up_to_date_ind is not null then 1 else 0 end as pcv_13_eligible_ind,
        -- pneumovax up-to-date if received two or more pneumovax,
        -- or if most recent pneumovax was less than 5 years ago.
        case
            when n_pneumovax >= 2 or (months_between(active_registry_historical.historical_date,
                date(most_recent_pneumovax)) / 12) <= 5 then 1 else 0 end as pneumovax_up_to_date_ind,
        --pneumococcal up-to-date if pcv13 and pneumovax are both up-to-date
        case
            when pcv_13_up_to_date_ind = 1 and pneumovax_up_to_date_ind = 1
            then 1 else 0 end as pneumococcal_up_to_date_ind,
        -- indicator to filter metrics to only patients seen in clinic (completed nephrology office visit)
        -- in year leading up to historical_date.
        case
            when nephrology_historical_visit_count.last_year_visit_count > 0
            then 1 else 0 end as seen_last_year_ind,
        -- indicator for filtering purposes within GLEAN Registry Tracking dashboard
        1 as active_glean_patient_ind,
        row_number() over (partition by stg_patient.pat_key,
                            date_trunc('month', active_registry_historical.historical_date)
                        order by active_registry_historical.historical_date desc) as rn,
                        -- rn = 1 pulls the last week of the month
        row_number() over (order by stg_patient.pat_key) as unique_rn
    from
        {{ref('stg_glean_active_registry_historical')}} as active_registry_historical
        left join {{ref('stg_glean_registry_metrics_current_historical')}} as registry_metrics_current_historical
            on active_registry_historical.pat_key = registry_metrics_current_historical.pat_key
            and active_registry_historical.historical_date = registry_metrics_current_historical.historical_date
        left join {{ref('stg_glean_sde_metrics_current_historical')}} as sde_metrics_current_historical
            on active_registry_historical.pat_key = sde_metrics_current_historical.pat_key
            and active_registry_historical.historical_date = sde_metrics_current_historical.historical_date
        inner join {{ref('stg_patient')}} as stg_patient
            on active_registry_historical.pat_key = stg_patient.pat_key
        left join {{source('cdw', 'provider')}} as last_neph_provider
            on registry_metrics_current_historical.last_neph_prov = last_neph_provider.prov_id
        left join {{source('cdw', 'diagnosis')}} as last_primary_dx
            on registry_metrics_current_historical.gd_primary_dx_last_neph_visit = last_primary_dx.dx_id
        inner join {{source('cdw', 'department')}} as last_neph_department
            on registry_metrics_current_historical.last_nephrology_department_id = last_neph_department.dept_id
        left join nephrology_historical_visit_count
            on active_registry_historical.pat_key = nephrology_historical_visit_count.pat_key
            and active_registry_historical.historical_date = nephrology_historical_visit_count.historical_date
    where
        -- criteria for active GLEAN patient
        (registry_metrics_current_historical.neph_count_ind = 1
        or (sde_metrics_current_historical.earliest_phenotype_entered_date
            <= active_registry_historical.cutoff_date))
        and registry_metrics_current_historical.dialysis_ind is null
            -- exclude patients who have had dialysis in past 30 days
        and registry_metrics_current_historical.transfer_ind is null
            -- exclude patients who have transferred
)

select
    {{
        dbt_utils.surrogate_key([
            'pat_key',
            'unique_rn',
            'site_name'
        ])
    }} as hashed_mrn,
    *
from
    fact_chop_glean_historical_cohort_metrics
