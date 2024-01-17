with external_sites as (
    select --noqa
        glean_network_registries_bjc.hashed_mrn,
        glean_network_registries_bjc.age_in_months,
        glean_network_registries_bjc.sex,
        glean_network_registries_bjc.facility,
        glean_network_registries_bjc.today,
        glean_network_registries_bjc.last_neph_visit,
        glean_network_registries_bjc.next_neph_appt,
        glean_network_registries_bjc.gd_primary_dx_last_neph_visit,
        glean_network_registries_bjc.phenotype,
        glean_network_registries_bjc.kidney_biopsy_date,
        glean_network_registries_bjc.kidney_biopsy_result,
        glean_network_registries_bjc.genetic_testing_done,
        glean_network_registries_bjc.remission_status,
        glean_network_registries_bjc.remission_status_date,
        glean_network_registries_bjc.last_ua_protein_yr,
        glean_network_registries_bjc.last_urinalysis_date_3yr,
        glean_network_registries_bjc.admission_count_past_30_days,
        glean_network_registries_bjc.ip_days_past_30_days,
        glean_network_registries_bjc.last_covid_19_vaccine,
        glean_network_registries_bjc.current_season_flu_vaccine,
        glean_network_registries_bjc.most_recent_pneumovax,
        glean_network_registries_bjc.second_most_recent_pneumovax,
        glean_network_registries_bjc.most_recent_prevnar_13,
        glean_network_registries_bjc.second_most_recent_prevnar_13,
        glean_network_registries_bjc.third_most_recent_prevnar_13,
        glean_network_registries_bjc.fourth_most_recent_prevnar_13,
        glean_network_registries_bjc.imm_rec_rev,
        glean_network_registries_bjc.tb_screen,
        glean_network_registries_bjc.nutrition_counseling,
        glean_network_registries_bjc.patient_family_education,
        glean_network_registries_bjc.upd_dt,
        'BJC' as site_name
    from
        {{source('manual_ods', 'glean_network_registries_bjc')}} as glean_network_registries_bjc
    union all
    select --noqa
        glean_network_registries_nem.hashed_mrn,
        glean_network_registries_nem.age_in_months,
        glean_network_registries_nem.sex,
        glean_network_registries_nem.facility,
        glean_network_registries_nem.today,
        glean_network_registries_nem.last_neph_visit,
        glean_network_registries_nem.next_neph_appt,
        glean_network_registries_nem.gd_primary_dx_last_neph_visit,
        glean_network_registries_nem.phenotype,
        glean_network_registries_nem.kidney_biopsy_date,
        glean_network_registries_nem.kidney_biopsy_result,
        glean_network_registries_nem.genetic_testing_done,
        glean_network_registries_nem.remission_status,
        glean_network_registries_nem.remission_status_date,
        glean_network_registries_nem.last_ua_protein_yr,
        glean_network_registries_nem.last_urinalysis_date_3yr,
        glean_network_registries_nem.admission_count_past_30_days,
        glean_network_registries_nem.ip_days_past_30_days,
        glean_network_registries_nem.last_covid_19_vaccine,
        glean_network_registries_nem.current_season_flu_vaccine,
        glean_network_registries_nem.most_recent_pneumovax,
        glean_network_registries_nem.second_most_recent_pneumovax,
        glean_network_registries_nem.most_recent_prevnar_13,
        glean_network_registries_nem.second_most_recent_prevnar_13,
        glean_network_registries_nem.third_most_recent_prevnar_13,
        glean_network_registries_nem.fourth_most_recent_prevnar_13,
        glean_network_registries_nem.imm_rec_rev,
        glean_network_registries_nem.tb_screen,
        glean_network_registries_nem.nutrition_counseling,
        glean_network_registries_nem.patient_family_education,
        glean_network_registries_nem.upd_dt,
        'NEM' as site_name
    from
        {{source('manual_ods', 'glean_network_registries_nem')}} as glean_network_registries_nem
),
unique_key as (
    select
        row_number() over (order by external_sites.hashed_mrn) as unique_rn,
        floor((external_sites.age_in_months * 1.00 / 12)) as age_years_historical,
        add_months(date_trunc('month', current_date), -cast(external_sites.age_in_months as int)) as dob,
        external_sites.*,
                case
            when external_sites.current_season_flu_vaccine is not null
            then 1 else 0 end as flu_ind,
        case
            when external_sites.most_recent_pneumovax is not null
            then 1 else 0 end as pneumovax_first_ind,
        case
            when external_sites.second_most_recent_pneumovax is not null
            then 1 else 0 end as pneumovax_second_ind,
            pneumovax_first_ind + pneumovax_second_ind as n_pneumovax, --noqa
        case
            when external_sites.most_recent_prevnar_13 is not null
            then 1 else 0 end as prevnar_first_ind,
        case
            when external_sites.second_most_recent_prevnar_13 is not null
            then 1 else 0 end as prevnar_second_ind,
        case
            when external_sites.third_most_recent_prevnar_13 is not null
            then 1 else 0 end as prevnar_third_ind,
        case
            when external_sites.fourth_most_recent_prevnar_13 is not null
            then 1 else 0 end as prevnar_fourth_ind,
            prevnar_first_ind + prevnar_second_ind + prevnar_third_ind + prevnar_fourth_ind as n_prevnar, --noqa
        case
            when external_sites.most_recent_prevnar_13
                <= add_months(date(dob), 24) --noqa
            then 1 else 0 end as first_prevnar_before_2_ind,
        case
            when external_sites.second_most_recent_prevnar_13
                <= add_months(date(dob), 24) --noqa
            then 1 else 0 end as second_prevnar_before_2_ind,
        case
            when external_sites.third_most_recent_prevnar_13
                <= add_months(date(dob), 24) --noqa
            then 1 else 0 end as third_prevnar_before_2_ind,
        case
            when external_sites.fourth_most_recent_prevnar_13
                <= add_months(date(dob), 24) --noqa
            then 1 else 0 end as fourth_prevnar_before_2_ind,
        first_prevnar_before_2_ind + second_prevnar_before_2_ind + third_prevnar_before_2_ind --noqa
                + fourth_prevnar_before_2_ind as n_prevnar_before_age_2, --noqa
        case when lower(external_sites.remission_status) like '%relapse%'
            and month(date(external_sites.remission_status_date))
                = month(date(external_sites.today))
            then 1 else 0 end as relapse_ind,
        case
            when lower(external_sites.last_ua_protein_yr) like '%negative%'
            or lower(external_sites.last_ua_protein_yr) like '%trace%'
            then 1 else 0 end as urine_protein_remisson_ind,
        case
            when external_sites.last_ua_protein_yr is not null then 1 else 0
            end as urine_protein_complete_ind,
        case
            when lower(external_sites.remission_status) like '%remission%'
            then 1 else 0 end as smartform_remission_ind,
        case
            when date(external_sites.last_urinalysis_date_3yr)
                >= coalesce(date(external_sites.remission_status_date), date('1900-01-01'))
                then urine_protein_remisson_ind --noqa
            when date(external_sites.remission_status_date)
                > coalesce(date(external_sites.last_urinalysis_date_3yr), date('1900-01-01'))
                then smartform_remission_ind --noqa
            else null end as remission_all_ind,
        case when external_sites.phenotype is not null then 1 else 0 end as smartform_used_ind,
        --covid vaccine eligibility based on patient age as of historical date.
        -- Age-based eligibility provided by project team 8.2022
        case
            when external_sites.last_covid_19_vaccine is not null
                and external_sites.today >= '2020-12-11'
                and age_years_historical >= 16 then 1 --noqa
            when external_sites.last_covid_19_vaccine is not null
                and external_sites.today >= '2021-05-10'
                and age_years_historical >= 12 then 1 --noqa
            when external_sites.last_covid_19_vaccine is not null
                and external_sites.today >= '2021-10-29'
                and age_years_historical >= 5 then 1 --noqa
            when external_sites.last_covid_19_vaccine is not null
                and external_sites.today >= '2022-06-19'
                and age_in_months >= 6 then 1 --noqa
            else 0 end as covid_vacc_ind,
        case
            when external_sites.today >= '2020-12-11' and age_years_historical >= 16 then 1 --noqa
            when external_sites.today >= '2021-05-10' and age_years_historical >= 12 then 1 --noqa
            when external_sites.today >= '2021-10-29' and age_years_historical >= 5 then 1 --noqa
            when external_sites.today >= '2022-06-19' and age_in_months >= 6 then 1 --noqa
            else 0 end as covid_vacc_eligible_ind,
        case
            when lower(external_sites.phenotype) like '%new onset%'
                then 1 else 0 end as new_onset_ind,
        case
            when lower(external_sites.imm_rec_rev) = 'yes' and new_onset_ind = 1 --noqa
                then 1 else 0 end as imm_rec_rev_ind,
        case
            when external_sites.tb_screen is not null and new_onset_ind = 1 --noqa
                then 1 else 0 end as tb_ind,
        case
            when lower(external_sites.nutrition_counseling) = 'yes' and new_onset_ind = 1 --noqa
                then 1 else 0 end as rd_counseling_ind,
        case
            when lower(external_sites.patient_family_education) = 'yes' and new_onset_ind = 1 --noqa
                then 1 else 0 end as pat_fam_edu_ind,
        case
            when kidney_biopsy_date is not null then 1 else 0 end as kidney_biopsy_done_ind, --noqa
        case
            when lower(genetic_testing_done) like '%yes%' then 1 else 0 end as genetic_testing_done_ind, --noqa
        -- *** below column not present in external data ***
        -- case
        --     when revisit_7_day_acute_3_month = 1 then 1 else 0 end as revisit_7_day_acute_3_month_ind,
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
            when pcv_13_up_to_date_ind is not null then 1 else 0 end as pcv_13_eligible_ind, --noqa
        -- pneumovax up-to-date if received two or more pneumovax,
        -- or if most recent pneumovax was less than 5 years ago.
        case
            when n_pneumovax >= 2 or (months_between(external_sites.today, --noqa
                date(most_recent_pneumovax)) / 12) <= 5 then 1 else 0 end as pneumovax_up_to_date_ind, --noqa
        --pneumococcal up-to-date if pcv13 and pneumovax are both up-to-date --noqa
        case
            when pcv_13_up_to_date_ind = 1 and pneumovax_up_to_date_ind = 1 --noqa
            then 1 else 0 end as pneumococcal_up_to_date_ind, --noqa
        -- indicator to filter metrics to only patients seen in clinic (completed nephrology office visit)
        -- in year leading up to historical_date.
        -- *** below column not present in external data ***
        -- case
        --     when nephrology_historical_visit_count.last_year_visit_count > 0
        --     then 1 else 0 end as seen_last_year_ind,
        -- indicator for filtering purposes within GLEAN Registry Tracking dashboard
        1 as active_glean_patient_ind
    from
        external_sites as external_sites
)
select
    {{
            dbt_utils.surrogate_key([
                'hashed_mrn',
                'unique_rn',
                'site_name'
            ])
    }} as hashed_mrn_key,
    *
from
    unique_key
