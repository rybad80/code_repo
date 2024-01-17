{{
  config(
    materialized='incremental',
    unique_key='primary_site_date_key'
  )
}}

with visit_summary as (
    select
        registry.pat_key,
        registry.mrn,
        registry.last_nephrology_department_name,
        registry.last_neph_visit,
        sum(case when patient_nephrology_visits.department_id = 82377022
            then 1 else 0 end) as vnj_nephrology_visit_count,
        sum(case when patient_nephrology_visits.department_id = 101012023
            then 1 else 0 end) as kop_nephrology_visit_count,
        sum(case when patient_nephrology_visits.department_id = 101022052
            then 1 else 0 end) as pnj_nephrology_visit_count,
        sum(case when patient_nephrology_visits.department_id = 101022049
            then 1 else 0 end) as virtua_nephrology_visit_count,
        sum(case when patient_nephrology_visits.department_id = 101012089
            then 1 else 0 end) as bwv_nephrology_visit_count,
        sum(case when patient_nephrology_visits.department_id = 101012142
            then 1 else 0 end) as bgr_nephrology_visit_count,
        sum(case when patient_nephrology_visits.department_id = 89375022
            then 1 else 0 end) as main_nephrology_visit_count,

        -- some patients will have visit_count_all_sites = 0 as this metric only considers in-person visits.
        case
            when
                (
                    vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                    + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                    + main_nephrology_visit_count > 0
                ) then
                  (
                      bgr_nephrology_visit_count + main_nephrology_visit_count
                  ) / (
                      vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                      + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                      + main_nephrology_visit_count
                  )
                  end as prop_bgr_main_nephrology,

        case
            when
                (
                    vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                    + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                    + main_nephrology_visit_count > 0
                ) then
                     (
                         vnj_nephrology_visit_count + virtua_nephrology_visit_count
                     ) / (
                         vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                         + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                         + main_nephrology_visit_count
                     )
                   -- vnj and virtua are combined at suggestion of team - overlap in visits among patients of
                   -- these sites
                   end as prop_vnj_virtua_nephrology,

        case
            when
                (
                    vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                    + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                    + main_nephrology_visit_count > 0
                ) then
                     kop_nephrology_visit_count / (
                         vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                         + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                         + main_nephrology_visit_count
                     )
                   end as prop_kop_nephrology,

        case
            when
                (
                    vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                    + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                    + main_nephrology_visit_count > 0
                ) then
                     pnj_nephrology_visit_count / (
                         vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                         + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                         + main_nephrology_visit_count
                     )
                   end as prop_pnj_nephrology,

        case
            when
                (
                    vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                    + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                    + main_nephrology_visit_count > 0
                ) then
                     bwv_nephrology_visit_count / (
                    vnj_nephrology_visit_count + kop_nephrology_visit_count + pnj_nephrology_visit_count
                    + virtua_nephrology_visit_count + bwv_nephrology_visit_count + bgr_nephrology_visit_count
                    + main_nephrology_visit_count
                     )
                   end as prop_bwv_nephrology,
        sum(case when visit_bgr_main_vaccines.prevnar_main_ind = 1
            then 1 else 0 end) as prevnar_vac_bgr_main_ind,
        sum(case when visit_bgr_main_vaccines.pneumovax_main_ind = 1
            then 1 else 0 end) as pneumovax_vac_bgr_main_ind,
        sum(case when visit_bgr_main_vaccines.flu_main_ind = 1
            then 1 else 0 end) as flu_vac_bgr_main_ind,
        sum(case when visit_bgr_main_vaccines.covid_19_main_ind = 1
            then 1 else 0 end) as covid_19_vac_bgr_main_ind,
        -- number of vaccines patient received at BGR/Main in 18 months preceding their most recent
        -- nephrology visit
        covid_19_vac_bgr_main_ind + flu_vac_bgr_main_ind + pneumovax_vac_bgr_main_ind
        + prevnar_vac_bgr_main_ind as sum_bgr_main_vac,
        /* determine each patient's primary site: primary site is bgr_main for patients with majority visits
        at BGR/Main, and for patients who received vaccinations at BGR/Main Nephrology in 18 months
        preceding last nephrology visit*/
        /* primary site is satellite site (kop, pnj, etc.) for patients with majority visits at that site,
        who received no vaccinations at BGR/Main Nephrology in 18 months preceding last nephrology visit*/
        -- primary site is undetermined if patient without a primary site (i.e. 50% of visits at each of two sites)
        case when (prop_bgr_main_nephrology > 0.50 or (prop_bgr_main_nephrology < 0.50 and sum_bgr_main_vac > 0))
                   or (
                       prop_bgr_main_nephrology is null and lower(
                           registry.last_nephrology_department_name
                       ) in ('bgr nephrology', 'main nephrology')
                   ) then 'bgr_main'
             when (sum_bgr_main_vac = 0
                  and (
                      prop_vnj_virtua_nephrology > 0.50 or (
                          prop_vnj_virtua_nephrology is null and lower(
                              registry.last_nephrology_department_name
                          ) in ('vnj nephrology', 'virtua nephrology')
                      )
                  )
              ) then 'vnj_virtua'
             when (sum_bgr_main_vac = 0
                  and (
                      prop_kop_nephrology > 0.50 or (
                          prop_kop_nephrology is null and lower(
                              registry.last_nephrology_department_name
                          ) = 'kop nephrology'
                      )
                  )
              ) then 'kop'
             when (sum_bgr_main_vac = 0
                  and (
                      prop_pnj_nephrology > 0.50 or (
                          prop_pnj_nephrology is null and lower(
                              registry.last_nephrology_department_name
                          ) = 'pnj nephrology'
                      )
                  )
              ) then 'pnj'
             when (sum_bgr_main_vac = 0
                  and (
                      prop_bwv_nephrology > 0.50 or (
                          prop_bwv_nephrology is null and lower(
                              registry.last_nephrology_department_name
                          ) = 'bwv nephrology'
                      )
                  )
              ) then 'bwv'
             /* undetermined indicates that the patient did not have over 50% of their visits at any given site
             (or pair of sites - VNJ/Virtua)*/
             else 'undetermined'
             end as primary_site
    from
        {{ ref('stg_glean_nephrology_registry')}} as registry
        left join
            {{ ref('stg_glean_nephrology_patient_nephrology_visits')}} as patient_nephrology_visits on
                registry.pat_key = patient_nephrology_visits.pat_key
        left join
            {{ ref('stg_glean_nephrology_visit_bgr_main_vaccines')}} as visit_bgr_main_vaccines on
                patient_nephrology_visits.visit_key = visit_bgr_main_vaccines.visit_key
    group by
        registry.pat_key,
        registry.mrn,
        registry.last_nephrology_department_name,
        registry.last_neph_visit
),
all_data as (--region begin to aggregate
    select
        *,
        --DOB will eventually need tro use submssion date note current date
        add_months(current_date, age_in_months * -1) as dob_est,
        age_in_months / 12 as age_years,
        case when most_recent_flu_vaccine_ is not null then 1 else 0 end as flu_ind,
        case when most_recent_pneumovax is not null then 1 else 0 end as pneumovax_first_ind,
        case when second_most_recent_pneumovax is not null then 1 else 0 end as pneumovax_second_ind,
        pneumovax_first_ind + pneumovax_second_ind as n_pneumovax,
        case when most_recent_prevnar_13 is not null then 1 else 0 end as prevnar_first_ind,
        case when second_most_recent_prevnar_13 is not null then 1 else 0 end as prevnar_second_ind,
        case when third_most_recent_prevnar_13 is not null then 1 else 0 end as prevnar_third_ind,
        case when fourth_most_recent_prevnar_13 is not null then 1 else 0 end as prevnar_fourth_ind,
        prevnar_first_ind + prevnar_second_ind + prevnar_third_ind + prevnar_fourth_ind as n_prevnar,
        case
            when most_recent_prevnar_13 <= add_months(date(dob_est), 24) then 1 else 0
        end as first_prevnar_before_2_ind,
        case
            when second_most_recent_prevnar_13 <= add_months(date(dob_est), 24) then 1 else 0
        end as second_prevnar_before_2_ind,
        case
            when third_most_recent_prevnar_13 <= add_months(date(dob_est), 24) then 1 else 0
        end as third_prevnar_before_2_ind,
        case
            when fourth_most_recent_prevnar_13 <= add_months(date(dob_est), 24) then 1 else 0
        end as fourth_prevnar_before_2_ind,
        first_prevnar_before_2_ind + second_prevnar_before_2_ind + third_prevnar_before_2_ind
        + fourth_prevnar_before_2_ind as n_prevnar_before_age_2,
        case
            when
                lower(
                    remission_status
                ) like '%relapse%' and month(date(remission_status_date)) = month(date(today)) then 1
            else 0
        end as relapse_ind,
        case
            when lower(urine_protein) like '%negative%' or lower(urine_protein) like '%trace%' then 1 else 0
        end as urine_protein_remisson_ind,
        case when urine_protein is not null then 1 else 0 end as urine_protein_complete_ind,
        case when lower(remission_status) like '%remission%' then 1 else 0 end as smartform_remission_ind,
        case
            when
                date(
                    last_urinalysis_3yr
                ) >= nvl(date(remission_status_date), date('1900-01-01')) then urine_protein_remisson_ind
             when
                 date(
                     remission_status_date
                 ) > nvl(date(last_urinalysis_3yr), date('1900-01-01')) then smartform_remission_ind
             else null end as remission_all_ind,
        case when phenotype is not null then 1 else 0 end as smartform_used_ind,
        --covid vaccine + 6+ months old (note that this was changed from 12+ years to 5+ years on 12.20.21, 
        --and changed from 5+ years to 6+ months on 8.18.22)
        case when last_covid_19_vaccine is not null and age_in_months >= 6
            then 1 else 0 end as covid_vacc_ind,
        case when age_in_months >= 6 then 1 else 0 end as over_6mos_ind,            
        case when lower(phenotype) like '%new onset%' then 1 else 0 end as new_onset_ind,
        case when lower(imm_rec_rev) = 'yes' and new_onset_ind = 1 then 1 else 0 end as imm_rec_rev_ind,
        case when tb is not null and new_onset_ind = 1 then 1 else 0 end as tb_ind,
        case when lower(rd_counseling) = 'yes' and new_onset_ind = 1 then 1 else 0 end as rd_counseling_ind,
        case when lower(patient_family_education) = 'yes' and new_onset_ind = 1
            then 1 else 0 end as pat_fam_edu_ind,
        --new metrics as of 5.11.2021
        case when kidney_biopsy_date is not null then 1 else 0 end as kidney_biopsy_done_ind,
        case when lower(genetic_testing_performed) like '%yes%' then 1 else 0 end as genetic_testing_done_ind,
        case when revisit_7_day_acute_3_month = 1 then 1 else 0 end as revisit_7_day_acute_3_month_ind
    from {{ ref('stg_glean_nephrology_registry')}} as registry
    inner join visit_summary on registry.pat_key = visit_summary.pat_key
--end region
),
pneumococcal as (--region pneumococcal logic is more complicated
    select
        *,
        case     when n_prevnar >= 4 then 1
                when age_years > 6 and n_prevnar >= 1 then 1
                when age_years > 6 and n_prevnar < 1 then 0
                --if patient is age <2, they need 4 pcv 13 to be up to date
                 when age_years < 2 and n_prevnar >= 4 then 1
                --if patient is age 2-6 and have received 4 pcv 13 before age 2, they are up to date
                when age_years > 2 and age_years < 6 and n_prevnar_before_age_2 >= 4 then 1
                --if patient is age 2-6 and have received 3 pcv 13 before age 2,
                -- they need one more pcv 13 to be up to date
                when age_years > 2 and age_years < 6 and n_prevnar_before_age_2 = 3 then 0
                --if patient is age 2-6 and have received 0, 1, or 2 pcv 13 before age 2,
                -- they need two more pcv13 after age 2 to be up to date
                when
                    age_years > 2 and age_years < 6 and n_prevnar_before_age_2 <= 2
                    and n_prevnar < n_prevnar_before_age_2 + 2 then 0
                when
                    age_years > 2 and age_years < 6 and n_prevnar_before_age_2 <= 2
                    and n_prevnar >= n_prevnar_before_age_2 + 2 then 1
                else null end as pcv_13_up_to_date_ind,
        case
            when
                n_pneumovax >= 2 or (months_between(current_date, date(most_recent_pneumovax)) / 12) <= 5 then 1
            else 0
        end as pneumovax_up_to_date_ind,
        --pcv13 up to date and either 2 pneumovax or 1 pneumovax within 5 years
        case
            when pcv_13_up_to_date_ind = 1 and pneumovax_up_to_date_ind = 1 then 1 else 0
        end as pneumococcal_up_to_date_ind,
        current_date as loaded_date
    from all_data
--end region
)

select
      {{
        dbt_utils.surrogate_key([
            'primary_site',
            'loaded_date'
            ])
    }} as primary_site_date_key,
    primary_site,
    loaded_date,
    count(*) as patient_count,
    sum(flu_ind) as flu_num, -- flu numerator
    -- flu denominator - in practice this is the same as n, including as its own column due to how the percentage
    -- was calculated (in case flu_ind diverges from n in the future)
    count(flu_ind) as flu_denom,
    round((sum(flu_ind) / nullif(count(flu_ind), 0)) * 100, 1) as flu_pct,
    sum(pneumovax_first_ind) as pneumovax_1_num, -- pneumovax numerator
    count(pneumovax_first_ind) as pneumovax_1_denom, -- pneumovax denominator
    --may need to exclude ineligible patients by age
    round((sum(pneumovax_first_ind) / nullif(count(pneumovax_first_ind), 0)) * 100, 1) as pneumovax_1_pct,
    sum(pcv_13_up_to_date_ind) as prevnar_13_num, -- prevnar 13 numerator
    -- prevnar 13 denominator - this denominator currently differs from n (can be null for some patients)
    count(pcv_13_up_to_date_ind) as prevnar_13_denom,
    round((sum(pcv_13_up_to_date_ind) / nullif(count(pcv_13_up_to_date_ind), 0)) * 100, 1) as prevnar_13_pct,
    sum(pneumococcal_up_to_date_ind) as pneumococcal_num, -- pneumococcal numerator
    count(pneumococcal_up_to_date_ind) as pneumococcal_denom, -- pneumococcal denominator
    round(
        (sum(pneumococcal_up_to_date_ind) / nullif(count(pneumococcal_up_to_date_ind), 0)) * 100, 1
    ) as pneumococcal_pct,
    sum(covid_vacc_ind) as covid_vacc_num, -- covid numerator
    -- covid denominator - this denominator currently differs from n (null for patients under age 6 months)
    sum(over_6mos_ind) as covid_vacc_denom,
    round((sum(covid_vacc_ind) / nullif(sum(over_6mos_ind), 0)) * 100, 1) as covid_vacc_pct,
    -- ip days per 100 pat months numerator (not including denominator as separate column - equals n)
    (sum(cast(ip_days_past_30_days as int)) * 100) as ip_days_per_100_pat_months_num,
    round((sum(cast(ip_days_past_30_days as int)) * 100) / nullif(patient_count, 0), 1)
        as ip_days_per_100_pat_months,
    -- admissions per 100 pat months numerator (denominator equals n)
    (sum(cast(admission_count_past_30_days as int)) * 100) as admissions_per_100_pat_months_num,
    round((sum(cast(admission_count_past_30_days as int)) * 100) / nullif(patient_count, 0), 1)
        as admissions_per_100_pat_months,
    -- relapses per 100 pat months numerator (denominator equals n)
    (sum(relapse_ind) * 100) as relapses_per_100_pat_months_num,
    round((sum(relapse_ind) * 100) / nullif(patient_count, 0), 1) as relapses_per_100_pat_months,
    /* note 12/20/21: the following three columns are not currently populating report
    leaving for potential future use */
    --round((sum(complete_remission_ind) / n) * 100, 1) as complete_remission_pct,
    --round((sum(partial_remission_ind) / n) * 100, 1) as partial_remission_pct,
    /* round((sum(complete_remission_ind) + sum(partial_remission_ind))/ n * 100,2)
        as complete_or_partial_remission_pct */
    sum(smartform_used_ind) as n_smartform_used,
    -- smartform remission pct numerator (not including denominator as separate column - equals n_smartform_used)
    (sum(smartform_remission_ind) * 100) as smartform_remission_num,
    round((sum(smartform_remission_ind) * 100) / nullif(sum(smartform_used_ind), 0), 1) as smartform_remission_pct,
    (sum(urine_protein_remisson_ind) * 100) as urine_protein_remission_num, -- urine protein remission numerator
    sum(urine_protein_complete_ind) as urine_protein_remission_denom, -- urine protein remission denominator
    round(
        (sum(urine_protein_remisson_ind) * 100) / nullif(sum(urine_protein_complete_ind), 0), 1
    ) as urine_protein_remission_pct,
    (sum(remission_all_ind) * 100) as remission_all_num, -- remission all numerator (denominator equals n)
    round((sum(remission_all_ind) * 100) / nullif(patient_count, 0), 1) as remission_all_pct,
    sum(new_onset_ind) as n_new_onset,
    (sum(imm_rec_rev_ind) * 100) as imm_rec_rev_num, -- imm rec rev numerator (denominator equals n_new_onset)
    round((sum(imm_rec_rev_ind) * 100 / nullif(n_new_onset, 0)), 1) as imm_rec_rev_pct,
    (sum(tb_ind) * 100) as tb_num, -- tb numerator (denominator equals n_new_onset)
    round((sum(tb_ind) * 100 / nullif(n_new_onset, 0)), 1) as tb_pct,
    (sum(rd_counseling_ind) * 100) as rd_counseling_num, -- rd counseling numerator(denominator equals n_new_onset)
    round((sum(rd_counseling_ind) * 100 / nullif(n_new_onset, 0)), 1) as rd_counseling_pct,
    (sum(pat_fam_edu_ind) * 100) as pat_fam_edu_num, -- pat fam edu numerator (denominator equals n_new_onset)
    round((sum(pat_fam_edu_ind) * 100 / nullif(n_new_onset, 0)), 1) as pat_fam_edu_pct,
    -- kidney biopsy done numerator (denominator equals n_smartform_used)
    (sum(kidney_biopsy_done_ind) * 100) as kidney_biopsy_done_num,
    round((sum(kidney_biopsy_done_ind) * 100) / nullif(sum(smartform_used_ind), 0), 1) as kidney_biopsy_done_pct,
    -- genetic testing done numerator (denominator equals n_smartform_used)
    (sum(genetic_testing_done_ind) * 100) as genetic_testing_done_num,
    round((sum(genetic_testing_done_ind) * 100) / nullif(sum(smartform_used_ind), 0), 1)
        as genetic_testing_done_pct,
    -- revisit 7 day acute 3 month numerator (denominator equals n_smartform_used)
    (sum(revisit_7_day_acute_3_month_ind) * 100) as revisit_7_day_acute_3_month_num,
    round(
        (sum(revisit_7_day_acute_3_month_ind) * 100) / nullif(sum(smartform_used_ind), 0), 1
    ) as revisit_7_day_acute_3_month_pct
    from
        pneumococcal
    group by
        -- present metrics by primary site for nephrology visits - bgr/main, voorhees/virtua
        -- (combined due to patient visit overlap), princeton, kop, brandywine, undetermined
        primary_site,
        loaded_date
