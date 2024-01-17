with marketing_ci_sick_season_survey_fy24 as (
    select
        record_id,
        email,
        chop_family_feedback_survey_timestamp,
        starttime,
        case
            when lower(chop_family_feedback_survey_complete) = 'complete' then
            chop_family_feedback_survey_timestamp::timestamp
            else starttime
        end as survey_timestamp,
        case
            when extract(dow from starttime) != 1 then
                date_trunc('week', starttime) - 1
            else date(starttime)
        end as week_start ,
        case
            when extract(dow from starttime) != 1 then
                date_trunc('week', starttime) - 1 + 6
            else date(starttime) + 6
        end as week_end,
        month(starttime) as start_month,
        respondent,
        state,
        case
            when regexp_replace(state, '.*\((.*)\)', '\1') in ('PA', 'NJ', 'DE')
                then regexp_replace(state, '.*\((.*)\)', '\1')
            when state = 'Living outside of U.S.' then 'International'
            else 'Other/US Territory'
        end as state_abbr, --noqa: PRS,L048
        coalesce(county_pa, county_nj, county_de, county_blank) as county,
        case
            when county_de = 'New Castle County'
                or county_pa in ('Bucks County', 'Chester County',
                'Delaware County', 'Montgomery County', 'Philadelphia County' )
                or county_nj in ('Atlantic County', 'Burlington County',
                'Camden County', 'Cape May County', 'Cumberland County',
                'Gloucester County', 'Salem County') then 1
            when state is not null then 0
        end as chop_primary_service_area,
        case
            when county_pa in ('Bucks County', 'Chester County',
                'Delaware County', 'Montgomery County', 'Philadelphia County' ) then 1
            when state is not null then 0
        end as chop_primary_service_area_pa_only,
        case
            when county_nj in ('Atlantic County', 'Burlington County',
                'Camden County', 'Cape May County', 'Cumberland County',
                'Gloucester County', 'Salem County') then 1
            when state is not null then 0
        end as chop_primary_service_area_nj_only,
        case
            when childages___1 = 'Checked' then 1
            else 0
        end as under_age_1_ind,
        case
            when childages___2 = 'Checked' then 1
            else 0
        end as age_1_3_yrs_ind,
        case
            when childages___3 = 'Checked' then 1
            else 0
        end as age_4_5_yrs_ind,
        case
            when childages___4 = 'Checked' then 1
            else 0
        end as age_6_8_yrs_ind,
        case
            when childages___5 = 'Checked' then 1
            else 0
        end as age_9_12_yrs_ind,
        case
            when childages___6 = 'Checked' then 1
            else 0
        end as age_13_17_yrs_ind,
        case
            when under_age_1_ind = 1
                or age_1_3_yrs_ind = 1
                or age_4_5_yrs_ind = 1
                or age_6_8_yrs_ind = 1
                or age_9_12_yrs_ind = 1
                or age_13_17_yrs_ind = 1 then 1
            else 0
        end as childages_reported,
        case
            when age_13_17_yrs_ind = 1 then '13-17 yrs'
            when age_9_12_yrs_ind = 1 then '9-12 yrs'
            when age_6_8_yrs_ind = 1 then '6-8 yrs'
            when age_4_5_yrs_ind = 1 then '4-5 yrs'
            when age_1_3_yrs_ind = 1 then '1-3 yrs'
            when under_age_1_ind = 1 then '<1 yrs'
            else '>17 yrs'
        end  as oldest_child_age_group,
        case
            when age_13_17_yrs_ind = 1 then 6
            when age_9_12_yrs_ind = 1 then 5
            when age_6_8_yrs_ind = 1 then 4
            when age_4_5_yrs_ind = 1 then 3
            when age_1_3_yrs_ind = 1 then 2
            when under_age_1_ind = 1 then 1
            else 98
        end  as oldest_child_age_group_cat,
        case
            when chopcare___1 = 'Checked' then 1
            else 0
        end as chopcare_primary_care,
        case
            when chopcare___2 = 'Checked' then 1
            else 0
        end as chopcare_urgent_care,
        case
            when chopcare___3 = 'Checked' then 1
            else 0
        end as chopcare_op_specialty_care,
        case
            when chopcare___4 = 'Checked' then 1
            else 0
        end as chopcare_ip_hospital_care,
        case
            when chopcare___5 = 'Checked' then 1
            else 0
        end as chopcare_emergency_care,
        case
            when chopcare___98 = 'Checked' then 1
            else 0
        end as chopcare_notlisted,
        chopcare_other as chopcare_other,
        case
            when chopcare___99 = 'Checked' then 1
            else 0
        end as chopcare_noneofthese,
        case
            when chopcare_primary_care = 1
                or (chopcare_urgent_care = 1)
                or (chopcare_op_specialty_care = 1)
                or (chopcare_ip_hospital_care = 1)
                or (chopcare_emergency_care = 1)
                or (chopcare_notlisted = 1)
                or (chopcare_noneofthese = 1)
                or (chopcare_other is not null) then 1
            else 0
        end as chopcare_reported,
            case
            when chopcare_primary_care = 1 then 1
            when (chopcare_urgent_care = 1)
                or (chopcare_op_specialty_care = 1)
                or (chopcare_ip_hospital_care = 1)
                or (chopcare_emergency_care = 1)
                or (chopcare_notlisted = 1)
                or (chopcare_noneofthese = 1)
                or (chopcare_other is not null) then 0
        end as chop_primary_care,
        case
            when buzz___1 = 'Checked' then 1
            else 0
        end as buzz_flu,
        case
            when buzz___2 = 'Checked' then 1
            else 0
        end as buzz_covid19,
        case
            when buzz___3 = 'Checked' then 1
            else 0
        end as buzz_rsv,
        case
            when buzz___4 = 'Checked' then 1
            else 0
        end as buzz_rhino,
        case
            when buzz___5 = 'Checked' then 1
            else 0
        end as buzz_norovirus,
        case
            when buzz___6 = 'Checked' then 1
            else 0
        end as buzz_strep,
        case
            when buzz___7 = 'Checked' then 1
            else 0
        end as buzz_measles,
        null as buzz_pneumonia,
        null as buzz_conjunctivitis,
        null as buzz_hand_foot_mouth_disease,
        case
            when buzz___98 = 'Checked' then 1
            else 0
        end as buzz_notlisted,
        buzz_other as buzz_otherillnesses,
        case
            when buzz___99 = 'Checked' then 1
            else 0
        end as buzz_noneofthese,
        case
            when buzz_flu = 1
                or (buzz_covid19 = 1)
                or (buzz_rsv = 1)
                or (buzz_rhino = 1)
                or (buzz_norovirus = 1)
                or (buzz_strep = 1)
                or (buzz_measles = 1)
                or (buzz_notlisted = 1)
                or (buzz_noneofthese = 1)
                or (buzz_other is not null) then 1
            else 0
        end as buzz_reported,
        buzz_flu + buzz_covid19 + buzz_rsv
        + buzz_rhino + buzz_norovirus + buzz_measles
        + buzz_strep + buzz_notlisted + case when
            (buzz_other is not null) and buzz_notlisted != 1 then 1
            else 0
        end as no_of_buzz_reported,

        case
            when buzzsources___1 = 'Checked' then 1
            else 0
        end as buzzsources_email,
        case
            when buzzsources___2 = 'Checked' then 1
            else 0
        end as buzzsources_socialmedia_chop,
        case
            when buzzsources___3 = 'Checked' then 1
            else 0
        end as buzzsources_child_docs_nurses,
        case
            when buzzsources___4 = 'Checked' then 1
            else 0
        end as buzzsources_school_daycare,
        case
            when buzzsources___5 = 'Checked' then 1
            else 0
        end as buzzsources_socialmedia_acquaintance,
        case
            when buzzsources___6 = 'Checked' then 1
            else 0
        end as buzzsources_news,
        case
            when buzzsources___7 = 'Checked' then 1
            else 0
        end as buzzsources_local_healthofficials,
        case
            when buzzsources___8 = 'Checked' then 1
            else 0
        end as buzzsources_acquaintance_conversation,
        case
            when buzzsources___98 = 'Checked' then 1
            else 0
        end as buzzsources_notlisted,
        buzzsources_other,
        case
            when buzzsources_email = 1
                or buzzsources_socialmedia_chop = 1
                or buzzsources_child_docs_nurses = 1
                or buzzsources_school_daycare = 1
                or buzzsources_socialmedia_acquaintance = 1
                or buzzsources_news = 1
                or buzzsources_local_healthofficials = 1
                or buzzsources_notlisted = 1
                or buzzsources_other is not null then 1
            else 0
        end as buzzsource_reported,
        buzzsources_email
                + buzzsources_socialmedia_chop
                + buzzsources_child_docs_nurses
                + buzzsources_school_daycare
                + buzzsources_socialmedia_acquaintance
                + buzzsources_news
                + buzzsources_local_healthofficials
                + buzzsources_notlisted
        + case
            when buzzsources_other is not null and buzzsources_notlisted != 1 then 1
            else 0
        end as no_of_buzzsource_reported,
        case
            when precautions___1 = 'Checked' then 1
            else 0
        end as precautions_employee_flu_vaccine,
        case
            when precautions___2 = 'Checked' then 1
            else 0
        end as precautions_employee_covid_vaccine,
        case
            when precautions___3 = 'Checked' then 1
            else 0
        end as precautions_employee_masking,
        case
            when precautions___4 = 'Checked' then 1
            else 0
        end as precautions_visitor_masking,
        case
            when precautions___5 = 'Checked' then 1
            else 0
        end as precautions_employee_daily_flu_covid_test,
        case
            when precautions___6 = 'Checked' then 1
            else 0
        end as precautions_enhanced_cleaning_sick_season,
        case
            when precautions___7 = 'Checked' then 1
            else 0
        end as precautions_patients_wait_in_car,
        case
            when precautions___8 = 'Checked' then 1
            else 0
        end as precautions_require_negative_covidtest_before_surgery,
        case
            when precautions_additional___98 = 'Checked' then 1
            else 0
        end as precautions_notlisted,
        precautions_additional_other,
        case
            when  precautions_additional___99 = 'Checked' then 1
            else 0
        end as precautions_noneofthese,
        case
            when precautions_employee_flu_vaccine = 1
                or precautions_employee_covid_vaccine = 1
                or precautions_employee_masking = 1
                or precautions_visitor_masking = 1
                or precautions_employee_daily_flu_covid_test = 1
                or precautions_enhanced_cleaning_sick_season = 1
                or precautions_patients_wait_in_car = 1
                or precautions_require_negative_covidtest_before_surgery = 1
                or precautions_notlisted = 1
                or precautions_noneofthese = 1
                or precautions_additional_other is not null then 1
            else 0
        end as precautions_reported,
        precautions_employee_flu_vaccine
        + precautions_employee_covid_vaccine
        + precautions_employee_masking
        + precautions_visitor_masking
        + precautions_employee_daily_flu_covid_test
        + precautions_enhanced_cleaning_sick_season
        + precautions_patients_wait_in_car
        + precautions_require_negative_covidtest_before_surgery
        + precautions_notlisted
        + precautions_noneofthese
        + case
            when precautions_additional_other is not null and precautions_notlisted != 1 then 1
            else 0
        end as no_of_precautions_reported,
        nosamedayappt as no_same_day_appt,
        preferreduc,
        preferreded,
        case
            when preferreded is null then null
            when preferreded
                in ('CHILDREN''S HOSPITAL OF PHILADELPHIA - MAIN HOSPITAL (PHILADELPHIA)', --noqa: PRS,L048
                    'CHILDREN''S HOSPITAL OF PHILADELPHIA - MIDDLEMAN FAMILY PAVILION (KING OF PRUSSIA)')  --noqa: PRS,L048
                then 1
            else 0
        end as chop_ed, --noqa: PRS,L048
        preferreduc_piping,
        preferreded_piping,
        sickseasonseverity,
        case
            when sickseasonseverity is null
            then null
        when sickseasonseverity in ('Severe',
                                        'Very Severe')
                then 1
            else 0
        end as sickseasonseverity_bottom_2_box,
        case
            when sickseasonseverity is null
            then null
            when sickseasonseverity in ('Very Mild',
                                        'Mild')
                then 1
            else 0
        end as sickseasonseverity_top_2_box,
        currentstate,
        case
            when currentstate is null
            then null
            when currentstate = 'The worst is behind us' then 1
            else 0
        end as currentstate_positive,
        case
            when currentstate is null
            then null
            when currentstate in ('We''re in the middle of the worst part', --noqa: PRS,L048
                                    'The worst is yet to come') then 1
            else 0
        end as currentstate_negative, --noqa: PRS,L048
        pcappt_sameday,
        case
        when pcappt_sameday is null
            then null
            when pcappt_sameday in ('Definitely could NOT get',
                                        'Probably could NOT get')
                then 1
            else 0
        end as pcappt_sameday_bottom_2_box,
        case
            when pcappt_sameday is null
            then null
            when pcappt_sameday in ('Probably could get',
                                        'Definitely could get')
                then 1
            else 0
        end as pcappt_sameday_top_2_box,
        pcappt_nextday,
        case
            when pcappt_nextday is null
            then null
            when pcappt_nextday in ('Definitely could NOT get',
                                        'Probably could NOT get')
                then 1
            else 0
        end as pcappt_nextday_bottom_2_box,
        case
            when pcappt_nextday is null
            then null
            when pcappt_nextday in ('Probably could get',
                                        'Definitely could get')
                then 1
            else 0
        end as pcappt_nextday_top_2_box,
        waittime_uc,
        case
            when waittime_uc is null then null
            when waittime_uc in ('Much longer than average',
                                        'Longer than average')
                then 1
            else 0
        end as waittime_uc_bottom_2_box,
        case
            when waittime_uc is null then null
            when waittime_uc in ('Shorter than average',
                                        'Much shorter than average')
                then 1
            else 0
        end as waittime_uc_top_2_box,
        waittime_ed,
        case
        when waittime_ed is null then null
            when waittime_ed in ('Much longer than average',
                                        'Longer than average')
                then 1
            else 0
        end as waittime_ed_bottom_2_box,
        case
            when waittime_ed is null then null
            when waittime_ed in ('Shorter than average',
                                        'Much shorter than average')
                then 1
            else 0
        end as waittime_ed_top_2_box,
        case
            when chopsources___1 = 'Checked' then 1
            else 0
        end as chopsources_email,
        case
            when chopsources___2 = 'Checked' then 1
            else 0
        end as chopsources_website,
        case
            when chopsources___3 = 'Checked' then 1
            else 0
        end as chopsources_follow_socialmedia,
        case
            when chopsources___99 = 'Checked' then 1
            else 0
        end as chopsources_noneofthese,
        case
            when  chopsources_email = 1
                or chopsources_website = 1
                or chopsources_follow_socialmedia = 1
                or chopsources_noneofthese = 1 then 1
            else 0
        end as chopsources_reported,
        regexp_replace(emailfreq, '</?b>', '') as emailfreq_mod,
        regexp_replace(choprating_helpful, '<br>', '') as choprating_helpful_mod,
        case
            when choprating_helpful_mod is null then null
            when choprating_helpful_mod in ('Strongly agree',
                                        'Agree') then 1
            else 0
        end as choprating_helpful_top_2_box,
        case
            when choprating_helpful_mod is null then null
            when choprating_helpful_mod in ('Strongly disagree',
                                        'Disagree')
                then 1
            else 0
        end as choprating_helpful_bottom_2_box,
        regexp_replace(choprating_informed, '<br>', '') as choprating_informed_mod,
        case
            when choprating_informed_mod is null then null
            when choprating_informed_mod in ('Strongly agree',
                                        'Agree') then 1
            else 0
        end as choprating_informed_top_2_box,
        case
            when choprating_informed_mod is null then null
            when choprating_informed_mod in ('Strongly disagree',
                                        'Disagree')
                then 1
            else 0
        end as choprating_informed_bottom_2_box,
        questions,
        chop_family_feedback_survey_complete,
        upd_dt
    from
        {{source('marketing_ods', 'marketing_ci_sick_season_survey_fy24_wave1')}}
    union all
    select
        record_id,
        email,
        chop_family_feedback_survey_timestamp,
        starttime,
        case
            when lower(chop_family_feedback_survey_complete) = 'complete' then
            chop_family_feedback_survey_timestamp::timestamp
            else starttime
        end as survey_timestamp,
        case
            when extract(dow from starttime) != 1 then
                date_trunc('week', starttime) - 1
            else date(starttime)
        end as week_start ,
        case
            when extract(dow from starttime) != 1 then
                date_trunc('week', starttime) - 1 + 6
            else date(starttime) + 6
        end as week_end,
        month(starttime) as start_month,
        respondent,
        state,
        case
            when regexp_replace(state, '.*\((.*)\)', '\1') in ('PA', 'NJ', 'DE')
                then regexp_replace(state, '.*\((.*)\)', '\1')
            when state = 'Living outside of U.S.' then 'International'
            else 'Other/US Territory'
        end as state_abbr, --noqa: PRS,L048
        coalesce(county_pa, county_nj, county_de, county_blank) as county,
        case
            when county_de = 'New Castle County'
                or county_pa in ('Bucks County', 'Chester County',
                'Delaware County', 'Montgomery County', 'Philadelphia County' )
                or county_nj in ('Atlantic County', 'Burlington County',
                'Camden County', 'Cape May County', 'Cumberland County',
                'Gloucester County', 'Salem County') then 1
            when state is not null then 0
        end as chop_primary_service_area,
        case
            when county_pa in ('Bucks County', 'Chester County',
                'Delaware County', 'Montgomery County', 'Philadelphia County' ) then 1
            when state is not null then 0
        end as chop_primary_service_area_pa_only,
        case
            when county_nj in ('Atlantic County', 'Burlington County',
                'Camden County', 'Cape May County', 'Cumberland County',
                'Gloucester County', 'Salem County') then 1
            when state is not null then 0
        end as chop_primary_service_area_nj_only,
        case
            when childages___1 = 'Checked' then 1
            else 0
        end as under_age_1_ind,
        case
            when childages___2 = 'Checked' then 1
            else 0
        end as age_1_3_yrs_ind,
        case
            when childages___3 = 'Checked' then 1
            else 0
        end as age_4_5_yrs_ind,
        case
            when childages___4 = 'Checked' then 1
            else 0
        end as age_6_8_yrs_ind,
        case
            when childages___5 = 'Checked' then 1
            else 0
        end as age_9_12_yrs_ind,
        case
            when childages___6 = 'Checked' then 1
            else 0
        end as age_13_17_yrs_ind,
        case
            when under_age_1_ind = 1
                or age_1_3_yrs_ind = 1
                or age_4_5_yrs_ind = 1
                or age_6_8_yrs_ind = 1
                or age_9_12_yrs_ind = 1
                or age_13_17_yrs_ind = 1 then 1
            else 0
        end as childages_reported,
        case
            when age_13_17_yrs_ind = 1 then '13-17 yrs'
            when age_9_12_yrs_ind = 1 then '9-12 yrs'
            when age_6_8_yrs_ind = 1 then '6-8 yrs'
            when age_4_5_yrs_ind = 1 then '4-5 yrs'
            when age_1_3_yrs_ind = 1 then '1-3 yrs'
            when under_age_1_ind = 1 then '<1 yrs'
            else '>17 yrs'
        end  as oldest_child_age_group,
        case
            when age_13_17_yrs_ind = 1 then 6
            when age_9_12_yrs_ind = 1 then 5
            when age_6_8_yrs_ind = 1 then 4
            when age_4_5_yrs_ind = 1 then 3
            when age_1_3_yrs_ind = 1 then 2
            when under_age_1_ind = 1 then 1
            else 98
        end  as oldest_child_age_group_cat,
        case
            when chopcare___1 = 'Checked' then 1
            else 0
        end as chopcare_primary_care,
        case
            when chopcare___2 = 'Checked' then 1
            else 0
        end as chopcare_urgent_care,
        case
            when chopcare___3 = 'Checked' then 1
            else 0
        end as chopcare_op_specialty_care,
        case
            when chopcare___4 = 'Checked' then 1
            else 0
        end as chopcare_ip_hospital_care,
        case
            when chopcare___5 = 'Checked' then 1
            else 0
        end as chopcare_emergency_care,
        case
            when chopcare___98 = 'Checked' then 1
            else 0
        end as chopcare_notlisted,
        chopcare_other as chopcare_other,
        case
            when chopcare___99 = 'Checked' then 1
            else 0
        end as chopcare_noneofthese,
        case
            when chopcare_primary_care = 1
                or (chopcare_urgent_care = 1)
                or (chopcare_op_specialty_care = 1)
                or (chopcare_ip_hospital_care = 1)
                or (chopcare_emergency_care = 1)
                or (chopcare_notlisted = 1)
                or (chopcare_noneofthese = 1)
                or (chopcare_other is not null) then 1
            else 0
        end as chopcare_reported,
            case
            when chopcare_primary_care = 1 then 1
            when (chopcare_urgent_care = 1)
                or (chopcare_op_specialty_care = 1)
                or (chopcare_ip_hospital_care = 1)
                or (chopcare_emergency_care = 1)
                or (chopcare_notlisted = 1)
                or (chopcare_noneofthese = 1)
                or (chopcare_other is not null) then 0
        end as chop_primary_care,
        case
            when buzz___1 = 'Checked' then 1
            else 0
        end as buzz_flu,
        case
            when buzz___2 = 'Checked' then 1
            else 0
        end as buzz_covid19,
        case
            when buzz___3 = 'Checked' then 1
            else 0
        end as buzz_rsv,
        case
            when buzz___4 = 'Checked' then 1
            else 0
        end as buzz_rhino,
        case
            when buzz___5 = 'Checked' then 1
            else 0
        end as buzz_norovirus,
        case
            when buzz___6 = 'Checked' then 1
            else 0
        end as buzz_strep,
        case
            when buzz___7 = 'Checked' then 1
            else 0
        end as buzz_measles,
        case
            when buzz___8 = 'Checked' then 1
            else 0
        end as buzz_pneumonia,
        case
            when buzz___9 = 'Checked' then 1
            else 0
        end as buzz_conjunctivitis,
        case
            when buzz___10 = 'Checked' then 1
            else 0
        end as buzz_hand_foot_mouth_disease,
        case
            when buzz___98 = 'Checked' then 1
            else 0
        end as buzz_notlisted,
        buzz_other as buzz_otherillnesses,
        case
            when buzz___99 = 'Checked' then 1
            else 0
        end as buzz_noneofthese,
        case
            when buzz_flu = 1
                or (buzz_covid19 = 1)
                or (buzz_rsv = 1)
                or (buzz_rhino = 1)
                or (buzz_norovirus = 1)
                or (buzz_strep = 1)
                or (buzz_pneumonia = 1)
                or (buzz_conjunctivitis = 1)
                or (buzz_hand_foot_mouth_disease = 1)
                or (buzz_notlisted = 1)
                or (buzz_noneofthese = 1)
                or (buzz_other is not null) then 1
            else 0
        end as buzz_reported,
        buzz_flu + buzz_covid19 + buzz_rsv
        + buzz_rhino + buzz_norovirus + buzz_pneumonia
        + buzz_strep + buzz_notlisted + buzz_conjunctivitis
        + buzz_hand_foot_mouth_disease + case when
            (buzz_other is not null) and buzz_notlisted != 1 then 1
            else 0
        end as no_of_buzz_reported,

        case
            when buzzsources___1 = 'Checked' then 1
            else 0
        end as buzzsources_email,
        case
            when buzzsources___2 = 'Checked' then 1
            else 0
        end as buzzsources_socialmedia_chop,
        case
            when buzzsources___3 = 'Checked' then 1
            else 0
        end as buzzsources_child_docs_nurses,
        case
            when buzzsources___4 = 'Checked' then 1
            else 0
        end as buzzsources_school_daycare,
        case
            when buzzsources___5 = 'Checked' then 1
            else 0
        end as buzzsources_socialmedia_acquaintance,
        case
            when buzzsources___6 = 'Checked' then 1
            else 0
        end as buzzsources_news,
        case
            when buzzsources___7 = 'Checked' then 1
            else 0
        end as buzzsources_local_healthofficials,
        case
            when buzzsources___8 = 'Checked' then 1
            else 0
        end as buzzsources_acquaintance_conversation,
        case
            when buzzsources___98 = 'Checked' then 1
            else 0
        end as buzzsources_notlisted,
        buzzsources_other,
        case
            when buzzsources_email = 1
                or buzzsources_socialmedia_chop = 1
                or buzzsources_child_docs_nurses = 1
                or buzzsources_school_daycare = 1
                or buzzsources_socialmedia_acquaintance = 1
                or buzzsources_news = 1
                or buzzsources_local_healthofficials = 1
                or buzzsources_notlisted = 1
                or buzzsources_other is not null then 1
            else 0
        end as buzzsource_reported,
        buzzsources_email
                + buzzsources_socialmedia_chop
                + buzzsources_child_docs_nurses
                + buzzsources_school_daycare
                + buzzsources_socialmedia_acquaintance
                + buzzsources_news
                + buzzsources_local_healthofficials
                + buzzsources_notlisted
        + case
            when buzzsources_other is not null and buzzsources_notlisted != 1 then 1
            else 0
        end as no_of_buzzsource_reported,
        case
            when precautions___1 = 'Checked' then 1
            else 0
        end as precautions_employee_flu_vaccine,
        case
            when precautions___2 = 'Checked' then 1
            else 0
        end as precautions_employee_covid_vaccine,
        case
            when precautions___3 = 'Checked' then 1
            else 0
        end as precautions_employee_masking,
        case
            when precautions___4 = 'Checked' then 1
            else 0
        end as precautions_visitor_masking,
        case
            when precautions___5 = 'Checked' then 1
            else 0
        end as precautions_employee_daily_flu_covid_test,
        case
            when precautions___6 = 'Checked' then 1
            else 0
        end as precautions_enhanced_cleaning_sick_season,
        case
            when precautions___7 = 'Checked' then 1
            else 0
        end as precautions_patients_wait_in_car,
        case
            when precautions___8 = 'Checked' then 1
            else 0
        end as precautions_require_negative_covidtest_before_surgery,
        case
            when precautions_additional___98 = 'Checked' then 1
            else 0
        end as precautions_notlisted,
        precautions_additional_other,
        case
            when  precautions_additional___99 = 'Checked' then 1
            else 0
        end as precautions_noneofthese,
        case
            when precautions_employee_flu_vaccine = 1
                or precautions_employee_covid_vaccine = 1
                or precautions_employee_masking = 1
                or precautions_visitor_masking = 1
                or precautions_employee_daily_flu_covid_test = 1
                or precautions_enhanced_cleaning_sick_season = 1
                or precautions_patients_wait_in_car = 1
                or precautions_require_negative_covidtest_before_surgery = 1
                or precautions_notlisted = 1
                or precautions_noneofthese = 1
                or precautions_additional_other is not null then 1
            else 0
        end as precautions_reported,
        precautions_employee_flu_vaccine
        + precautions_employee_covid_vaccine
        + precautions_employee_masking
        + precautions_visitor_masking
        + precautions_employee_daily_flu_covid_test
        + precautions_enhanced_cleaning_sick_season
        + precautions_patients_wait_in_car
        + precautions_require_negative_covidtest_before_surgery
        + precautions_notlisted
        + precautions_noneofthese
        + case
            when precautions_additional_other is not null and precautions_notlisted != 1 then 1
            else 0
        end as no_of_precautions_reported,
        nosamedayappt as no_same_day_appt,
        preferreduc,
        preferreded,
        case
            when preferreded is null then null
            when preferreded
                in ('CHILDREN''S HOSPITAL OF PHILADELPHIA - MAIN HOSPITAL (PHILADELPHIA)', --noqa: PRS,L048
                    'CHILDREN''S HOSPITAL OF PHILADELPHIA - MIDDLEMAN FAMILY PAVILION (KING OF PRUSSIA)')  --noqa: PRS,L048
                then 1
            else 0
        end as chop_ed, --noqa: PRS,L048
        preferreduc_piping,
        preferreded_piping,
        sickseasonseverity,
        case
            when sickseasonseverity is null
            then null
        when sickseasonseverity in ('Severe',
                                        'Very Severe')
                then 1
            else 0
        end as sickseasonseverity_bottom_2_box,
        case
            when sickseasonseverity is null
            then null
            when sickseasonseverity in ('Very Mild',
                                        'Mild')
                then 1
            else 0
        end as sickseasonseverity_top_2_box,
        currentstate,
        case
            when currentstate is null
            then null
            when currentstate = 'The worst is behind us' then 1
            else 0
        end as currentstate_positive,
        case
            when currentstate is null
            then null
            when currentstate in ('We''re in the middle of the worst part', --noqa: PRS,L048
                                    'The worst is yet to come') then 1
            else 0
        end as currentstate_negative, --noqa: PRS,L048
        pcappt_sameday,
        case
        when pcappt_sameday is null
            then null
            when pcappt_sameday in ('Definitely could NOT get',
                                        'Probably could NOT get')
                then 1
            else 0
        end as pcappt_sameday_bottom_2_box,
        case
            when pcappt_sameday is null
            then null
            when pcappt_sameday in ('Probably could get',
                                        'Definitely could get')
                then 1
            else 0
        end as pcappt_sameday_top_2_box,
        pcappt_nextday,
        case
            when pcappt_nextday is null
            then null
            when pcappt_nextday in ('Definitely could NOT get',
                                        'Probably could NOT get')
                then 1
            else 0
        end as pcappt_nextday_bottom_2_box,
        case
            when pcappt_nextday is null
            then null
            when pcappt_nextday in ('Probably could get',
                                        'Definitely could get')
                then 1
            else 0
        end as pcappt_nextday_top_2_box,
        waittime_uc,
        case
            when waittime_uc is null then null
            when waittime_uc in ('Much longer than average',
                                        'Longer than average')
                then 1
            else 0
        end as waittime_uc_bottom_2_box,
        case
            when waittime_uc is null then null
            when waittime_uc in ('Shorter than average',
                                        'Much shorter than average')
                then 1
            else 0
        end as waittime_uc_top_2_box,
        waittime_ed,
        case
        when waittime_ed is null then null
            when waittime_ed in ('Much longer than average',
                                        'Longer than average')
                then 1
            else 0
        end as waittime_ed_bottom_2_box,
        case
            when waittime_ed is null then null
            when waittime_ed in ('Shorter than average',
                                        'Much shorter than average')
                then 1
            else 0
        end as waittime_ed_top_2_box,
        case
            when chopsources___1 = 'Checked' then 1
            else 0
        end as chopsources_email,
        case
            when chopsources___2 = 'Checked' then 1
            else 0
        end as chopsources_website,
        case
            when chopsources___3 = 'Checked' then 1
            else 0
        end as chopsources_follow_socialmedia,
        case
            when chopsources___99 = 'Checked' then 1
            else 0
        end as chopsources_noneofthese,
        case
            when  chopsources_email = 1
                or chopsources_website = 1
                or chopsources_follow_socialmedia = 1
                or chopsources_noneofthese = 1 then 1
            else 0
        end as chopsources_reported,
        regexp_replace(emailfreq, '</?b>', '') as emailfreq_mod,
        regexp_replace(choprating_helpful, '<br>', '') as choprating_helpful_mod,
        case
            when choprating_helpful_mod is null then null
            when choprating_helpful_mod in ('Strongly agree',
                                        'Agree') then 1
            else 0
        end as choprating_helpful_top_2_box,
        case
            when choprating_helpful_mod is null then null
            when choprating_helpful_mod in ('Strongly disagree',
                                        'Disagree')
                then 1
            else 0
        end as choprating_helpful_bottom_2_box,
        regexp_replace(choprating_informed, '<br>', '') as choprating_informed_mod,
        case
            when choprating_informed_mod is null then null
            when choprating_informed_mod in ('Strongly agree',
                                        'Agree') then 1
            else 0
        end as choprating_informed_top_2_box,
        case
            when choprating_informed_mod is null then null
            when choprating_informed_mod in ('Strongly disagree',
                                        'Disagree')
                then 1
            else 0
        end as choprating_informed_bottom_2_box,
        questions,
        chop_family_feedback_survey_complete,
        upd_dt
    from
        {{source('marketing_ods', 'marketing_ci_sick_season_survey_fy24_q3')}}
)
select
    {{
    dbt_utils.surrogate_key([
        'record_id',
	'email',
        'starttime'
        ])
    }} as primary_key,
    *
from
   marketing_ci_sick_season_survey_fy24
where respondent = 'Yes'
