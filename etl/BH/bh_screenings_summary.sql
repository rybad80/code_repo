with
stg_bh as (
    select
    visit_key,
        max(case when bh_screening_question_id = '109679' and bh_screening_answer  >= 11  then 1
                when (bh_screening_question_id = '109679') then 0
            end) as phq_9_total_pos_ind,
        max(case when bh_screening_question_id in ('109728', '109729', '109730')
                    and bh_screening_answer  > 0 then 1
                 when bh_screening_question_id in ('109728', '109729', '109730') then 0
            end) as phq_9_suicide_risk_ind,
        max(case when bh_screening_question_id = '126237' and bh_screening_answer  >= 10  then 1
                 when bh_screening_question_id = '126237' then 0
            end) as phq_8_total_pos_ind,
        max(case when bh_screening_question_id in ('109735', '119562') and bh_screening_answer  >= 3 then 1
                 when bh_screening_question_id in ('109735', '119562') then 0
            end) as mchat_any_3_ind,
        max(case when bh_screening_question_id in ('118554', '118553') and bh_screening_answer  >= 6  then 1
                 when bh_screening_question_id in ('118554', '118553') then 0
            end) as nichq_pos_ind,
        max(case when bh_screening_question_id > 5 and screening_type = 'nichq_flowsheets' then 1 else 0
            end) as nichq_fs_pos_ind
    from
        {{ref('stg_bh_screenings')}}
    group by visit_key
),


cssrs_smart_forms_base as (
select
    visit_key,
    cssrs_entered_date,
coalesce(si_q1_slv, 0) as si_q1_slv,
coalesce(si_q2_slv, 0) as si_q2_slv,
coalesce(si_q3_slv, 0) as si_q3_slv,
coalesce(si_q4_slv, 0) as si_q4_slv,
coalesce(si_q5_slv, 0) as si_q5_slv,
coalesce(sb_q1_slv, 0) as sb_q1_slv,
coalesce(sb_q1_slv_injury, 0) as sb_q1_slv_injury,
coalesce(sb_q1_slv_nssi, 0) as sb_q1_slv_nssi,
coalesce(sb_q2_slv, 0) as sb_q2_slv,
coalesce(sb_q3_slv, 0) as sb_q3_slv,
coalesce(sb_q4_slv, 0) as sb_q4_slv,
slv_given_ind,
coalesce(si_q1_lifetime, 0) as si_q1_lifetime,
coalesce(si_q1_past_month, 0) as si_q1_past_month,
coalesce(si_q2_lifetime, 0) as si_q2_lifetime,
coalesce(si_q2_past_month, 0) as si_q2_past_month,
coalesce(si_q3_lifetime, 0) as si_q3_lifetime,
coalesce(si_q3_past_month, 0) as si_q3_past_month,
coalesce(si_q4_lifetime, 0) as si_q4_lifetime,
coalesce(si_q4_past_month, 0) as si_q4_past_month,
coalesce(si_q5_lifetime, 0) as si_q5_lifetime,
coalesce(si_q5_past_month, 0) as si_q5_past_month,
coalesce(sb_q1_lifetime, 0) as sb_q1_lifetime,
coalesce(sb_q1_past_3_months, 0) as sb_q1_past_3_months,
coalesce(sb_q2_lifetime, 0) as sb_q2_lifetime,
coalesce(sb_q2_past_3_months, 0) as sb_q2_past_3_months,
coalesce(sb_q3_lifetime, 0) as sb_q3_lifetime,
coalesce(sb_q3_past_3_months, 0) as sb_q3_past_3_months,
coalesce(sb_q4_lifetime, 0) as sb_q4_lifetime,
coalesce(sb_q4_past_3_months, 0) as sb_q4_past_3_months,
coalesce(sb_q1_nssi_lifetime, 0) as sb_q1_nssi_lifetime,
coalesce(sb_q1_injury_lifetime, 0) as sb_q1_injury_lifetime,
coalesce(sb_q1_nssi_past_3_months, 0) as sb_q1_nssi_past_3_months,
coalesce(sb_q1_injury_past_3_months, 0) as sb_q1_injury_past_3_months,
lifetime_month_given_ind,
coalesce(qs_q1, 0) as qs_q1,
coalesce(qs_q2, 0) as qs_q2,
coalesce(qs_q3, 0) as qs_q3,
qs_given_ind,
coalesce(cssrs_declined_ind, 0) as cssrs_declined_ind,
coalesce(cssrs_noncompliant_ind, 0) as cssrs_noncompliant_ind
from {{ref('cssrs_survey')}}
),


cssrs_smart_forms as (
select
    visit_key,
    cssrs_entered_date,
    case when
        si_q3_lifetime + si_q4_lifetime + si_q5_lifetime >= 1 then 1 else 0
    end as lifetime_si_ind,
    case when
        si_q1_past_month + si_q2_past_month + si_q3_past_month + si_q4_past_month >= 1 then 1 else 0
    end as past_month_si_ind,
    case when
        sb_q1_lifetime + sb_q2_lifetime + sb_q3_lifetime + sb_q4_lifetime >= 1 then 1 else 0
    end as lifetime_sb_ind,
    case when
        sb_q1_past_3_months + sb_q2_past_3_months + sb_q3_past_3_months + sb_q4_past_3_months
        + sb_q1_nssi_past_3_months  + sb_q1_injury_past_3_months >= 1 then 1 else 0
    end as past_3_months_sb_ind,
    case when
        qs_q2 + qs_q3 >= 1 then 1 else 0
    end as cssrs_quick_screen_pos_ind,
    case when
        si_q2_slv + si_q3_slv + si_q4_slv + si_q5_slv >= 1 then 1 else 0
    end as since_last_visit_si_ind,
    case when
        sb_q1_slv + sb_q2_slv + sb_q3_slv + sb_q4_slv >= 1 then 1 else 0
    end as since_last_visit_sb_ind,
    case when si_q2_slv + si_q3_slv + si_q4_slv + si_q5_slv + si_q2_past_month + si_q3_lifetime
        + si_q3_past_month + si_q4_lifetime + si_q4_past_month + si_q5_lifetime + si_q5_past_month
        + sb_q1_lifetime + sb_q1_past_3_months + sb_q2_lifetime + sb_q3_lifetime + sb_q2_past_3_months
        + sb_q3_past_3_months + sb_q4_lifetime + sb_q4_past_3_months + sb_q1_slv + sb_q2_slv
        + sb_q3_slv + sb_q4_slv  >= 1 then 1 else 0
    end as cssrs_qi_positive_ind,
    cssrs_declined_ind,
    cssrs_noncompliant_ind,
    slv_given_ind,
    lifetime_month_given_ind,
    qs_given_ind
    from cssrs_smart_forms_base
),
-- end region

screenings_visits_combined as (
select
    visit_key
from
    stg_bh
union
select
    visit_key
from
    cssrs_smart_forms
),

-- region Combines Questionnaires with Flowsheet and Smart Form Results
bh_screenings as (
    select
        screenings_visits_combined.visit_key,
        stg_bh.phq_9_total_pos_ind,
        stg_bh.phq_9_suicide_risk_ind,
        stg_bh.phq_8_total_pos_ind,
        stg_bh.mchat_any_3_ind,
        stg_bh.nichq_pos_ind,
        stg_bh.nichq_fs_pos_ind,
        cssrs_smart_forms.cssrs_entered_date,
        cssrs_smart_forms.lifetime_si_ind,
        cssrs_smart_forms.past_month_si_ind,
        cssrs_smart_forms.lifetime_sb_ind,
        cssrs_smart_forms.past_3_months_sb_ind,
        cssrs_smart_forms.cssrs_quick_screen_pos_ind,
        cssrs_smart_forms.cssrs_qi_positive_ind,
        cssrs_smart_forms.since_last_visit_si_ind,
        cssrs_smart_forms.since_last_visit_sb_ind,
        cssrs_smart_forms.cssrs_declined_ind,
        cssrs_smart_forms.cssrs_noncompliant_ind,
        cssrs_smart_forms.slv_given_ind,
        cssrs_smart_forms.lifetime_month_given_ind,
        cssrs_smart_forms.qs_given_ind,
        case when
            phq_9_total_pos_ind = 1
            or phq_9_suicide_risk_ind = 1
            or phq_8_total_pos_ind = 1
            or mchat_any_3_ind = 1
            or nichq_pos_ind = 1
            or nichq_fs_pos_ind = 1
            or lifetime_si_ind = 1
            or past_month_si_ind = 1
            or lifetime_sb_ind = 1
            or past_3_months_sb_ind = 1
            or cssrs_quick_screen_pos_ind = 1
            or since_last_visit_si_ind = 1
            or since_last_visit_sb_ind = 1
            then 1 else 0 end as bh_screened_pos_any_ind
    from
    screenings_visits_combined
    left join
        stg_bh on stg_bh.visit_key = screenings_visits_combined.visit_key
    left join
        cssrs_smart_forms on cssrs_smart_forms.visit_key = screenings_visits_combined.visit_key
)


select
    bh_screenings.visit_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    bh_screenings.cssrs_entered_date,
    bh_screenings.phq_9_total_pos_ind,
    bh_screenings.phq_9_suicide_risk_ind,
    bh_screenings.phq_8_total_pos_ind,
    bh_screenings.mchat_any_3_ind,
    bh_screenings.nichq_pos_ind,
    bh_screenings.nichq_fs_pos_ind,
    bh_screenings.lifetime_si_ind,
    bh_screenings.past_month_si_ind,
    bh_screenings.lifetime_sb_ind,
    bh_screenings.past_3_months_sb_ind,
    bh_screenings.cssrs_quick_screen_pos_ind,
    bh_screenings.since_last_visit_si_ind,
    bh_screenings.since_last_visit_sb_ind,
    bh_screenings.cssrs_declined_ind,
    bh_screenings.cssrs_noncompliant_ind,
    bh_screenings.bh_screened_pos_any_ind,
    bh_screenings.cssrs_qi_positive_ind,
    bh_screenings.slv_given_ind,
    bh_screenings.lifetime_month_given_ind,
    bh_screenings.qs_given_ind,
    stg_encounter.pat_key
from
    bh_screenings
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = bh_screenings.visit_key
