/* this model identifies the hourly risk of cardiac arrest
-- for cicu patients based on the epic acuity rule score.
-- patients who are flagged as high risk require a huddle
-- after the flag is triggered
*/

with epic_flag_data as (
    select
        registry_quality_info.pat_key,
        to_timestamp(
            timezone(acuity_rule_score.score_calc_utc_dttm, 'UTC', 'America/New_York'), 'YYYY-MM-DD HH:MI:SS'
        ) as score_calc_time,
        date_trunc('hour', to_timestamp(
            timezone(acuity_rule_score.score_calc_utc_dttm, 'UTC', 'America/New_York'), 'YYYY-MM-DD HH:MI:SS')
        ) as time_mark,
        master_charge_edit_rule.display_nm,
        master_charge_edit_rule.rule_id,
        acuity_rule_score.rule_score
    from
        {{source('clarity_ods', 'acuity_rule_score')}} as acuity_rule_score
        inner join {{source('cdw', 'registry_quality_info')}} as registry_quality_info
            on acuity_rule_score.registry_data_id = registry_quality_info.registry_data_id
        inner join {{source('cdw', 'master_charge_edit_rule')}} as master_charge_edit_rule
            on acuity_rule_score.rule_id = master_charge_edit_rule.rule_id

    where
        master_charge_edit_rule.rule_id in (
            '1024152', --'current bipap or airway lda in past 4h'
            '1022995', --'myocarditis or cardiomyopathy diagnosis'
            '1022994', --'myocarditis/ cardiomyopathy & invasive vent'
            '1023683', --'narrow pulse pressure (<15 in previous 2h)'
            '1024091', --'neo: single ventricle w/o fontan or glenn'
            '1024482', --'neo: 48 hrs post-cardiac surgery'
            '1024830', --'neo: 48 hrs post-op with periop ino'
            '1023484', --'non-neo: ph < 7.2'
            '1022998', --'non-neo: neuromuscular blockade'
            '1022993', --'non-neo: invasive vent & fi02>50%'
            '1023686', --'non-neo: chest tube out > 5ml/k/hr'
            '1024832', --'non-neo: base decrease >=5 (4h or 2 consecutive)'
            '1026074', --'non-neo: potassium > 6'
            '1024747', --'ecmo or vad'
            '1379234' --'recent transfer from ccu'
        )
),

flags_pre1 as (

    select
        cardiac_arrest_cohort_hourly.pat_key,
        cardiac_arrest_cohort_hourly.time_mark,
        -- score component values       
        max(
            case
                when epic_flag_data.rule_id = '1024747' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as ecmo_vad,
        max(
            case
                when epic_flag_data.rule_id = '1024482' and epic_flag_data.rule_score > 0
                    then epic_flag_data.rule_score else 0 end
        ) as surg,
        max(
            case when epic_flag_data.rule_id = '1024091' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as stage1,
        max(
            case when epic_flag_data.rule_id = '1022995' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as myo_cardio,
        max(
            case when epic_flag_data.rule_id = '1023484' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as ph_lt7,
        max(
            case when epic_flag_data.rule_id = '1024832' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as base_neg5,
        max(
            case when epic_flag_data.rule_id = '1024830' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as periop_nitric,
        max(
            case when epic_flag_data.rule_id = '1379234' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as readmit,
        max(
            case when epic_flag_data.rule_id = '1024152' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as esc_resp,
        max(
            case when epic_flag_data.rule_id = '1022994' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as myocardio_inv_vent_4hr,
        max(
            case when epic_flag_data.rule_id = '1022998' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as musc_block,
        max(
            case when epic_flag_data.rule_id = '1023683' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as narrow,
        max(
            case when epic_flag_data.rule_id = '1023686' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as drain_gt5,
        max(
            case when epic_flag_data.rule_id = '1026074' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as pot_gt6,
        max(
            case when epic_flag_data.rule_id = '1022993' and epic_flag_data.rule_score > 0
                then epic_flag_data.rule_score else 0 end
        ) as fio2_gt50,
        -- component indicators
        max(
            case when epic_flag_data.rule_id = '1024747' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as ecmo_vad_ind,
        max(
            case when epic_flag_data.rule_id = '1024482' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as surg_ind,
        max(
            case when epic_flag_data.rule_id = '1024091' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as stage1_ind,
        max(
            case when epic_flag_data.rule_id = '1022995' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as myo_cardio_ind,
        max(
            case when epic_flag_data.rule_id = '1023484' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as ph_lt7_ind,
        max(
            case when epic_flag_data.rule_id = '1024832' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as base_neg5_ind,
        max(
            case when epic_flag_data.rule_id = '1024830' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as periop_nitric_ind,
        max(
            case when epic_flag_data.rule_id = '1379234' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as readmit_ind,
        max(
            case when epic_flag_data.rule_id = '1024152' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as esc_resp_ind,
        max(
            case when epic_flag_data.rule_id = '1022994' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as myocardio_inv_vent_4hr_ind,
        max(
            case when epic_flag_data.rule_id = '1022998' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as musc_block_ind,
        max(
            case when epic_flag_data.rule_id = '1023683' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as narrow_ind,
        max(
            case when epic_flag_data.rule_id = '1023686' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as drain_gt5_ind,
        max(
            case when epic_flag_data.rule_id = '1026074' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as pot_gt6_ind,
        max(
            case when epic_flag_data.rule_id = '1022993' and epic_flag_data.rule_score > 0
                then 1 else 0 end
        ) as fio2_gt50_ind

    from
        {{ref('cardiac_arrest_cohort_hourly')}} as cardiac_arrest_cohort_hourly
        left join epic_flag_data
            on cardiac_arrest_cohort_hourly.pat_key = epic_flag_data.pat_key
                and cardiac_arrest_cohort_hourly.time_mark = epic_flag_data.time_mark
    where
        minute(cardiac_arrest_cohort_hourly.time_mark) = 0
        and second(cardiac_arrest_cohort_hourly.time_mark) = 0
    group by
        cardiac_arrest_cohort_hourly.pat_key,
        cardiac_arrest_cohort_hourly.time_mark
),

flags_pre as (

    select
        flags_pre1.pat_key,
        flags_pre1.time_mark,
        flags_pre1.ecmo_vad_ind,
        flags_pre1.surg_ind,
        flags_pre1.stage1_ind,
        flags_pre1.myo_cardio_ind,
        flags_pre1.ph_lt7_ind,
        flags_pre1.base_neg5_ind,
        flags_pre1.periop_nitric_ind,
        flags_pre1.readmit_ind,
        flags_pre1.esc_resp_ind,
        flags_pre1.myocardio_inv_vent_4hr_ind,
        flags_pre1.musc_block_ind,
        flags_pre1.narrow_ind,
        flags_pre1.drain_gt5_ind,
        flags_pre1.pot_gt6_ind,
        flags_pre1.fio2_gt50_ind,
        (flags_pre1.ecmo_vad + flags_pre1.surg + flags_pre1.stage1
            + flags_pre1.myo_cardio + flags_pre1.ph_lt7
            + flags_pre1.base_neg5 + flags_pre1.periop_nitric
            + flags_pre1.readmit + flags_pre1.esc_resp
            + flags_pre1.myocardio_inv_vent_4hr + flags_pre1.musc_block
            + flags_pre1.narrow + flags_pre1.drain_gt5
            + flags_pre1.pot_gt6 + flags_pre1.fio2_gt50) as score_raw,
        case when score_raw < 0.5 -- noqa: L028
                then 2
            when score_raw >= 20 -- noqa: L028
                then 3
            when score_raw < 2.0 -- noqa: L028
                then 4
            when score_raw >= 2.0 -- noqa: L028
                then 5
            else null end as risk_cat
    from
        flags_pre1
),

first_flag as (
    select
        flags_pre.pat_key,
        flags_pre.time_mark,
        cardiac_arrest_cohort_hourly.cicu_enc_key,
        max(flags_pre.risk_cat) over(partition by cardiac_arrest_cohort_hourly.cicu_enc_key
            order by flags_pre.time_mark rows between
            4 preceding and 1 preceding) as max_risk_4_hr,
        case when (max_risk_4_hr != 5 or max_risk_4_hr is null)
                and flags_pre.risk_cat = 5
                then 1
            else 0 end as first_new_flag_ind
    from
        flags_pre
        inner join {{ref('cardiac_arrest_cohort_hourly')}} as cardiac_arrest_cohort_hourly
            on cardiac_arrest_cohort_hourly.pat_key = flags_pre.pat_key
            and cardiac_arrest_cohort_hourly.time_mark = flags_pre.time_mark
    group by
        flags_pre.pat_key,
        flags_pre.time_mark,
        flags_pre.risk_cat,
        cardiac_arrest_cohort_hourly.cicu_enc_key
),

prev_flag as (

    select
        first_flag.pat_key,
        first_flag.time_mark,
        first_flag.cicu_enc_key,
        first_flag.first_new_flag_ind,
        lag(first_flag.first_new_flag_ind) over(
            partition by first_flag.cicu_enc_key
            order by first_flag.time_mark
        ) as prev_flag_new_ind
    from
        first_flag
),

huddles as (

    select
        flags_pre.pat_key,
        flags_pre.time_mark,
        max(case
                when cardiac_arrest_huddle.huddle_date >= flags_pre.time_mark - cast('2 hours' as interval)
                and cardiac_arrest_huddle.huddle_date <= flags_pre.time_mark + cast('4 hours' as interval)
                then 1
            else 0 end) as huddle_4hr_ind,
        max(case when cardiac_arrest_cohort_hourly.next_arrest_date is not null
                and cardiac_arrest_huddle.huddle_date <= cardiac_arrest_cohort_hourly.next_arrest_date
                    and cardiac_arrest_huddle.huddle_date
                        >= cardiac_arrest_cohort_hourly.next_arrest_date - cast('24 hours' as interval)
                then 1
            else 0 end
        ) as arrest_huddle_24hr_ind
    from
        flags_pre
        left join {{ref('cardiac_arrest_cohort_hourly')}} as cardiac_arrest_cohort_hourly
            on cardiac_arrest_cohort_hourly.pat_key = flags_pre.pat_key
                and cardiac_arrest_cohort_hourly.time_mark = flags_pre.time_mark
        left join {{ref('cardiac_arrest_huddle')}} as cardiac_arrest_huddle
            on cardiac_arrest_huddle.pat_key = flags_pre.pat_key

    group by
        flags_pre.pat_key,
        flags_pre.time_mark
)

select
    cardiac_arrest_cohort_hourly.visit_key,
    cardiac_arrest_cohort_hourly.pat_key,
    cardiac_arrest_cohort_hourly.mrn,
    patient.full_nm,
    cardiac_arrest_cohort_hourly.hospital_admit_date,
    cardiac_arrest_cohort_hourly.hospital_discharge_date,
    cardiac_arrest_cohort_hourly.in_date,
    cardiac_arrest_cohort_hourly.out_date,
    cardiac_arrest_cohort_hourly.cicu_enc_key,
    cardiac_arrest_cohort_hourly.arrest_ind,
    flags_pre.time_mark,
    cardiac_arrest_cohort_hourly.time_mark_key,
    cardiac_arrest_cohort_hourly.age_days,
    cardiac_arrest_cohort_hourly.lt_30days_ind,
    cardiac_arrest_cohort_hourly.next_arrest_date,
    cardiac_arrest_cohort_hourly.hrs_to_arrest,
    cardiac_arrest_cohort_hourly.hrs_to_arrest_neg,
    flags_pre.ecmo_vad_ind,
    flags_pre.surg_ind,
    flags_pre.stage1_ind,
    flags_pre.myo_cardio_ind,
    flags_pre.ph_lt7_ind,
    flags_pre.base_neg5_ind,
    flags_pre.periop_nitric_ind,
    flags_pre.readmit_ind,
    flags_pre.esc_resp_ind,
    flags_pre.myocardio_inv_vent_4hr_ind,
    flags_pre.musc_block_ind,
    flags_pre.narrow_ind,
    flags_pre.drain_gt5_ind,
    flags_pre.pot_gt6_ind,
    flags_pre.fio2_gt50_ind,
    flags_pre.score_raw,
    flags_pre.risk_cat,
    first_flag.first_new_flag_ind,
    prev_flag.prev_flag_new_ind,
    case when flags_pre.risk_cat = 5
            and prev_flag.prev_flag_new_ind = 1
            then 1
        else 0 end as second_new_flag_ind,
    huddles.huddle_4hr_ind,
    huddles.arrest_huddle_24hr_ind,
    case when second_new_flag_ind = 1 -- L027
        and huddles.huddle_4hr_ind = 1
        then 1
        when second_new_flag_ind = 1 -- L027
            and huddles.huddle_4hr_ind = 0
            then 0
        else null end as flag_compliance_ind

from
    flags_pre
    inner join {{ref('cardiac_arrest_cohort_hourly')}} as cardiac_arrest_cohort_hourly
        on cardiac_arrest_cohort_hourly.pat_key = flags_pre.pat_key
            and cardiac_arrest_cohort_hourly.time_mark = flags_pre.time_mark
    left join huddles
        on huddles.pat_key = flags_pre.pat_key
            and huddles.time_mark = flags_pre.time_mark
    left join first_flag
        on first_flag.pat_key = flags_pre.pat_key
            and first_flag.time_mark = flags_pre.time_mark
    left join prev_flag
        on prev_flag.pat_key = flags_pre.pat_key
            and prev_flag.time_mark = flags_pre.time_mark
    inner join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = cardiac_arrest_cohort_hourly.pat_key
