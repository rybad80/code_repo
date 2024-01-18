with pivoted as (
    select
        {{ dbt_utils.pivot(
      'field_nm',
      dbt_utils.get_column_values(
        table=ref('stg_hand_hygiene'),
        column='field_nm',
        default='default_column_value'),
      agg = "max",
      then_value = 'RESPONSE',
      else_value = 'NULL',
      quote_identifiers=False
        )}}
    from
        {{ref('stg_hand_hygiene')}} as stg_hand_hygiene
    where
        project_id = 695 --Hand Hygiene IP / Hand Hygiene Program
    group by
        stg_hand_hygiene.record
)

select
    participant_id,
    date as rec_dt,
    unittype as unit_type,
    coalesce(campus, 'Philadelphia Campus') as campus,
    coalesce(ip, kop_ip) as unit,
    coalesce(nicubreak, picubreak) as subunit,
    room,
    bed,
    observertype as observer_type,
    time,
    caregiverobserved as caregiver_observed,
    apptype as app_type,
    mdtype as md_type,
    cgother as caregiver_observed_other,
    beforetouch as hh_before_touch_pat,
    promptbeforetouch as hh_before_touch_pat_prompt,
    beforetouchmissedtheme as hh_before_touch_missed_theme,
    beforetouchindic as hh_before_touch_pat_type,
    beforecleanaseptic as hh_before_clean_proc,
    promptbeforecleanaseptic as hh_before_clean_proc_prompt,
    beforecleanmissedtheme as hh_before_clean_missed_theme,
    beforecleanascepindicator as hh_before_clean_proc_type,
    afterbodyfluid as hh_after_body_fluid,
    promptafterbodyfluid as hh_after_body_fluid_prompt,
    afterbodymissedtheme as hh_after_body_missed_theme,
    afterbodyfluidindicator as hh_after_body_fluid_type,
    aftertouchpt as hh_after_touch_pat,
    promptaftertouch as hh_after_touch_pat_prompt,
    aftertouchmissedtheme as hh_after_touch_missed_theme,
    aftertouchptindicator as hh_after_touch_pat_type,
    aftersurroun as hh_after_surround,
    promptaftersurroun as hh_after_surround_prompt,
    missedtheme as hh_after_surround_missed_theme,
    aftertouchsurroundindic as hh_after_surround_type,
    feedbackgiven as feedback_given,
    survey_complete as complete_ind
from pivoted
