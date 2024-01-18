with pivoted as (
    select
        stg_hand_hygiene.project_id,
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
        project_id in (
            659, --Hand Hygiene OP / Hand Hygiene Tool-Ambulatory
            895 --Hand Hygiene Program - Ambulatory GBP
        )
    group by
        stg_hand_hygiene.record,
        stg_hand_hygiene.project_id
)

--data from old REDCap
select
    project_id,
    participant_id,
    date as rec_dt,
    unittype as unit_type,
    op as unit,
    observertype as observer_type,
    caregiverobservedop as caregiver_observed,
    cgother as caregiver_observed_other,
    beforetouchop as hh_before_touch_pat,
    beforetouchindicop as hh_before_touch_pat_type,
    beforecleanasepticop as hh_before_clean_proc,
    beforecleanascepindicop as hh_before_clean_proc_type,
    afterbodyfluidop as hh_after_body_fluid,
    afterbodyfluidindicatop as hh_after_body_fluid_type,
    aftertouchptop as hh_after_touch_pat,
    aftertouchptindicatorop as hh_after_touch_pat_type,
    feedbackgiven as feedback_given,
    survey_complete as complete_ind
from
    pivoted
where
    project_id = 659

union all

--data from new REDCap
--How do null fields affect QS?
select
    project_id,
    participant_id,
    date as rec_dt,
    'Outpatient' as unit_type,
    op as unit,
    obs_type as observer_type,
    caregiverobservedop as caregiver_observed, --different than original names + list
    cgother as caregiver_observed_other,
    case when beforetouchop = 'No (Missed)' then 'Missed'
        when beforetouchop = 'Not Observed' then 'NA/Not observed'
        else 'Yes' end as hh_before_touch_pat,
    beforetouchindicop as hh_before_touch_pat_type, --null; field does not exist in new RC
    case when yesobs_op in ('HR', 'HW') then 'Yes'
        when yesobs_op = 'Missed' then 'Missed'
        else 'NA/Not observed' end as hh_before_clean_proc,
    case when yesobs_op in ('HR', 'HW')
        then yesobs_op end as hh_before_clean_proc_type,
    case when yesobs_op_2 in ('HR', 'HW') then 'Yes'
        when yesobs_op_2 = 'Missed' then 'Missed'
        else 'NA/Not observed' end as hh_after_body_fluid,
     case when yesobs_op_2 in ('HR', 'HW')
        then yesobs_op_2 end as hh_after_body_fluid_type,
    case when aftertouchop_2 = 'No (Missed)' then 'Missed'
        when aftertouchop_2 = 'Not Observed' then 'NA/Not observed'
        else 'Yes' end as hh_after_touch_pat,
    aftertouchptindicatorop as hh_after_touch_pat_type, --null; field does not exist in new RC
    feedbackgiven as feedback_given, --null; field does not exist in new RC
    survey_complete as complete_ind
from
    pivoted
where
    project_id = 895
