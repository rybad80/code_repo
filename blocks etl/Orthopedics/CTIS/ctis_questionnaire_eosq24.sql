{{ config(meta = {
    'critical': false
}) }}

/* column titles need to be shortened. */
{%- set value_lookup = [
    ('118634', 'score_health'),
    ('118635', 'score_pain'),
    ('118636', 'score_pulmonary'),
    ('118637', 'score_access'),
    ('118638', 'score_physical_functioning'),
    ('118639', 'score_daily_living'),
    ('118640', 'score_fatigue'),
    ('118641', 'score_emotion'),
    ('118642', 'score_parent_impact'),
    ('118643', 'score_financial_impact'),
    ('118644', 'score_patient_satisfaction'),
    ('118646', 'score_parent_satisfaction')
] %}

with
    questions as (
        select
            visit_key,
            question_answer_id,
            min(answer_date) as answer_date, --noqa: L008
        {%- for value_id, value_name in value_lookup %}
                max(
                    case
                        when
                            form_question_id = '{{ value_id }}'
                            then answer_as_numeric
                    end
                ) as {{ value_name }}{{ "," if not loop.last }}
        {%- endfor %}
        from
            {{ ref('question_patient_answered') }}
        where
            form_id = '101043' --'ortho eosq-24 (main) questionnaire (welcome/mychop)'
            and form_question_id in ( --domain questions
                {%- for value_id, _ in value_lookup %}
                    '{{ value_id }}'{{ "," if not loop.last }}
                {%- endfor %}
            )
        group by
            visit_key,
            question_answer_id
)

 select
    questions.question_answer_id,
    ctis_registry.mrn,
    ctis_registry.patient_name,
    stg_encounter.age_years,
    stg_encounter.encounter_date,
    row_number() over(
        partition by ctis_registry.pat_key order by questions.answer_date --noqa: L008
    ) as survey_seq_num,
    {%- for value_id, value_name in value_lookup %}
        questions.{{ value_name }}, --noqa: L008
    {%- endfor %}
    questions.visit_key,
    ctis_registry.pat_key
 from
    questions
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.visit_key = questions.visit_key
    inner join {{ ref('ctis_registry') }} as ctis_registry
        on ctis_registry.pat_key = stg_encounter.pat_key
