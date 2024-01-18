-- Combinining both sources of positives (diagnoses and labs) with isolation data
with dx_and_labs as ( -- noqa: PRS
    select
        stg_bioresponse_labs_episode.patient_key,
        stg_bioresponse_labs_episode.diagnosis_hierarchy_1,
        stg_bioresponse_labs_episode.first_pos_date as pos_start_date,
        stg_bioresponse_labs_episode.estimated_end_date as neg_date,
        'labs' as positive_source
    from
        {{ ref('stg_bioresponse_labs_episode') }} as stg_bioresponse_labs_episode
    union all
    select
        bioresponse_diagnosis_encounter.patient_key,
        bioresponse_diagnosis_encounter.diagnosis_hierarchy_1,
        bioresponse_diagnosis_encounter.problem_noted_date as pos_start_date,
        bioresponse_diagnosis_encounter.estimated_end_date as neg_date,
        'problem list' as positive_source
    from
        {{ ref('bioresponse_diagnosis_encounter') }} as bioresponse_diagnosis_encounter
    where
        bioresponse_diagnosis_encounter.problem_noted_date is not null
),

add_isolation as (
    select
        dx_and_labs.patient_key,
        dx_and_labs.positive_source,
        case when enter_date is not null then 1 else 0 end as isolation_ind,
        dx_and_labs.diagnosis_hierarchy_1,
        case when
            -- if patient was not isolated
            -- of if they were noted positive before isolation, then lab/dx date
            enter_date is null or pos_start_date < enter_date then pos_start_date
            -- otherwise, we're assuming isolation as the start of the positives
            else enter_date
        end as positive_start,
        case when
            -- assuming a patient would not be off precautions unless they are negative
            exit_date is not null then exit_date
            -- if pt positive more then the max expected time, then give them a new negative date
            when positive_start::date + max_infectious_window < neg_date::date
            then positive_start + normal_infectious_window
            else neg_date
            -- patient may end up with multiple rows for end dates--depending on isolation, dx, and lab dates
        end as estimated_end_date,
        lag(positive_start) over (
            partition by
                dx_and_labs.patient_key,
                dx_and_labs.diagnosis_hierarchy_1
            order by positive_start
        ) as prior_positive_start,
        lead(positive_start) over (
            partition by
                dx_and_labs.patient_key,
                dx_and_labs.diagnosis_hierarchy_1
            order by positive_start
        ) as next_positive_start,
        case when
           -- if it's the first date of a diagnosis_hierarchy_1 then new infection then 1
            min(positive_start) over (
                partition by
                    dx_and_labs.patient_key,
                    dx_and_labs.diagnosis_hierarchy_1
                order by positive_start
            ) = positive_start
            -- accounting for dupe starts because of pts with multiple isolation stays (patient_key = 11001171)
            and coalesce(prior_positive_start, {{ var('default_min_date') }}) != positive_start
            then 1
            -- if it within the min to the max infectious window of from last positive then it's the same infection and is 0 --noqa: L016
            when positive_start > lag(positive_start) over (
                    partition by
                        dx_and_labs.patient_key,
                        dx_and_labs.diagnosis_hierarchy_1
                    order by positive_start
                ) + stg_bioresponse_infectious_windows.max_infectious_window -- noqa
            then 1
            else 0
        end as infection_start_ind
    from
        dx_and_labs
        inner join {{ ref('stg_bioresponse_infectious_windows') }} as stg_bioresponse_infectious_windows
            on dx_and_labs.diagnosis_hierarchy_1 = stg_bioresponse_infectious_windows.diagnosis_hierarchy_1
        left join {{ ref('bioresponse_isolation_events') }} as bioresponse_isolation_events
            on dx_and_labs.patient_key = bioresponse_isolation_events.patient_key
            -- two days is an arbitrary pick to create a bit of a buffer
            and enter_date::date >= pos_start_date::date - interval('2 days')
            and pos_start_date::date < exit_date::date
    group by
        dx_and_labs.patient_key,
        dx_and_labs.diagnosis_hierarchy_1,
        max_infectious_window,
        normal_infectious_window,
        pos_start_date,
        enter_date,
        neg_date,
        exit_date,
        positive_source
),

stg_infectious_episode as (
    select
        patient_key,
        diagnosis_hierarchy_1,
        positive_source,
        positive_start,
        estimated_end_date,
        infection_start_ind,
        isolation_ind,
        sum(infection_start_ind) over (-- cumulative sum of same results
            partition by
                patient_key,
                diagnosis_hierarchy_1
            order by positive_start, infection_start_ind desc
            rows between unbounded preceding and current row
        ) as episode_number
    from
        add_isolation
)

select
    patient_key,
    diagnosis_hierarchy_1,
    episode_number,
    max(case when positive_source = 'labs' then 1 else 0 end) as positive_lab_ind,
    max(case when positive_source = 'problem list' then 1 else 0 end) as problem_list_ind,
    max(case when isolation_ind = 1 then 1 else 0 end) as isolation_ind,
    min(positive_start) as episode_start_date,
    max(estimated_end_date) as episode_end_date
from
    stg_infectious_episode
group by
    patient_key,
    diagnosis_hierarchy_1,
    episode_number
