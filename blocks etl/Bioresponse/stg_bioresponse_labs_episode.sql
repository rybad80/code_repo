with find_same_results as (
    select
        bioresponse_lab_results.mrn,
        bioresponse_lab_results.csn,
        bioresponse_lab_results.patient_key,
        bioresponse_lab_results.encounter_key,
        bioresponse_lab_results.encounter_type,
        bioresponse_lab_results.diagnosis_hierarchy_1,
        bioresponse_lab_results.diagnosis_hierarchy_2,
        bioresponse_lab_results.placed_date,
        bioresponse_lab_results.result_date,
        bioresponse_lab_results.procedure_order_result_key,
        lead(bioresponse_lab_results.placed_date) over(
            partition by
                bioresponse_lab_results.patient_key,
                bioresponse_lab_results.diagnosis_hierarchy_1,
                bioresponse_lab_results.diagnosis_hierarchy_2
            order by bioresponse_lab_results.placed_date
        ) as next_placed_date,
        positive_ind,
        case
            /* find a new "episode" of illness,
            * defined as a positive not within a reasonable infectious window of any prior positive,
            * likely indicating a new infection */
            -- any negative test is not a new infection and is therefore 0
            when positive_ind = 0 then 0
            -- if it is a first row, and it's a positive, it's a new infection and is a 1
            -- using result_date because of specimens taken from multiple sources
            when min(bioresponse_lab_results.result_date) over(
                partition by
                    bioresponse_lab_results.patient_key,
                    bioresponse_lab_results.diagnosis_hierarchy_1,
                    bioresponse_lab_results.diagnosis_hierarchy_2
                order by bioresponse_lab_results.placed_date
                ) = result_date then 1
            -- if prior indicator is not equal to the current positive
            when lag(positive_ind) over(
                partition by
                    bioresponse_lab_results.patient_key,
                    bioresponse_lab_results.diagnosis_hierarchy_1,
                    bioresponse_lab_results.diagnosis_hierarchy_2
                order by bioresponse_lab_results.placed_date
                ) != positive_ind
            -- AND not more then max-infectious-days between tests then it should be a new infection and it's a 1
                and lag(placed_date) over(
                    partition by
                        bioresponse_lab_results.patient_key,
                        bioresponse_lab_results.diagnosis_hierarchy_1,
                        bioresponse_lab_results.diagnosis_hierarchy_2
                    order by bioresponse_lab_results.placed_date
                ) < bioresponse_lab_results.placed_date::date + stg_bioresponse_infectious_windows.max_infectious_window -- noqa
                then 1
            else 0
        end as new_positive_ind
    from
        {{ ref('bioresponse_lab_results') }} as bioresponse_lab_results
        inner join {{ ref('stg_bioresponse_infectious_windows') }} as stg_bioresponse_infectious_windows
            on bioresponse_lab_results.diagnosis_hierarchy_1 = stg_bioresponse_infectious_windows.diagnosis_hierarchy_1 -- noqa
),

find_episodes as (
    select
        mrn,
        csn,
        patient_key,
        diagnosis_hierarchy_1,
        diagnosis_hierarchy_2,
        placed_date,
        next_placed_date,
        positive_ind,
        new_positive_ind,
        first_value(procedure_order_result_key) over (
            partition by patient_key, diagnosis_hierarchy_1, diagnosis_hierarchy_2, positive_ind
            order by result_date
        ) as procedure_order_result_key,
        sum(new_positive_ind) over( -- cumulative sum of same results
            partition by patient_key, diagnosis_hierarchy_1, diagnosis_hierarchy_2, positive_ind
            order by placed_date
            rows between unbounded preceding and current row
        ) as episode_number
    from
        find_same_results
)

select
    find_episodes.mrn,
    find_episodes.csn,
    find_episodes.patient_key,
    find_episodes.diagnosis_hierarchy_1,
    find_episodes.diagnosis_hierarchy_2,
    episode_number,
    procedure_order_result_key,
    min(placed_date) as first_pos_date,
    sum(positive_ind) as n_positive_tests,
    max(next_placed_date) as episode_neg_date,
    episode_neg_date::date - first_pos_date::date as n_days,
    case
        when
            episode_neg_date is null
            or n_days > max_infectious_window
            then first_pos_date + normal_infectious_window
        else episode_neg_date
        end as estimated_end_date
from
    find_episodes
    inner join {{ ref('stg_bioresponse_infectious_windows') }} as stg_bioresponse_infectious_windows
        on find_episodes.diagnosis_hierarchy_1 = stg_bioresponse_infectious_windows.diagnosis_hierarchy_1
where
    positive_ind = 1
group by
    find_episodes.mrn,
    find_episodes.csn,
    find_episodes.patient_key,
    find_episodes.diagnosis_hierarchy_1,
    find_episodes.diagnosis_hierarchy_2,
    procedure_order_result_key,
    max_infectious_window,
    normal_infectious_window,
    episode_number
