/* Query pulling if patient was in the ICU at all during infection
Looking if ICU stay occured within pt's thirty days of start of infection
Final granularity should be one row per infection*/

select
    stg_doh_virals_cohort.encounter_key,
    stg_doh_virals_cohort.encounter_episode_key,
    max(
        case when
        -- test had to have placed before icu exit
        -- and would have had to enter within a reasonable period of time from viral start
            stg_doh_virals_cohort.placed_date <= stg_adt_all.dept_exit_date_or_current_date
            and stg_doh_virals_cohort.thirty_day_window > stg_adt_all.dept_enter_date
            then 1
            else 0
            end
    ) as icu_ind
from
    {{ ref('stg_doh_virals_cohort') }} as stg_doh_virals_cohort
    inner join {{ref('stg_adt_all')}} as stg_adt_all
        on stg_doh_virals_cohort.encounter_key = stg_adt_all.encounter_key
where
    stg_adt_all.icu_ind = 1
    and stg_doh_virals_cohort.order_of_tests = 1
group by
    stg_doh_virals_cohort.encounter_key,
    stg_doh_virals_cohort.encounter_episode_key
