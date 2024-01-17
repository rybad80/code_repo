/* neo_nicu_respiratory_summary
One row per nicu episode with summarized respiratory support information.
*/

with high_fio2 as (
    select
        stg_neo_nicu_visit_demographics.visit_key,
        max(stg_neo_nicu_respiratory_flowsheet.recorded_date) as last_high_fio2_datetime
    from
        {{ ref('stg_neo_nicu_visit_demographics') }} as stg_neo_nicu_visit_demographics
        left join {{ ref('stg_neo_nicu_respiratory_flowsheet') }} as stg_neo_nicu_respiratory_flowsheet
            on stg_neo_nicu_respiratory_flowsheet.visit_key = stg_neo_nicu_visit_demographics.visit_key
    where
        stg_neo_nicu_respiratory_flowsheet.flowsheet_id = 40002468
        and (
            /* sometimes fio2 is recorded with decimal (ie, .21 == 21, .44 == 44) */
            case
                when stg_neo_nicu_respiratory_flowsheet.meas_val_num < 1
                    then stg_neo_nicu_respiratory_flowsheet.meas_val_num * 100
                else
                    stg_neo_nicu_respiratory_flowsheet.meas_val_num
            end
        ) > 21
    group by
        stg_neo_nicu_visit_demographics.visit_key
),

support_days as (
    select
        stg_neo_nicu_visit_demographics.visit_key,
        cast(neo_nicu_respiratory_category.recorded_date as date) as support_date,
        max(
            case when neo_nicu_respiratory_category.respiratory_support_type = 'invasive' then 1 end
        ) as invasive_support_day,
        max(
            case when neo_nicu_respiratory_category.respiratory_support_type = 'non-invasive' then 1 end
        ) as non_invasive_support_day
    from
        {{ ref('stg_neo_nicu_visit_demographics') }} as stg_neo_nicu_visit_demographics
        inner join {{ ref('neo_nicu_respiratory_category') }} as neo_nicu_respiratory_category
            on neo_nicu_respiratory_category.visit_key = stg_neo_nicu_visit_demographics.visit_key
    group by
        stg_neo_nicu_visit_demographics.visit_key,
        support_date
),

support_day_summary as (
    select
        visit_key,
        sum(invasive_support_day) as invasive_support_day_count,
        /* if the day had both invasive and non-invasive support, only count towards invasive
        since that is the more severe treatment */
        sum(
            case when invasive_support_day = 1 then 0 else non_invasive_support_day end
        ) as non_invasive_support_day_count
    from
        support_days
    group by
        visit_key
)

select
    stg_neo_nicu_visit_demographics.visit_key,
    stg_neo_nicu_visit_demographics.patient_name,
    stg_neo_nicu_visit_demographics.mrn,
    stg_neo_nicu_visit_demographics.dob,
    stg_neo_nicu_visit_demographics.sex,
    stg_neo_nicu_visit_demographics.gestational_age_complete_weeks,
    stg_neo_nicu_visit_demographics.gestational_age_remainder_days,
    stg_neo_nicu_visit_demographics.birth_weight_grams,
    stg_neo_nicu_visit_demographics.hospital_admit_date,
    stg_neo_nicu_visit_demographics.hospital_discharge_date,
    high_fio2.last_high_fio2_datetime,
    coalesce(support_day_summary.invasive_support_day_count, 0) as invasive_support_days,
    coalesce(support_day_summary.non_invasive_support_day_count, 0) as non_invasive_support_days,
    invasive_support_days + non_invasive_support_days as total_support_days,
    stg_neo_nicu_visit_demographics.pat_key
from
    {{ ref('stg_neo_nicu_visit_demographics') }} as stg_neo_nicu_visit_demographics
    left join high_fio2
        on high_fio2.visit_key = stg_neo_nicu_visit_demographics.visit_key
    left join support_day_summary
        on support_day_summary.visit_key = stg_neo_nicu_visit_demographics.visit_key
