{{ config(meta = {
    'critical': true
}) }}

/* stg_nursing_job_group_levels
to set-up these nursing-specific addendums to make nursing_job_group_levels:
staff_nurse_ind - if the level 4 is StaffNurse which are the Acute and Ambulatory care nurses
job_group_level_4_id -- the level 4 or the job group itself (used for Staffing) if the branch ends above level 4
variable_job_ind -- based on current setting via the lookup_job_group_set and rolled up to lvl 4 if set
for all chiclren of that level 4
nursing_job_group_sort_num -- pops the nursing section and the nursing variable ones within it up top in
the provider sorted list
*/

with variable_job as (
    select
        lookup_job_group_set.usage,
        lookup_job_group_set.set_desc,
        lookup_job_group_set.job_group_id
    from
        {{ ref('lookup_job_group_set') }} as lookup_job_group_set
        inner join {{ ref('nursing_pay_period') }} as nursing_pay_period
            on lookup_job_group_set.effective_thru_fiscal_year >= nursing_pay_period.fiscal_year
            and nursing_pay_period.latest_pay_period_ind = 1
    where
        lookup_job_group_set.usage = 'flex component'
        and lookup_job_group_set.set_desc = 'variable'
),

apply_variable as (
    select
        stg_all_job_group_levels.job_group_id,
        stg_all_job_group_levels.end_level,
        stg_all_job_group_levels.level_4_id,
        case variable_job.set_desc
            when 'variable'
            then 1 else 0
        end as jg_variable_job_ind
    from
        {{ ref('stg_all_job_group_levels') }} as stg_all_job_group_levels
        left join variable_job
            on variable_job.job_group_id in (
                stg_all_job_group_levels.level_3_id,
                stg_all_job_group_levels.level_4_id,
                stg_all_job_group_levels.level_5_id)
),

level_4_with_fixed as (
    select
        level_4_id,
        jg_variable_job_ind as level_4_variable_job_ind
    from
        apply_variable
    where
        jg_variable_job_ind = 0
        and level_4_id is not null
        and end_level > 4
    group by
        level_4_id,
        jg_variable_job_ind
),

level_4_with_variable as (
    select
        level_4_id,
        jg_variable_job_ind as level_4_variable_job_ind
    from
        apply_variable
    where
        jg_variable_job_ind = 1
        and level_4_id is not null
        and end_level > 4
    group by
        level_4_id,
        jg_variable_job_ind
)

select
    job_group_levels.*,
    case job_group_levels.level_4_id
        when 'StaffNurse'
        then 1 else 0
    end as staff_nurse_ind,
    coalesce(
        job_group_levels.level_4_id,
        job_group_levels.job_group_id,
        'job group TBD') as job_group_level_4_id,
    case
        when job_group_levels.end_level = 4
        then coalesce( -- if any child groups are fixed that wins, else if variable, set it
            level_4_with_fixed.level_4_variable_job_ind,
            level_4_with_variable.level_4_variable_job_ind,
            0)
        else apply_variable.jg_variable_job_ind
    end as variable_job_ind,
    case variable_job_ind
            when 1 then 0
            else case
                when job_group_levels.job_group_parent = 'root'
                    and job_group_levels.job_group_id = 'Provider'
                then 0 -- for Nursing put Provider header at the top
                when job_group_levels.level_4_id = 'SafetyObserver'
                then 20000 -- put the Sitter/psychTech (BHC) right under the variable jobs
                when job_group_levels.level_2_id = 'Nursing'
                    then case -- and rest of Nursing after the variable ones
                        when job_group_levels.end_level > 2
                        then 1100000 -- bump all else under Nursing heder down
                        else 0
                    end
                else 10000000 end
            end as nursing_job_group_sort_addend

from
     {{ ref('stg_all_job_group_levels') }} as job_group_levels
    left join apply_variable
        on job_group_levels.job_group_id = apply_variable.job_group_id
    left join level_4_with_variable
        on job_group_levels.job_group_id = level_4_with_variable.level_4_id
    left join level_4_with_fixed
        on job_group_levels.job_group_id = level_4_with_fixed.level_4_id
