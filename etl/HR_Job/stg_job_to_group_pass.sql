{{ config(meta = {
    'critical': true
}) }}

/* stg_job_to_group_pass,
putting job_code to group mappings into one table from these job_code collections stage tables:
=> stg_job_group_s2_rn_job_plus.sql
=> stg_job_group_s3_nursing_category_plus
=> stg_job_group_s4_magnet_reporting_plus
the following will be renamed for steps 5 & 6
=> stg_job_group_s2_abc_job_code_and_family
=> stg_job_group_s3_def_job_family_group_and_category
*/

with
job_code_method as (
 select
        s2_pass.job_code,
        s2_pass.provider_alignment_use_ind,
        s2_pass.order_num,
        s2_pass.process_rank,
        s2_pass.job_group_id
    from
        {{ ref('stg_job_group_s2_rn_job_plus') }} as s2_pass

union all

 select
        s3_pass.job_code,
        s3_pass.provider_alignment_use_ind,
        s3_pass.order_num,
        s3_pass.process_rank,
        s3_pass.job_group_id
    from
        {{ ref('stg_job_group_s3_nursing_category_plus') }} as s3_pass

union all

select
        s4_pass.job_code,
        s4_pass.provider_alignment_use_ind,
        s4_pass.order_num,
        s4_pass.process_rank,
        s4_pass.job_group_id
    from
        {{ ref('stg_job_group_s4_magnet_reporting_plus') }} as s4_pass

union all

 select
        s5_pass.job_code,
        s5_pass.provider_alignment_use_ind,
        s5_pass.order_num,
        s5_pass.process_rank,
        s5_pass.job_group_id
    from
        {{ ref('stg_job_group_s5_job_code_and_family') }} as s5_pass

union all

 select
        s6_pass.job_code,
        s6_pass.provider_alignment_use_ind,
        s6_pass.order_num,
        s6_pass.process_rank,
        s6_pass.job_group_id
    from
        {{ ref('stg_job_group_s6_job_family_group_and_category') }} as s6_pass
)

select
    job_code_method.job_code,
    job_code_method.provider_alignment_use_ind,
    job_code_method.order_num,
    job_code_method.process_rank,
    job_code_method.job_group_id,
    coalesce(lookup_job_group_root_attribute.plurality_ind, 1) as plurality_ind,
    case
        when
            row_number() over(
                partition by
                    job_code_method.job_code,
                    job_code_method.process_rank
                order by
                    coalesce(job_code_method.order_num, 999),
                        job_code_method.job_group_id
                ) = 1 then 1
        when
            plurality_ind = 1 then 1
        else 0
    end as take_this_job_group_id_ind,
    job_group_levels.job_group_granularity_path
from
    job_code_method
    left join {{ ref('lookup_job_group_root_attribute') }} as lookup_job_group_root_attribute
        on job_code_method.process_rank
            = lookup_job_group_root_attribute.process_rank
    left join {{ ref('job_group_levels') }} as job_group_levels
        on job_code_method.job_group_id
            =  job_group_levels.job_group_id
