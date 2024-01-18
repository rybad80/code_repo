{{ config(meta = {
    'critical': true
}) }}

with job_group_attribute_alignment as (
/*  combine the direct job_code mapping with other attribute combination rows
    and capture the root hierarchy to which the target job_group_id belongs*/
    select
        job_group_levels.root_job_hierarchy,
        lookup_job_group_by_attribute.order_num,
        lookup_job_group_by_attribute.job_group_id,
        lookup_job_group_by_attribute.attribute_1,
        lookup_job_group_by_attribute.attribute_1_value,
        lookup_job_group_by_attribute.attribute_2,
        lookup_job_group_by_attribute.attribute_2_value,
        lookup_job_group_by_attribute.attribute_3,
        lookup_job_group_by_attribute.attribute_3_value
    from {{ref('lookup_job_group_by_attribute')}} as lookup_job_group_by_attribute
    left join {{ref('job_group_levels')}} as job_group_levels
        on lookup_job_group_by_attribute.job_group_id = job_group_levels.job_group_id

    union all

    select job_group_levels.root_job_hierarchy,
        '10' as order_num,
        lookup_job_group_direct_job_code.job_group as job_group_id,
        'job_code' as aattribute_1_value,
        lookup_job_group_direct_job_code.job_code as attribute_1_value,
        null as attribute_2,
        null as attribute_2_value,
        null as attribute_3,
        null as attribute_3_value
    from {{ref('lookup_job_group_direct_job_code')}} as lookup_job_group_direct_job_code
    left join {{ref('job_group_levels')}} as job_group_levels
        on lookup_job_group_direct_job_code.job_group = job_group_levels.job_group_id

)

select
    job_group_attribute_alignment.order_num,
    job_group_attribute_alignment.job_group_id,
    job_group_attribute_alignment.attribute_1,
    job_group_attribute_alignment.attribute_1_value,
    job_group_attribute_alignment.attribute_2,
    job_group_attribute_alignment.attribute_2_value,
    job_group_attribute_alignment.attribute_3,
    job_group_attribute_alignment.attribute_3_value,
    job_group_attribute_alignment.root_job_hierarchy,
    coalesce(lookup_job_group_root_attribute.provider_alignment_use_ind, 0) as provider_alignment_use_ind,
    coalesce(lookup_job_group_root_attribute.process_rank, 999) as process_rank
from job_group_attribute_alignment
left join {{ref('lookup_job_group_root_attribute')}} as lookup_job_group_root_attribute
    on job_group_attribute_alignment.root_job_hierarchy = lookup_job_group_root_attribute.root_job_hierarchy
