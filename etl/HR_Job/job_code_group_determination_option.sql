{{ config(meta = {
    'critical': true
}) }}

with how_determined_parts as (
    select order_num, attribute_1, attribute_2, attribute_3
    from {{ ref('stg_job_group_s1_attribute_alignment_map') }}
    group by order_num, attribute_1, attribute_2, attribute_3
)
select
case how_determined_parts.attribute_1
	when 'job_code' then prfl.job_code
	when 'nursing_category' then coalesce(prfl.nursing_category, 'NULL')
	when 'job_family' then prfl.job_family
	when 'magnet_reporting_name' then prfl.magnet_reporting_name
	when 'job_category' then prfl.job_category_name
	when 'rn_job_ind' then trim(to_char(prfl.rn_job_ind, '9'))
	when 'management_level' then prfl.management_level
	when 'job_family_group' then prfl.job_family_group
	else how_determined_parts.attribute_1 || ' value'
	end as how_determined_parts_attribute_1,
case when how_determined_parts.attribute_2 is not null then
case how_determined_parts.attribute_2
	when 'job_code' then prfl.job_code
	when 'nursing_category' then prfl.nursing_category
	when 'job_family' then prfl.job_family
	when 'magnet_reporting_name' then prfl.magnet_reporting_name
	when 'job_category' then prfl.job_category_name
	when 'rn_job_ind' then trim(to_char(prfl.rn_job_ind, '9') )
	when 'management_level' then prfl.management_level
	when 'job_family_group' then prfl.job_family_group
	else how_determined_parts.attribute_2 || ' value'
	end
	else ''
	end as how_determined_parts_attribute_2,
case when how_determined_parts.attribute_3 is not null then
case how_determined_parts.attribute_3
	when 'job_code' then prfl.job_code
	when 'nursing_category' then prfl.nursing_category
	when 'job_family' then prfl.job_family
	when 'magnet_reporting_name' then prfl.magnet_reporting_name
	when 'job_category' then prfl.job_category_name
	when 'rn_job_ind' then trim(to_char(prfl.rn_job_ind, '9'))
	when 'management_level' then prfl.management_level
	when 'job_family_group' then prfl.job_family_group
	else how_determined_parts.attribute_3 || ' value'
	end
	else ''
	end as how_determined_parts_attribute_3,
how_determined_parts.attribute_1,
how_determined_parts.attribute_2,
how_determined_parts.attribute_3,
trim(how_determined_parts.attribute_1
|| nvl2(how_determined_parts.attribute_2, ', ', '')
|| coalesce(how_determined_parts.attribute_2, '')
|| nvl2(how_determined_parts.attribute_3, ', ', '')
|| coalesce(how_determined_parts.attribute_3, '')) as possible_how_determined,
coalesce(how_determined_parts_attribute_1, '')
|| coalesce(case when how_determined_parts.attribute_2 is not null
then ' * ' || coalesce(how_determined_parts_attribute_2, '') end, '')
|| coalesce(case when how_determined_parts.attribute_3 is not null
then ' * ' || coalesce(how_determined_parts_attribute_3, '') end, '') as possible_values_used,
prfl.job_title_display,
dtrmtn.job_code,
dtrmtn.provider_alignment_use_ind,
dtrmtn.order_num,
dtrmtn.process_rank,
dtrmtn.job_group_id,
dtrmtn.plurality_ind,
dtrmtn.job_group_granularity_path,
lvls.level_1_id,
lvls.root_job_hierarchy,
case
    when dtrmtn.take_this_job_group_id_ind = 1
    and prfl.provider_job_group_id = dtrmtn.job_group_id
    then 'checked'
    else case
        when lvls.level_1_id in ('Provider', 'TBD' )
        then case
            when dtrmtn.take_this_job_group_id_ind = 1
			then case
                when check_if_process_1_too.job_code is null
				then 'concern!  check job_group_id'
                else 'process 1 won; ' || dtrmtn.process_rank || ' lost' end
            else 'not matched, lost selection' end
        else case
            when dtrmtn.take_this_job_group_id_ind = 1
            and dtrmtn.plurality_ind = 0
            then 'won'
            else case
                when dtrmtn.plurality_ind = 1
                then 'extra' else 'lost' end
            end
            || case
		when lvls.level_1_id is null
		then ' tree to be determined'
		else ' other job group tree'
		end
        end
end as option_final_state,
case
    when dtrmtn.process_rank = 3
    then case
        when check_if_process_1_too.job_code is null
        then dtrmtn.take_this_job_group_id_ind
        else 0 end
    when dtrmtn.process_rank = 1
    then dtrmtn.take_this_job_group_id_ind
    when option_final_state = 'checked'
	then dtrmtn.take_this_job_group_id_ind
else 0 end  as take_this_job_group_id_ind
from {{ ref('stg_job_to_group_pass') }} as dtrmtn
left join {{ ref('job_code_profile') }} as prfl
    on dtrmtn.job_code = prfl.job_code
left join {{ ref('stg_job_to_group_pass') }} as check_if_process_1_too
    on dtrmtn.job_code = check_if_process_1_too.job_code /* only to solve if  */
	and check_if_process_1_too.process_rank = 1    /* provider (1)) and TBD (2)) present */
	and check_if_process_1_too.take_this_job_group_id_ind = 1 /*  take process 1 */
left join {{ ref('job_group_levels') }} as lvls
    on dtrmtn.job_group_id = lvls.job_group_id
left join how_determined_parts
    on dtrmtn.order_num = how_determined_parts.order_num
