with purchase_item as (
	select distinct
		purchase_item_id as item_number,
		purchase_item_wid,
		item_identifier || ' - ' || item_name as purchase_item,
		item_name as description,
		item_description,
		item_unit_price as unit_price
	from
		{{ source('workday_ods', 'purchase_item') }}
),
-- Purchase Items have multiple tags. getting a unique row of data by concatenating the Item Tags
-- This replicates the data as was presented by the Workday RAAS created to serve the kill report
-- process.
item_tags as (
	select
		purchase_item_wid,
		group_concat(coalesce(item_tag_descriptor, ''), '; ')  as item_tags
	from
		(
		select distinct
			purchase_item_wid,
			item_tag_descriptor
		from
			{{ source('workday_ods', 'purchase_item_item_tags') }}
		) as t
group by
	purchase_item_wid
)
select distinct
	pi.item_number,
	pi.purchase_item_wid,
	pi.purchase_item,
	pi.description,
	scm_purchase_item_master_substitutes.item_description,
	case
        when it.item_tags != '' then it.item_tags
    end as item_tags,
    scm_purchase_item_master_substitutes.worktags,
    scm_purchase_item_master_substitutes.item_status,
	pi.unit_price,
    scm_purchase_item_master_substitutes.sub_priority,
    scm_purchase_item_master_substitutes.substitute_usage,
    scm_purchase_item_master_substitutes.item_sub,
	abs({{
	dbt_utils.surrogate_key([
		'pi.purchase_item_wid',
		'scm_purchase_item_master_substitutes.sub_priority',
		'scm_purchase_item_master_substitutes.item_sub'
	])
	}}) as primary_key
from
	purchase_item as pi
inner join
    {{ source('workday_ods', 'scm_purchase_item_master_substitutes') }} as scm_purchase_item_master_substitutes
on
    pi.purchase_item_wid = scm_purchase_item_master_substitutes.purchase_item_wid
left join
	item_tags as it
on
	pi.purchase_item_wid = it.purchase_item_wid
