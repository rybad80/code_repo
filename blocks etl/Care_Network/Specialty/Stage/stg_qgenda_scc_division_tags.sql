with division_tags as (
	select
		scheduleentry_stafftags.schedulekey,
        stg_qgenda_scc_schedule_entry_base.staffkey,
        stafftags_tags_name,
		substr(stafftags_tags_name, 0, 3) as site,
		regexp_extract(stafftags_tags_name, '\d+')::bigint as qgenda_division_id,
        trim(regexp_extract(stafftags_tags_name, '^.* ')) as qgenda_division_name
	from {{ source('qgenda_ods', 'scheduleentry_stafftags') }} as scheduleentry_stafftags
	inner join {{ ref('stg_qgenda_scc_schedule_entry_base') }} as stg_qgenda_scc_schedule_entry_base
		on stg_qgenda_scc_schedule_entry_base.schedulekey = scheduleentry_stafftags.schedulekey
    inner join {{ ref('stg_qgenda_scc_location') }} as stg_qgenda_scc_location
        on stg_qgenda_scc_schedule_entry_base.schedulekey = stg_qgenda_scc_location.schedulekey
	where stafftags_categoryname = 'Division'
    and site = stg_qgenda_scc_location.site
    group by
        scheduleentry_stafftags.schedulekey,
        stg_qgenda_scc_schedule_entry_base.staffkey,
        stafftags_tags_name,
        site,
        qgenda_division_id,
        qgenda_division_name
),

division_id_tags as (
	select
		scheduleentry_stafftags.schedulekey,
        stg_qgenda_scc_schedule_entry_base.staffkey,
		stafftags_tags_name::bigint as qgenda_division_id
	from {{ source('qgenda_ods', 'scheduleentry_stafftags') }} as scheduleentry_stafftags
	inner join {{ ref('stg_qgenda_scc_schedule_entry_base') }} as stg_qgenda_scc_schedule_entry_base
		on stg_qgenda_scc_schedule_entry_base.schedulekey = scheduleentry_stafftags.schedulekey
	where stafftags_categoryname = 'Division ID'
    group by
		scheduleentry_stafftags.schedulekey,
		stg_qgenda_scc_schedule_entry_base.staffkey,
		qgenda_division_id
),

qgenda_division_tags as (
    select
        division_tags.schedulekey,
        division_tags.staffkey,
        division_tags.site,
        division_tags.qgenda_division_name,
        case
            when qgenda_division_name = 'ABG CARDIOLOGY' then '54'
            when qgenda_division_name = 'BUC ENDOCRINE' then '83241010'
            else coalesce(division_id_tags.qgenda_division_id, division_tags.qgenda_division_id)
        end as department_id
    from division_tags
    left join division_id_tags
        on division_id_tags.schedulekey = division_tags.schedulekey
        and division_tags.qgenda_division_id = division_id_tags.qgenda_division_id
    group by
        division_tags.schedulekey,
        division_tags.staffkey,
        division_tags.site,
        division_tags.qgenda_division_name,
        department_id
)

select
    qgenda_division_tags.*,
    department_name,
    specialty_name
from qgenda_division_tags
left join {{ ref('department_care_network') }} as department_care_network
    on department_care_network.department_id = qgenda_division_tags.department_id
