{{
  config(
    meta = {
      'critical': false
    }
  )
}}
with container_events as (
    select
        ovc_events.container_id,
        min(case when lower(ed_event_tmpl_info.record_name) = 'lab specimen accessioned'
            then ovc_events.event_instant else null end) as specimen_accessioned_datetime,
        min(case when lower(ed_event_tmpl_info.record_name) = 'lab tracked'
            and lower(zc_event_track_reasn.title) = 'sent to cls'
            then ovc_events.event_instant else null end) as sent_to_central_lab_datetime,
        min(case when lower(ed_event_tmpl_info.record_name) = 'lab tracked'
            and lower(zc_event_track_reasn.title) = 'arrived in cls'
            then ovc_events.event_instant else null end) as arrived_in_central_lab_datetime,
        min(case when lower(ed_event_tmpl_info.record_name) = 'lab received'
            and lower(ovc_events.event_sys_comments) like 'received into: uc hospital lab%'
            then ovc_events.event_instant else null end) as received_in_uc_lab_datetime,
        min(case when lower(ed_event_tmpl_info.record_name) = 'lab received'
            and lower(ovc_events.event_sys_comments) like 'received into: kop hospital lab%'
            then ovc_events.event_instant else null end) as received_in_koph_lab_datetime
    from {{source('clarity_ods','ovc_events')}} as ovc_events
    left join {{source('clarity_ods','zc_event_track_reasn')}} as zc_event_track_reasn
        on zc_event_track_reasn.event_track_reasn_c = ovc_events.event_track_reasn_c
    left join {{source('clarity_ods', 'ed_event_tmpl_info')}} as ed_event_tmpl_info
        on ed_event_tmpl_info.record_id = ovc_events.event_type
    group by ovc_events.container_id
),
container_specimen as (
    select
        ovc_specimens.specimen_id,
        ovc_specimens.container_id
    from
        {{source('clarity_ods', 'ovc_specimens')}} as ovc_specimens
    inner join {{ref('stg_specimen_order')}} as stg_specimen_order
        on stg_specimen_order.specimen_id = ovc_specimens.specimen_id
    group by
        ovc_specimens.specimen_id,
        ovc_specimens.container_id
)
select
{{
        dbt_utils.surrogate_key(["'CLARITY'", 'ovc_db_main.container_id'])
}} as specimen_container_key,
'CLARITY~' || ovc_db_main.container_id as integration_id,
ovc_db_main.container_id,
container_specimen.specimen_id,
container_type.container_type_name as container_type,
coalesce(media_type.container_type_name, 'NA') as media_type,
case
    when ovc_db_main.pour_off_ctnr_yn is null 
        or ovc_db_main.pour_off_ctnr_yn = 'N' then 1
    when ovc_db_main.pour_off_ctnr_yn = 'Y' then 0
    else null
end as initial_container_ind,
container_events.specimen_accessioned_datetime,
container_events.sent_to_central_lab_datetime,
container_events.arrived_in_central_lab_datetime,
container_events.received_in_uc_lab_datetime,
container_events.received_in_koph_lab_datetime
from {{source('clarity_ods','ovc_db_main')}} as ovc_db_main
left join {{source('clarity_ods','container_type')}} as container_type
	on container_type.container_type_id = ovc_db_main.spec_ctnr_type_id
left join {{source('clarity_ods','container_type')}} as media_type
	on media_type.container_type_id = ovc_db_main.media_type_id
inner join container_specimen
	on container_specimen.container_id = ovc_db_main.container_id
left join container_events
	on container_events.container_id = ovc_db_main.container_id
