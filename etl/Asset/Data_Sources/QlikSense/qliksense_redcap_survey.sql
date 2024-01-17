with get_recent_redcap_record_id as (
	select
		lower(
            replace(redcap_data.record_value, ' ', '')
        ) as record_values,
		max(redcap_data.record_id::int) as record
	from
        {{ ref('stg_redcap_record_value') }} as redcap_data
	where
        redcap_data.project_id = 903
		and redcap_data.field_name = 'app_name'
	group by
		record_values
),
stg_redcap_qliksense as (
	select
        redcap_data.record_id::int as record,
        max(
            case
                when redcap_data.field_name = 'app_name'
                then redcap_data.record_value::varchar(500)
            end
        ) as app_name,

        max(
            case
                when redcap_data.field_name = 'stream_name'
                then redcap_data.record_value::varchar(100)
            end
        ) as stream_name,
        max(
            case
                when redcap_data.field_name = 'migration_type'
                then stg_redcap_porter_value_label.element_text::varchar(30)
            end
        ) as migration_type,
        max(
            case
                when redcap_data.field_name = 'data_connections'
                then stg_redcap_porter_value_label.element_text::varchar(60)
            end
        ) as data_connections,
        max(
            case
                when redcap_data.field_name = 'tables_used'
                then redcap_data.record_value::varchar(500)
            end
        ) as tables_used,
        max(
            case
                when redcap_data.field_name = 'refresh_schedule_2'
                then stg_redcap_porter_value_label.element_text::varchar(50)
            end
        ) as refresh_schedule_2,
        max(
            case
                when redcap_data.field_name = 'reload_tier'
                then stg_redcap_porter_value_label.element_text::varchar(100)
            end
        ) as reload_tier,
        max(
            case
                when redcap_data.field_name = 'request_type'
                then stg_redcap_porter_value_label.element_text::varchar(30)
            end
        ) as request_type,
        max(
            case
                when redcap_data.field_name = 'requester_name'
                then redcap_data.record_value::varchar(100)
            end
        ) as requester_name,
        max(
            case
                when redcap_data.field_name = 'requester_email'
                then redcap_data.record_value::varchar(100)
            end
        ) as requester_email,
        max(
            case
                when redcap_data.field_name = 'failure_email'
                then redcap_data.record_value::varchar(1000)
            end
        ) as failure_email,
        max(
            case
                when redcap_data.field_name = 'require_refresh'
                then redcap_data.record_value::int
            end
        ) as require_refresh,
        max(
            case
                when redcap_data.field_name = 'qlik_sense_request_complete'
                then stg_redcap_porter_value_label.element_text::varchar(30)
            end
        ) as qlik_sense_request_complete
	from
        {{ ref('stg_redcap_record_value') }} as redcap_data
		inner join get_recent_redcap_record_id on get_recent_redcap_record_id.record = redcap_data.record_id
        left join {{ ref('stg_redcap_porter_value_label') }} as stg_redcap_porter_value_label
            on stg_redcap_porter_value_label.project_id = redcap_data.project_id
            and stg_redcap_porter_value_label.field_name = redcap_data.field_name
            and stg_redcap_porter_value_label.element_id = redcap_data.record_value
	where
        redcap_data.project_id = 903
	group by
        redcap_data.record_id
),
get_recent_publish_app as (
	select
		application_title,
		stream_name
	from
		{{ ref('asset_qliksense') }}
	where
		published_ind = 1
		and lower(stream_name) not in ('storage bin', 'recycle bin')
	group by
		application_title,
		stream_name
)
	select distinct
		asset_qliksense.qliksense_app_id,
		qliksense_qsr_reload_tasks.id as qliksense_reload_task_id,
		asset_qliksense.application_title as application_title,
		qliksense_qsr_reload_tasks.name as reload_task_name,
		asset_qliksense.owner_user_name,
		asset_qliksense.stream_name as stream_name,
		stg_redcap_qliksense.record,
		migration_type,
		data_connections,
		lower(
            replace(replace(replace(tables_used, ' ', ''), ';', ','), chr(13) || chr(10), '')
        ) as tables_used,
		trim(refresh_schedule_2) as reload_frequency,
		trim(reload_tier) as sla,
        case
            when (reload_frequency = 'Daily') and (sla = 'Gold - Before 7 AM') then 'gold'
            when (reload_frequency = 'Daily') and (sla = 'Silver - By 9 AM') then 'silver'
            when (reload_frequency = 'Daily' or reload_frequency is null) then 'bronze'
            else null
        end as airflow_dag,
		request_type,
		requester_name,
		requester_email,
        failure_email as redcap_email,
        lower(
            trim(trailing ',' from
                replace(
                    replace(
                        replace(replace(replace(failure_email, ' ', ''), ';', ','), chr(13) || chr(10), ' '),
                        '@chop.edu',
                        ''
                    ),
                    '@email.chop.edu',
                    ''
                )
            )
        ) as alert_email,
		require_refresh,
		asset_qliksense.created_date,
		asset_qliksense.last_reload_date,
		asset_qliksense.published_date,
		asset_qliksense.first_usage_date,
		asset_qliksense.last_usage_date,
		asset_qliksense.chqa_governed_ind,
		asset_qliksense.phi_ind,
		qlik_sense_request_complete
	from
		{{ ref('asset_qliksense') }} as asset_qliksense
		inner join get_recent_publish_app as recent_app on
			recent_app.application_title = asset_qliksense.application_title
			and asset_qliksense.stream_name = recent_app.stream_name
		inner join {{ source('qliksense_ods', 'qliksense_qsr_reload_tasks') }} as qliksense_qsr_reload_tasks on
			qliksense_qsr_reload_tasks.app_id = asset_qliksense.qliksense_app_id
            and enabled = true
            and is_manually_triggered != true
        left join stg_redcap_qliksense on
            lower(
                replace(stg_redcap_qliksense.app_name, ' ', '')
            ) = lower(replace(asset_qliksense.application_title, ' ', ''))
	where
		lower(asset_qliksense.stream_name) not in ('storage bin', 'recycle bin')
		and lower(asset_qliksense.stream_name) not like '%_deployment%'
		and lower(asset_qliksense.stream_name) not like '%_staging%'
