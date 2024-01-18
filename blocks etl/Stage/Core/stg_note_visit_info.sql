{{
	config(
		materialized = 'incremental',
		unique_key = 'stg_note_visit_info_key',
		meta = {
			'critical': true
		}
	)
}}

select
    {{
        dbt_utils.surrogate_key([
            'note_enc_info.contact_serial_num'
        ])
    }} as stg_note_visit_info_key,
    note_enc_info.note_id,
    note_enc_info.contact_serial_num,
    note_enc_info.contact_date_real,
    note_enc_info.contact_num,
    note_enc_info.spec_time_loc_dttm,
    note_enc_info.ent_inst_local_dttm,
    coalesce(note_enc_info.cnct_note_type_c, '0') as contact_note_type_id,
    coalesce(note_enc_info.author_service_c, '0') as author_service_id,
    coalesce(note_enc_info.author_prvd_type_c, '-2') as author_prvd_type_id,
    coalesce(note_enc_info.note_status_c, '0') as note_status_id,
    coalesce(note_enc_info.sensitive_stat_c, '0') as sensitive_status_id,
    coalesce(note_enc_info.author_user_id, '0') as author_user_id,
    note_enc_info.upd_dt as last_updated_date,
    current_timestamp as update_date,
    -- Try to avoid using this Informatica key:
    note_visit_info.note_visit_key
from
    {{ source('clarity_ods', 'note_enc_info') }} as note_enc_info
inner join {{ source('cdw', 'note_visit_info') }} as note_visit_info
    on note_enc_info.contact_serial_num = note_visit_info.note_enc_id
where 1 = 1
    {% if is_incremental() or target.name in ['ci', 'dev', 'local'] %}
        and (
            date(note_enc_info.upd_dt)
                >= current_date - interval('{{ var("stg_clarity_note_lookback_days") }} days')
            or date(note_visit_info.upd_dt)
                >= current_date - interval('{{ var("stg_clarity_note_lookback_days") }} days')
        )
    {% endif %}
