{{
	config(
		materialized = 'incremental',
		unique_key = 'stg_note_info_key',
		meta = {
			'critical': true
		}
	)
}}

select
    {{
        dbt_utils.surrogate_key([
            'hno_info.note_id'
        ])
    }} as stg_note_info_key,
    hno_info.note_id,
    hno_info.date_of_servic_dttm as note_svc_dt,
    hno_info.pat_enc_csn_id,
    coalesce(hno_info.current_author_id, '0') as current_author_id,
    hno_info.upd_dt as last_updated_at,
    current_timestamp as update_date,
    -- Try to avoid using these cdw keys:
    note_info.note_key,
    note_info.visit_key,
    note_info.dim_ip_note_type_key
from
    {{ source('clarity_ods', 'hno_info') }} as hno_info
-- Only used for cdw keys:
inner join {{ source('cdw', 'note_info') }} as note_info
    on hno_info.note_id = note_info.note_id
where 1 = 1
    {% if is_incremental() or target.name in ['ci', 'dev', 'local'] %}
        and (
            date(hno_info.upd_dt)
                >= current_date - interval('{{ var("stg_clarity_note_lookback_days") }} days')
            or date(note_info.upd_dt)
                >= current_date - interval('{{ var("stg_clarity_note_lookback_days") }} days')
        )
    {% endif %}
