{{
	config(
		materialized = 'incremental',
		unique_key = 'stg_note_text_line_key',
        dist = 'note_id',
		meta = {
			'critical': true
		}
	)
}}

with stg_note_visit_info_cte as (
    select
        stg_note_visit_info.note_visit_key,
        stg_note_visit_info.contact_serial_num
    from
        {{ ref('stg_note_visit_info') }} as stg_note_visit_info
    where 1 = 1
        {% if is_incremental() %}
            and date(stg_note_visit_info.last_updated_date)
                    >= current_date - interval('{{ var("stg_clarity_note_lookback_days") }} days')
        {% endif %}
),

hno_note_text_cte as (
    select
        hno_note_text.note_csn_id,
        hno_note_text.note_id,
        hno_note_text.line as line_number,
        hno_note_text.contact_date_real,
        hno_note_text.chron_item_num,
        hno_note_text.note_text,
        hno_note_text.contact_date,
        case
            when lower(hno_note_text.is_archived_yn) = 'y'
                then 1
            when lower(hno_note_text.is_archived_yn) = 'n'
                then 0
            else -2
        end::smallint as is_archived_ind,
        hno_note_text.upd_dt
    from
        {{ source('clarity_ods', 'hno_note_text') }} as hno_note_text
    where 1 = 1
        {% if is_incremental() %}
            and date(hno_note_text.upd_dt)
                    >= current_date - interval('{{ var("stg_clarity_note_lookback_days") }} days')
        {% endif %}
    {% if target.name in ['ci', 'dev', 'local'] %}
        order by hno_note_text.upd_dt desc
        limit 100000  -- 100,000
    {% endif %}
)

select
    {{
        dbt_utils.surrogate_key([
            'hno_note_text_cte.note_csn_id',
            'hno_note_text_cte.line_number'
        ])
    }} as stg_note_text_line_key,
    hno_note_text_cte.note_csn_id,
    hno_note_text_cte.note_id,
    hno_note_text_cte.line_number,
    hno_note_text_cte.contact_date_real,
    hno_note_text_cte.chron_item_num,
    hno_note_text_cte.note_text,
    hno_note_text_cte.contact_date,
    hno_note_text_cte.is_archived_ind,
    hno_note_text_cte.upd_dt as last_updated_date,
    current_timestamp as update_date,
    -- Avoid using this cdw key:
    stg_note_visit_info_cte.note_visit_key
from
    hno_note_text_cte
inner join stg_note_visit_info_cte
    on hno_note_text_cte.note_csn_id = stg_note_visit_info_cte.contact_serial_num
