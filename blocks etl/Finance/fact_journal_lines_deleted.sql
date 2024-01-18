{{
    config(
        materialized = 'incremental',
        unique_key = 'journal_entry_line_wid',
        meta = {
            'critical': true
        }
    )
}}
--
select
    stg_workday_journal_lines_deleted.*
from
    {{ref('stg_workday_journal_lines_deleted')}} as stg_workday_journal_lines_deleted
