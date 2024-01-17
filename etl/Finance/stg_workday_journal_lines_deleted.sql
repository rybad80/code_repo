{{ config(meta = {
    'critical': true
}) }}

select
    workday_journal_lines.*,
    (to_timestamp(substring(workday_journal_lines.last_updated_moment, 1, 19),
    'yyyy-mm-dd"T"hh24:mi:ss') + interval '3 hour')::date as offset_last_updated_moment,
    current_date as update_date
from
    {{source('workday_ods', 'workday_journal_lines')}} as workday_journal_lines
where
    offset_last_updated_moment > current_date - 29  -- noqa: L028
    and offset_last_updated_moment -- noqa: L028
    < (select
       (max(substring(last_updated_moment, 1, 10)))::date
    from
       {{source('workday_ods', 'workday_journal_lines_compare')}}
    )
    and workday_journal_lines.journal_status = 'Posted'
    and workday_journal_lines.journal_entry_line_wid not in
     (
    select
        workday_journal_lines_compare.journal_entry_line_wid
    from
        {{source('workday_ods', 'workday_journal_lines_compare')}}  as workday_journal_lines_compare
    where upd_dt::date --noqa:L028
        = (select max(upd_dt::date) --noqa:L028
          from {{source('workday_ods', 'workday_journal_lines_compare')}}
        )
     )
