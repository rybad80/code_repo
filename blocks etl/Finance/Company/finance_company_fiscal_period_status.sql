with getfirstclose as (
    select
        ledger_period_close_status_history.company_id,
            ledger_period_close_status_history.fiscal_period_end_date,
            min(ledger_period_close_status_history.closed_date) as earliest_close_date,
            max(ledger_period_close_status_history.closed_date) as latest_close_date
            from
                {{source('workday_ods', 'ledger_period_close_status_history')}} as ledger_period_close_status_history --noqa: L016
            group by
                ledger_period_close_status_history.company_id,
                ledger_period_close_status_history.fiscal_period_end_date
) --changed to CTE
select
    getfirstclose.earliest_close_date,
    case when getfirstclose.earliest_close_date is not null and ledger_period_status_id != 'CLOSED'
            then 'in ' || ledger_period_status_descriptor
        when getfirstclose.earliest_close_date is null
            then 'Company not Closed for the period yet; ' || ledger_period_status_descriptor
        when ledger_period_status_id = 'CLOSED'
            then ledger_period_status_descriptor
    end as status_description,
    case when getfirstclose.earliest_close_date is not null and ledger_period_status_id != 'CLOSED'
            then ledger_period_descriptor || ' Company '
                || ledger_period_close_status.company_id || ' in '
                || ledger_period_status_descriptor || '; earliest close date was '
                || to_char(getfirstclose.earliest_close_date, 'mm/dd/yyyy')
        when getfirstclose.earliest_close_date is null
            then ledger_period_descriptor || ' Company '
                || ledger_period_close_status.company_id || ': ' || ledger_period_status_descriptor
        when ledger_period_status_id = 'CLOSED'
            then ledger_period_descriptor || ' Company ' || ledger_period_close_status.company_id
            || ': ' || ledger_period_status_descriptor
    end as ledger_period_status_description,
    case when getfirstclose.earliest_close_date is not null then 1 else 0 end as company_period_earliest_close_ind,
/*  to support aggregate roll-ups for metrics that are dependent on monthly close being completed;
 Days in AR would be able to use the first one */
-- sum these and if over 0 for an aggregate, that aggregate level should not be shown yet (Days in AR)
    case when getfirstclose.earliest_close_date is null then 1 else 0 end as company_period_close_not_yet_ind,
-- sum these and if over 0 for an aggregate, that aggregate level should not be shown yet
    case when ledger_period_status_id != 'CLOSED' then 1 else 0 end as company_period_not_current_closed_ind,
    ledger_period_close_status.company_id, --, fiscal_period_start_date
    ledger_period_close_status.fiscal_period_end_date,
    master_date.dt_key as fiscal_end_dt_key,
    fiscal_period_descriptor,
    ledger_type_descriptor, --, ledger_period_status_id
    ledger_period_status_descriptor,
    ledger_period_descriptor,
    last_updated_by as workday_last_updated_by, -- parse out to just the name after the /?
    last_update_date as workday_last_update_date
from
    {{source('workday_ods', 'ledger_period_close_status')}} as ledger_period_close_status
    inner join
        {{source('cdw', 'master_date')}} as master_date
            on ledger_period_close_status.fiscal_period_end_date = master_date.full_dt
    left join
        getfirstclose
            on ledger_period_close_status.company_id = getfirstclose.company_id
            and ledger_period_close_status.fiscal_period_end_date = getfirstclose.fiscal_period_end_date
