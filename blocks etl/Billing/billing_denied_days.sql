-- Query to return denied days in long format
-- Each row returned represents a day within the hospital stay which was denied
-- Denial can come from either bdc records OR the 'bed days table'
-- No duplicate combinations of visit key and denied day will be returned
-- there is an indicator column signifying if the denial was overturned for that day

with denied as (
    select
        visit_key,
        hsp_acct_key,
        har,
        csn,
        date_denied
    from
        {{ref('stg_bdc_denied')}}
    union
    select
        visit_key,
        hsp_acct_key,
        har,
        csn,
        date_denied
    from
        {{ref('stg_referral_denied')}}
),

denial_dept as (
    select
        denied.visit_key,
        denied.date_denied,
        adt_department.department_name,
        row_number() over(partition by denied.visit_key, denied.date_denied order by enter_date desc) as dept_rank
    from
        denied
        inner join {{ref('adt_department')}} as adt_department
            on denied.visit_key = adt_department.visit_key
            and denied.date_denied >= cast(adt_department.enter_date as date)
            and denied.date_denied <= adt_department.exit_date_or_current_date
),



overturned as (
    select
        hsp_acct_key,
        date_denied as date_overturned
    from
        {{ref('stg_bdc_denied')}}
    where
        overturned_ind = 1
    union
    select
        hsp_acct_key,
        date_overturned
    from
        {{ref('stg_referral_overturned')}}
),

denied_metrics as (
    select
        denied.hsp_acct_key,
        denied.date_denied,
        -- denial reason will be sourced from bdc record (prferably top ranked bdc record) unless there is no bdc denial reason
        max(case 
                when stg_bdc_denied.bdc_ranking = 1 
                    and stg_bdc_denied.bdc_denial_reason is not null 
                    then stg_bdc_denied.bdc_denial_reason 
                when stg_bdc_denied.bdc_denial_reason is not null then stg_bdc_denied.bdc_denial_reason
                else stg_referral_denied.rfl_denial_reason end) as denial_reason,
        max(case when stg_bdc_denied.date_denied is not null 
            then stg_bdc_denied.bdc_denial_payor
            else stg_referral_denied.rfl_denial_payor end) as denial_payor,
        max(case
                when stg_bdc_denied.bdc_appealed_ind = 1 then 1
                when stg_referral_denied.rfl_appealed_ind = 1 then 1
                else 0 end) as appealed_ind, -- did Case Management attempt an appeal for this day
        max(case when overturned.date_overturned is not null then 1 else 0 end) as overturned_ind,
        max(case when stg_bdc_denied.date_denied is not null then 1 else 0 end) as bdc_denied_ind,
        max(case when stg_referral_denied.date_denied is not null then 1 else 0 end) as referral_denied_ind,
        max(stg_bdc_denied.overturned_ind) as bdc_overturned_ind,
        max(case when stg_referral_overturned.date_overturned is not null then 1 else 0 end)
            as referral_overturned_ind,
        max(case when stg_referral_denied.peer_to_peer_appealed_ind = 1 then 1 else 0 end)
            as peer_to_peer_appealed_ind,
        max(
            case when stg_referral_overturned.peer_to_peer_overturned_ind = 1 then 1 else 0 end
        ) as peer_to_peer_overturned_ind,
        max(case
                when stg_bdc_denied.date_denied is null then null
                else stg_bdc_denied.bdc_closed_date end) as bdc_denial_close_date,
        max(case
                when stg_referral_denied.date_denied is null then null
                else stg_referral_denied.rfl_denial_closed_date end) as referral_denial_close_date
    from
        denied
        left join overturned
            on denied.hsp_acct_key = overturned.hsp_acct_key
            and denied.date_denied = overturned.date_overturned
        left join {{ref('stg_bdc_denied')}} as stg_bdc_denied
            on denied.hsp_acct_key = stg_bdc_denied.hsp_acct_key
            and denied.date_denied = stg_bdc_denied.date_denied
        left join {{ref('stg_referral_denied')}} as stg_referral_denied
            on denied.hsp_acct_key = stg_referral_denied.hsp_acct_key
            and denied.date_denied = stg_referral_denied.date_denied
        left join {{ref('stg_referral_overturned')}} as stg_referral_overturned
            on denied.hsp_acct_key = stg_referral_overturned.hsp_acct_key
            and denied.date_denied = stg_referral_overturned.date_overturned
    group by
        denied.hsp_acct_key,
        denied.date_denied
)

-- final SFW
select
    denied.hsp_acct_key,
    denied.har as hosp_acct_id,
    denied.visit_key,
    denied.csn,
    denied.date_denied,
    -- denial reason will be sourced from bdc record (prferably top ranked bdc record) unless there is no bdc denial reason
    denied_metrics.denial_reason,
    visit.hosp_admit_dt as hospital_admit_date,
    visit.hosp_dischrg_dt as hospital_discharge_date,
    denial_dept.department_name as denial_department,
    denied_metrics.denial_payor,
    stg_encounter.patient_class,
    denied_metrics.appealed_ind, -- did Case Management attempt an appeal for this day
    denied_metrics.overturned_ind,
    denied_metrics.bdc_denied_ind,
    denied_metrics.referral_denied_ind,
    denied_metrics.bdc_overturned_ind,
    denied_metrics.referral_overturned_ind,
    denied_metrics.peer_to_peer_appealed_ind,
    denied_metrics.peer_to_peer_overturned_ind,
    denied_metrics.bdc_denial_close_date,
    denied_metrics.referral_denial_close_date,
    visit.pat_key,
    {{dbt_utils.surrogate_key(['denied.hsp_acct_key', 'denied.date_denied'])}} as denial_key
from
    denied
    left join denied_metrics
        on denied.hsp_acct_key = denied_metrics.hsp_acct_key
        and denied.date_denied = denied_metrics.date_denied
    inner join {{source('cdw', 'visit')}} as visit
        on denied.visit_key = visit.visit_key
    left join {{ref('stg_encounter')}} as stg_encounter
        on denied.visit_key = stg_encounter.visit_key
    left join denial_dept 
        on denied.visit_key = denial_dept.visit_key
        and denied.date_denied = denial_dept.date_denied
        and denial_dept.dept_rank = 1
