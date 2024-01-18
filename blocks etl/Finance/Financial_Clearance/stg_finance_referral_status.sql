with referral_status as (
    select
        referral.referral_id,
        case
            when referral_hist.referral_id is null
                then 99999
            else
                referral_hist.line
        end as line,
        case
            when referral_hist.referral_id is null
                then referral.entry_date
            else
                referral_hist.change_datetime
        end as change_datetime,
        case
            when referral_hist.referral_id is null
                then referral.rfl_status_c
            else
                cast(case
                    when nullif(ltrim(referral_hist.auth_hx_item_value, '0123456789'), '') is null
                        then referral_hist.auth_hx_item_value
                    else
                        null
                end as integer)
        end as referral_status_c,
        referral.payor_id,
        referral.plan_id,
        case when
            referral_hist.referral_id is null
                then referral.entry_date
            else
                referral_hist.change_local_dttm
        end as change_local_dttm
    from
        {{source('clarity_ods', 'referral')}} as referral
    left join
        {{source('clarity_ods', 'referral_hist')}} as referral_hist
            on referral.referral_id = referral_hist.referral_id
            and referral_hist.auth_hx_item_number = '50'
)

select
    referral_status.referral_id,
    referral_status.change_datetime as status_start_datetime,
    case
        when lead(referral_status.change_datetime) over
            (partition by referral_status.referral_id
                order by referral_status.change_datetime, referral_status.line) is not null
            then lead(referral_status.change_datetime) over
                (partition by referral_status.referral_id
                    order by referral_status.change_datetime, referral_status.line)
        else
            to_date('2114-10-14', 'yyyy-mm-dd')
        end as status_end_datetime,
    referral_status.referral_status_c,
    zc_rfl_status.name as referral_status_name,
    case
        when referral_status.referral_status_c = 1
            then 'y'
        else
            'n'
    end as status_is_approved_yn,
    referral_status.payor_id,
    clarity_epm.payor_name,
    referral_status.plan_id,
    clarity_epp.benefit_plan_name,
    referral_status.change_local_dttm as status_start_loc_dttm,
    case
        when lead(referral_status.change_local_dttm) over
            (partition by referral_status.referral_id
                order by referral_status.change_local_dttm, referral_status.line) is not null
            then lead(referral_status.change_local_dttm)
                over (partition by referral_status.referral_id
                    order by referral_status.change_local_dttm, referral_status.line)
        else
            to_date('2114-10-14', 'yyyy-mm-dd')
    end as status_end_loc_dttm
from
    referral_status
left join
    {{source('clarity_ods', 'zc_rfl_status')}} as zc_rfl_status
        on referral_status.referral_status_c = zc_rfl_status.rfl_status_c
left join
    {{source('clarity_ods', 'clarity_epm')}} as clarity_epm
        on referral_status.payor_id = clarity_epm.payor_id
left join
    {{source('clarity_ods', 'clarity_epp')}} as clarity_epp
        on referral_status.plan_id = clarity_epp.benefit_plan_id
