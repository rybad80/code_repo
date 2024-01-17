select
    v_user.usesysid as user_id,
    nvl2(worker.worker_id, 1, 0) as worker_ind,
    nvl2(stg_asset_dna_analyst.worker_id, 1, 0) as dna_ind,
    coalesce(lookup_user_account.service_account_ind, 0) as service_account_ind,
    coalesce(lookup_user_account.other_account_ind, 0) as other_account_ind,
    case
        when lower(v_user.username) = 'admin' then 'admin'
        when lookup_user_account.service_account_ind = 1 then 'service account'
        when lookup_user_account.other_account_ind = 1 then 'other'
        when worker.worker_id is not null then 'user'
        else 'unknown'
        end as account_role,
    case
        when worker.active_ind is not null then worker.active_ind
        when v_user.validuntil is null and v_user.pwd_invalid is false then 1
        else 0
        end as active_ind,
    case
        when lower(v_user.username) = 'admin' then 'admin'
        when other_account_ind = 1 then 'other'
        else coalesce(
            lower(lookup_user_account.account_subgroup),
            lower(lookup_user_account.account_group),
            'user'
        )
        end as account_group,
    lower(v_user.username) as user_name,
    coalesce(worker.preferred_reporting_name, v_user.username) as reporting_name,
    worker.manager_id,
    worker.manager_name,
    worker.cost_center_id,
    worker.cost_center_name,
    worker.job_family,
    worker.job_family_id,
    worker.reporting_chain,
    worker.worker_id
from
    {{ source('cdw', '_v_user') }} as v_user
    -- join worker including 'e_' and '_epic_upg' accounts
    left join {{ ref('worker') }} as worker
        on worker.ad_login = regexp_replace(lower(v_user.username), '^e_|_epic_upg$', '')
    left join {{ref('lookup_user_account')}} as lookup_user_account
        on lookup_user_account.user_name = v_user.username
    left join {{ ref('stg_asset_dna_analyst') }} as stg_asset_dna_analyst
        on stg_asset_dna_analyst.worker_id = worker.worker_id
