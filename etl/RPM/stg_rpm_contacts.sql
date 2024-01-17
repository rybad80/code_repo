with pr_phone_num_mobile as (
    select
        stg_rpm_proxy_raw.pat_id,
        pat_rel_phone_num.phone_num,
        /* favor primary phone numbers over all others, then take the first one */
        row_number() over (
            partition by
                stg_rpm_proxy_raw.pat_id
            order by
                case when pat_rel_phone_num.primary_phone_yn = 'Y' then 1 else 99 end,
                pat_rel_phone_num.line
        ) as rn
    from
        {{ref('stg_rpm_proxy_raw') }} as stg_rpm_proxy_raw
        left join  {{ source('clarity_ods', 'pat_rel_phone_num') }} as pat_rel_phone_num
            on pat_rel_phone_num.pat_relationship_id = stg_rpm_proxy_raw.pat_relationship_id
                and pat_rel_phone_num.phone_num_type_c = '1' /* mobile */
    where
        stg_rpm_proxy_raw.rn = 1
),

pr_email_addr as (
    select
        stg_rpm_proxy_raw.pat_id,
        pat_rel_email_addr.email_address,
        /* favor primary email address over all others, then take the first one */
        row_number() over (
            partition by
                stg_rpm_proxy_raw.pat_id
            order by
                case when pat_rel_email_addr.primary_email_yn = 'Y' then 1 else 99 end,
                pat_rel_email_addr.line
        ) as rn
    from
        {{ref('stg_rpm_proxy_raw') }} as stg_rpm_proxy_raw
        left join {{ source('clarity_ods', 'pat_rel_email_addr') }} as pat_rel_email_addr
            on pat_rel_email_addr.pat_relationship_id = stg_rpm_proxy_raw.pat_relationship_id
    where
        stg_rpm_proxy_raw.rn = 1
),

proxy_mobile as (
    select
        stg_rpm_proxy_raw.pat_id,
        other_communctn.other_communic_num,
        row_number() over (
            partition by
                stg_rpm_proxy_raw.pat_id
            order by
                other_communctn.contact_priority,
                other_communctn.line
        ) as rn
    from
        {{ref('stg_rpm_proxy_raw') }} as stg_rpm_proxy_raw
        left join {{ source('clarity_ods', 'other_communctn') }} as other_communctn
            on other_communctn.pat_id = stg_rpm_proxy_raw.parent_pat_id
                and other_communctn.other_communic_c = 1 /* mobile */
                and other_communctn.other_communic_num is not null
    where
        stg_rpm_proxy_raw.rn = 1
),

proxy_email as (
    select distinct
        stg_rpm_proxy_raw.pat_id,
        patient.email_address
    from
        {{ref('stg_rpm_proxy_raw') }} as stg_rpm_proxy_raw
        inner join {{ source('clarity_ods', 'patient') }} as patient /* proxy's patient record */
            on patient.pat_id = stg_rpm_proxy_raw.parent_pat_id
    where
        stg_rpm_proxy_raw.rn = 1
),

/* if proxy_wpr_id doesn't tie back to a contact record, let's pull the primary contact
mobile / email as a back up */
non_proxy_mobile as (
    select
        stg_rpm_patient.pat_id,
        pat_rel_phone_num.phone_num,
        row_number() over (
            partition by
                stg_rpm_patient.pat_id
            order by
                pat_relationship_list.display_sequence desc nulls last,
                case when pat_rel_phone_num.primary_phone_yn = 'Y' then 1 else 99 end,
                pat_rel_phone_num.line
        ) as rn
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'pat_relationship_list') }} as pat_relationship_list
            on pat_relationship_list.pat_id = stg_rpm_patient.pat_id
        inner join {{ source('clarity_ods', 'pat_rel_phone_num') }} as pat_rel_phone_num
            on pat_rel_phone_num.pat_relationship_id = pat_relationship_list.pat_relationship_id
                and pat_rel_phone_num.phone_num_type_c = '1' /* mobile */
),

non_proxy_email as (
    select
        stg_rpm_patient.pat_id,
        pat_rel_email_addr.email_address,
        row_number() over (
            partition by
                stg_rpm_patient.pat_id
            order by
                pat_relationship_list.display_sequence desc nulls last,
                case when pat_rel_email_addr.primary_email_yn = 'Y' then 1 else 99 end,
                pat_rel_email_addr.line
        ) as rn
    from
        {{ref('stg_rpm_patient') }} as stg_rpm_patient
        inner join {{ source('clarity_ods', 'pat_relationship_list') }} as pat_relationship_list
            on pat_relationship_list.pat_id = stg_rpm_patient.pat_id
        inner join {{ source('clarity_ods', 'pat_rel_email_addr') }} as pat_rel_email_addr
            on pat_rel_email_addr.pat_relationship_id = pat_relationship_list.pat_relationship_id
)

select
    stg_rpm_patient.pat_id,
    stg_rpm_proxy_over_16.patient_over_16_phone_confirmed,
    stg_rpm_proxy_over_16.patient_over_16_phone_probable,
    replace(coalesce(
        pr_phone_num_mobile.phone_num,
        non_proxy_mobile.phone_num,
        proxy_mobile.other_communic_num
    ), '-', '') as non_patient_phone,
    stg_rpm_proxy_over_16.patient_over_16_email,
    coalesce(
        pr_email_addr.email_address,
        non_proxy_email.email_address,
        proxy_email.email_address,
        stg_rpm_patient.email_address
    ) as non_patient_email
from
    {{ref('stg_rpm_patient') }} as stg_rpm_patient
    left join {{ ref('stg_rpm_proxy_over_16') }} as stg_rpm_proxy_over_16
        on stg_rpm_proxy_over_16.pat_id = stg_rpm_patient.pat_id
    left join pr_phone_num_mobile
        on pr_phone_num_mobile.pat_id = stg_rpm_patient.pat_id
            and pr_phone_num_mobile.rn = 1
    left join pr_email_addr
        on pr_email_addr.pat_id = stg_rpm_patient.pat_id
            and pr_email_addr.rn = 1
	left join proxy_mobile
        on proxy_mobile.pat_id = stg_rpm_patient.pat_id
            and proxy_mobile.rn = 1
    left join proxy_email
        on proxy_email.pat_id = stg_rpm_patient.pat_id
    left join non_proxy_mobile
        on non_proxy_mobile.pat_id = stg_rpm_patient.pat_id
            and non_proxy_mobile.rn = 1
    left join non_proxy_email
        on non_proxy_email.pat_id = stg_rpm_patient.pat_id
            and non_proxy_email.rn = 1
