select
    {{
        dbt_utils.surrogate_key([
            'myc_mesg.message_id',
            'myc_mesg.created_time'
        ])
    }} as myc_message_key,
    myc_mesg.message_id as myc_message_id,
    inbasket_msg_id,
    created_time,
    myc_mesg.myc_msg_typ_c as message_type_id,
    zc_myc_msg_typ.name as message_type,
    parent_message_id,
    pat_id,
    pat_enc_csn_id,
    tofrom_pat_c,
    case
        when tofrom_pat_c = 1 then from_user_id
        when tofrom_pat_c = 2 then pat_id
    end as myc_message_sender,
    case
        when tofrom_pat_c = 1 then pat_id
        when tofrom_pat_c = 2 then original_to
    end as myc_message_recipient,
    prov_id,
    from_user_id,
    to_user_id,
    original_to,
    modified_to,
    case
        when modified_to like 'P %' then ltrim(modified_to, 'P ')
        when original_to like 'P %' then ltrim(modified_to, 'P ')
    end as pool_id,
    request_subject,
    subject,
    dep.department_id,
    dep.department_name,
    dep.specialty_name,
    not_handled_time,
    final_handled_time,
    proxy_wpr_id,
    reply_direct_yn,
    notallow_reply_yn,
    first_action_c as first_action_id,
    first_action.name as first_action,
    first_action_tm,
    last_action_c as last_action_id,
    last_action.name as last_action,
    last_action_tm
from
    {{ source('clarity_ods', 'myc_mesg') }} as myc_mesg
    left join {{ source('clarity_ods', 'myc_mesg_frst_last') }} as myc_mesg_frst_last
        on myc_mesg.message_id = myc_mesg_frst_last.message_id
    left join {{ source('clarity_ods', 'zc_how_handled') }} as first_action
        on myc_mesg_frst_last.first_action_c = first_action.how_handled_c
    left join {{ source('clarity_ods', 'zc_how_handled') }} as last_action
        on myc_mesg_frst_last.last_action_c = last_action.how_handled_c
    left join {{ source('clarity_ods', 'zc_myc_msg_typ') }} as zc_myc_msg_typ
            on myc_mesg.myc_msg_typ_c = zc_myc_msg_typ.myc_msg_typ_c
    left join {{ ref ('dim_department') }} as dep
            on myc_mesg.department_id = dep.department_id
where
    created_time >= '2022-07-14'
