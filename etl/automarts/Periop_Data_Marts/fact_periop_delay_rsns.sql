select
    rsns.log_key,
    max(case when or_log_delays.seq_num = 1 then dict_delay_type.dict_nm else null end) as delay_1_type,
    max(case when rsns.seq_num = 1 then dict_delay_rsn.dict_nm else null end) as delay_1_reason,
    max(case when or_log_delays.seq_num = 1 then or_log_delays.delay_lgth else null end) as delay_1_length,
    max(
        case when or_log_delay_comments.seq_num = 1 then or_log_delay_comments.delay_cmt else null end
    ) as delay_1_comment,

    max(case when or_log_delays.seq_num = 2 then dict_delay_type.dict_nm else null end) as delay_2_type,
    max(case when rsns.seq_num = 2 then dict_delay_rsn.dict_nm else null end) as delay_2_reason,
    max(case when or_log_delays.seq_num = 2 then or_log_delays.delay_lgth else null end) as delay_2_length,
    max(
        case when or_log_delay_comments.seq_num = 2 then or_log_delay_comments.delay_cmt else null end
    ) as delay_2_comment,

    max(case when or_log_delays.seq_num = 3 then dict_delay_type.dict_nm else null end) as delay_3_type,
    max(case when rsns.seq_num = 3 then dict_delay_rsn.dict_nm else null end) as delay_3_reason,
    max(case when or_log_delays.seq_num = 3 then or_log_delays.delay_lgth else null end) as delay_3_length,
    max(case when rsns.seq_num = 3 then or_log_delay_comments.delay_cmt else null end) as delay_3_comment,

    max(case when or_log_delays.seq_num = 4 then dict_delay_type.dict_nm else null end) as delay_4_type,
    max(case when rsns.seq_num = 4 then dict_delay_rsn.dict_nm else null end) as delay_4_reason,
    max(case when or_log_delays.seq_num = 4 then or_log_delays.delay_lgth else null end) as delay_4_length,
    max(case when rsns.seq_num = 4 then or_log_delay_comments.delay_cmt else null end) as delay_4_comment,

    max(case when or_log_delays.seq_num = 5 then dict_delay_type.dict_nm else null end) as delay_5_type,
    max(case when rsns.seq_num = 5 then dict_delay_rsn.dict_nm else null end) as delay_5_reason,
    max(case when or_log_delays.seq_num = 5 then or_log_delays.delay_lgth else null end) as delay_5_length,
    max(case when rsns.seq_num = 5 then or_log_delay_comments.delay_cmt else null end) as delay_5_comment

from {{ ref('fact_periop') }} as fact_periop
    inner join {{ source('cdw', 'or_log_delay_reasons') }} as rsns
        on rsns.log_key = fact_periop.log_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_delay_rsn
            on dict_delay_rsn.dict_key = rsns.dict_or_delay_rsn_key
    inner join {{ source('cdw', 'or_log_delays') }} as or_log_delays on or_log_delays.log_key = rsns.log_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_delay_type on
            dict_delay_type.dict_key = or_log_delays.dict_or_delay_type_key
    left join {{ source('cdw', 'or_log_delay_comments') }} as or_log_delay_comments
        on or_log_delay_comments.log_key = rsns.log_key
        and or_log_delay_comments.seq_num = rsns.seq_num

where
    rsns.seq_num = or_log_delays.seq_num
    and rsns.create_by = 'CLARITY'

group by rsns.log_key
