with
denials as (
    select
        pmt_eob_info_i.peob_mtch_chg_tx_id as tx_id,
        pmt_eob_info_i.tx_id as payment_tx_id,
        fol_info.fol_id,
        inv_basic_info.cvg_id,
        pmt_eob_info_i.tx_match_date as post_date,
        clarity_rmc.remit_code_id,
        clarity_rmc.remit_code_name,
        clarity_rmc.rpt_group_title,
        MIN(pmt_eob_info_ii.line) as action_line
    from
        {{ source('clarity_ods', 'pmt_eob_info_i') }} as pmt_eob_info_i
    -- inner join open_ar on
    --     open_ar.tx_id = pmt_eob_info_i.peob_mtch_chg_tx_id
    left join {{ source('clarity_ods', 'pmt_eob_info_ii') }} as pmt_eob_info_ii on
        pmt_eob_info_ii.tx_id = pmt_eob_info_i.tx_id
        and pmt_eob_info_ii.eob_i_line_number = pmt_eob_info_i.line
    left outer join
        {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions on
            arpb_transactions.tx_id = pmt_eob_info_i.peob_mtch_chg_tx_id
    left outer join {{ source('clarity_ods', 'clarity_rmc') }} as clarity_rmc on
        clarity_rmc.remit_code_id = pmt_eob_info_ii.winningrmc_id
    left join {{ source('clarity_ods', 'inv_basic_info') }} as inv_basic_info on
        inv_basic_info.inv_num = pmt_eob_info_i.invoice_num
    inner join {{ source('clarity_ods', 'fol_info') }} as fol_info on
        fol_info.transaction_id = arpb_transactions.tx_id
    inner join
        {{ source('clarity_ods', 'fol_reason_code') }} as fol_reason_code on
            fol_reason_code.fol_id = fol_info.fol_id
            and fol_info.coverage_id = inv_basic_info.cvg_id
            and fol_reason_code.reason_code_id = pmt_eob_info_ii.winningrmc_id
    group by
        pmt_eob_info_i.peob_mtch_chg_tx_id,
        pmt_eob_info_i.tx_id,
        fol_info.fol_id,
        inv_basic_info.cvg_id,
        pmt_eob_info_i.tx_match_date,
        clarity_rmc.remit_code_id,
        clarity_rmc.remit_code_name,
        clarity_rmc.rpt_group_title
),

denials2 as (
    select
        tx_id,
        payment_tx_id,
        post_date,
        action_line,
        remit_code_id,
        remit_code_name,
        rpt_group_title,
        cvg_id,
        ROW_NUMBER() over( partition by
            tx_id,
            fol_id,
            post_date
            order by
                payment_tx_id asc,
                fol_id asc,
                post_date asc,
                action_line asc
        ) as seq_num,
        MIN(post_date) over (partition by tx_id, cvg_id) as min_date,
        MAX(post_date) over (partition by tx_id, cvg_id) as max_date
    from
        denials
),

denials3 as (
    select
        denials2.tx_id,
        denials2.cvg_id,
        MAX(
            case
                when
                    denials2.post_date = denials2.min_date and denials2.seq_num = 1 then denials2.post_date
            end
        ) as first_rej_post_dt_key,
        MAX(
            case
                when
                    denials2.post_date = denials2.min_date and denials2.seq_num = 1 then denials2.rpt_group_title
            end
        ) as first_rej_cat_seq_1,
        MAX(
            case
                when
                    denials2.post_date = denials2.min_date and denials2.seq_num = 1 then denials2.remit_code_id
            end
        ) as first_rej_cd_seq_1,
        MAX(
            case
                when
                    denials2.post_date = denials2.min_date and denials2.seq_num = 1 then denials2.remit_code_name
            end
        ) as first_rej_desc_seq_1,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 1 then denials2.post_date
            end
        ) as last_rej_post_dt_key,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 1 then denials2.rpt_group_title
            end
        ) as last_rej_cat_seq_1,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 1 then denials2.remit_code_id
            end
        ) as last_rej_cd_seq_1,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 1 then denials2.remit_code_name
            end
        ) as last_rej_desc_seq_1,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 2 then denials2.rpt_group_title
            end
        ) as last_rej_cat_seq_2,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 2 then denials2.remit_code_id
            end
        ) as last_rej_cd_seq_2,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 2 then denials2.remit_code_name
            end
        ) as last_rej_desc_seq_2,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 3 then denials2.rpt_group_title
            end
        ) as last_rej_cat_seq_3,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 3 then denials2.remit_code_id
            end
        ) as last_rej_cd_seq_3,
        MAX(
            case
                when
                    denials2.post_date = denials2.max_date and denials2.seq_num = 3 then denials2.remit_code_name
            end
        ) as last_rej_desc_seq_3
    from
        denials2
    group by
        denials2.tx_id,
        denials2.cvg_id
)

select
    arpb_transactions.tx_id,
    denials3.cvg_id,
    denials3.first_rej_post_dt_key,
    denials3.first_rej_cat_seq_1,
    denials3.first_rej_cd_seq_1,
    denials3.first_rej_desc_seq_1,
    denials3.last_rej_post_dt_key,
    denials3.last_rej_cat_seq_1,
    denials3.last_rej_cd_seq_1,
    denials3.last_rej_desc_seq_1,
    denials3.last_rej_cat_seq_2,
    denials3.last_rej_cd_seq_2,
    denials3.last_rej_desc_seq_2,
    denials3.last_rej_cat_seq_3,
    denials3.last_rej_cd_seq_3,
    denials3.last_rej_desc_seq_3
from
    {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions
left join denials3
    on arpb_transactions.tx_id = denials3.tx_id
where
    arpb_transactions.tx_type_c = 1
    and arpb_transactions.void_date is null
