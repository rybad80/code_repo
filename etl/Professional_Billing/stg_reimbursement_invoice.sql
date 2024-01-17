with
max_invoice as (
    select
        arpb_transactions.tx_id,
        inv_clm_ln_addl.invoice_num,
        inv_clm_ln_addl.eob_icn as eob_icn,
        MAX(inv_clm_ln_addl.invoice_num) over (partition by arpb_transactions.tx_id) as max_invoice_number

    from
        {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions
    inner join
        {{ source('clarity_ods', 'inv_num_tx_pieces') }} as inv_num_tx_pieces on
            arpb_transactions.tx_id = inv_num_tx_pieces.tx_id
    inner join
        {{ source('clarity_ods', 'inv_clm_ln_addl') }} as inv_clm_ln_addl on
            inv_num_tx_pieces.inv_id = inv_clm_ln_addl.invoice_id
            and inv_num_tx_pieces.line = inv_clm_ln_addl.line
    where
        arpb_transactions.tx_type_c = 1
        and arpb_transactions.void_date is null
)

select
    arpb_transactions.tx_id,
    max_invoice.max_invoice_number,
    max_invoice.eob_icn
from
    {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions
left join max_invoice on
    max_invoice.tx_id = arpb_transactions.tx_id
    and max_invoice.invoice_num = max_invoice.max_invoice_number
where
    arpb_transactions.tx_type_c = 1
    and arpb_transactions.void_date is null
