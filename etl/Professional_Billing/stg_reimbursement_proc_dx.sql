select
    arpb_transactions.tx_id,
    clarity_eap.proc_name as proc_name,
    zc_eap_rpt_grp_10.name as cpt_cat,
    clarity_edg_dx1.dx_name as dx_one_name,
    clarity_edg_dx1.ref_bill_code as dx_one_icd,
    clarity_edg_dx2.dx_name as dx_two_name,
    clarity_edg_dx2.ref_bill_code as dx_two_icd,
    clarity_edg_dx3.dx_name as dx_three_name,
    clarity_edg_dx3.ref_bill_code as dx_three_icd,
    clarity_edg_dx4.dx_name as dx_four_name,
    clarity_edg_dx4.ref_bill_code as dx_four_icd,
    clarity_edg_dx5.dx_name as dx_five_name,
    clarity_edg_dx5.ref_bill_code as dx_five_icd,
    clarity_edg_dx6.dx_name as dx_six_name,
    clarity_edg_dx6.ref_bill_code as dx_six_icd
from
    {{ source('clarity_ods', 'arpb_transactions') }} as arpb_transactions
left join {{ source('clarity_ods', 'clarity_eap') }} as clarity_eap on
    clarity_eap.proc_id = arpb_transactions.proc_id
left join
    {{ source('clarity_ods', 'zc_eap_rpt_grp_10') }} as zc_eap_rpt_grp_10 on
        zc_eap_rpt_grp_10.rpt_grp_ten = clarity_eap.rpt_grp_ten
left join {{ source('clarity_ods', 'clarity_edg') }} as clarity_edg_dx1 on
    clarity_edg_dx1.dx_id = arpb_transactions.primary_dx_id
left join {{ source('clarity_ods', 'clarity_edg') }} as clarity_edg_dx2 on
    clarity_edg_dx2.dx_id = arpb_transactions.dx_two_id
left join {{ source('clarity_ods', 'clarity_edg') }} as clarity_edg_dx3 on
    clarity_edg_dx3.dx_id = arpb_transactions.dx_three_id
left join {{ source('clarity_ods', 'clarity_edg') }} as clarity_edg_dx4 on
    clarity_edg_dx4.dx_id = arpb_transactions.dx_four_id
left join {{ source('clarity_ods', 'clarity_edg') }} as clarity_edg_dx5 on
    clarity_edg_dx5.dx_id = arpb_transactions.dx_five_id
left join {{ source('clarity_ods', 'clarity_edg') }} as clarity_edg_dx6 on
    clarity_edg_dx6.dx_id = arpb_transactions.dx_six_id
where
    arpb_transactions.tx_type_c = 1
    and arpb_transactions.void_date is null
