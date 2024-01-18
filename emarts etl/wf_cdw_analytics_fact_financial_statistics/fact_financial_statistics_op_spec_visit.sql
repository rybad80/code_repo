select
    ft.acct_key,
    ft.bill_prov_key,
    ft.dept_key,
    ft.det_type_key,
    l.loc_key,
    m.mstr_op_spec_visit_cat_key,
    ft.orig_svc_dt_key,
    ft.pat_key,
    pr.proc_key,
    ft.post_dt_key,
    ft.pos_key,
    ft.chrg_tx_id,
    sum(ft.chrg_amt) as chrg_amt,
    sum(ft.proc_qty) as proc_qty,
    case when (l.loc_nm in ('CHCA NJ RL', 'CHCA PA RL', 'CSA NJ RL', 'CSA PA RL') or (l.loc_nm = 'CAA PA RL' and d.dept_id = 89120003)) then sum(ft.proc_qty) else 0 end as stat_measure
    --and PR.PROC_CD in ( '92002','92004','92012','92014','99024','99201','99202','99203','99204','99205','99211','99212','99213','99214','99215','99241','99242','99243','99244','99245','99381','99382','99383','99384','99385','99386','99387','99391','99392','99393','99394','99395','99396','99397')
from
    {{source('cdw', 'fact_transaction')}} as ft
    inner join {{source('cdw', 'department')}} as d on ft.dept_key = d.dept_key
    inner join {{source('cdw', 'location')}} as l on d.rev_loc_key = l.loc_key
    inner join {{source('cdw', 'procedure')}} as pr on ft.proc_key = pr.proc_key
    inner join {{source('cdw_analytics', 'master_financial_statistic_op_spec_visit_proc')}} as m
        on ft.proc_key = m.mstr_op_spec_visit_proc_key
where
    det_type_key in (1, 10)
    and ft.proc_qty < 900 and ft.proc_qty > -900
    and ft.post_dt_key >= 20090701
    and (loc_nm in ('CHCA NJ RL', 'CHCA PA RL', 'CSA NJ RL', 'CSA PA RL')
        or (loc_nm = 'CAA PA RL' and d.dept_id = 89120003))
group by
    ft.acct_key,
    l.loc_key,
    pr.proc_key,
    ft.post_dt_key,
    ft.orig_svc_dt_key,
    ft.pat_key,
    ft.det_type_key,
    ft.dept_key,
    ft.bill_prov_key,
    l.loc_nm,
    m.mstr_op_spec_visit_cat_key,
    ft.pos_key,
    d.dept_id,
    ft.chrg_tx_id
