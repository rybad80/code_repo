select
    fs.stats_cd,
    ms.stat_nm,
    fs.patient_type,
    h.hsp_acct_key,
    h.hsp_acct_id,
    hav.visit_key as hsp_acct_pri_visit_key,
    c.cost_cntr_key,
    c.cost_cntr_cd,
    c.cost_cntr_nm,
    fs.post_dt_key,
    md.full_dt as post_dt,
    substr(fs.credit_gl_num, 16, 4) as gl_payor,
    sum(fs.stat_measure) as stat_measure
from
    (
        (
            (
                (
                    (
                        {{source('cdw', 'fact_financial_statistic')}} as fs
                        left join {{source('cdw', 'cost_center')}} as c on ((fs.cost_cntr_key = c.cost_cntr_key))
                    )
                    left join {{source('cdw', 'master_statistic')}} as ms on ((int8("VARCHAR"(fs.stats_cd)) = ms.stat_cd))
                )
                left join {{source('cdw', 'master_date')}} as md on ((fs.post_dt_key = md.dt_key))
            )
            left join {{source('cdw', 'hospital_account')}} as h on ((fs.fs_acct_key = h.hsp_acct_key))
        )
        left join {{source('cdw', 'hospital_account_visit')}} as hav on (
            (
                (h.hsp_acct_key = hav.hsp_acct_key)
                and (hav.pri_visit_ind = 1)
            )
        )
    )
where
    (int4("VARCHAR"(fs.stats_cd)) = 8)
group by
    fs.stats_cd,
    ms.stat_nm,
    fs.patient_type,
    h.hsp_acct_key,
    h.hsp_acct_id,
    hav.visit_key,
    c.cost_cntr_key,
    c.cost_cntr_cd,
    c.cost_cntr_nm,
    fs.post_dt_key,
    md.full_dt,
    substr(fs.credit_gl_num, 16, 4)
