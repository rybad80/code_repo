select
        surg_enc_id,
        case when ip7600 = 1 then 1549
              when ip7600 = 2 then 1550
              when ip7600 = 3 then 1551
              else null end as pdaprocind,
        ip7605 as pdadiameteraortside,
        ip7610 as pdaminlumdiameter,
        ip7615 as pdalength,
        case when ip7620 = 1 then 1544
              when ip7620 = 2 then 1545
              when ip7620 = 3 then 1546
              when ip7620 = 4 then 1547
              when ip7620 = 5 then 1548
              else null end as pdaclass,
        null as pdadefecttreated,
        case when ip7630 = 2 then 4182
              when ip7630 = 1 then 4183
              else 4184 end as pdapaobst,
        case when ip7635 = 2 then 4182
              when ip7635 = 1 then 4183
              else 4184 end as pdaaortobst,
        case when ip7640 = 1 then 1480
              when ip7640 = 2 then 1481
              else null end as pdaresshunt
  from
        {{ref('stg_impact_cathstudy')}} as study
        inner join {{source('ccis_ods', 'sensis_ipdindic')}} as ipdindic
            on study.refno = ipdindic.refno
