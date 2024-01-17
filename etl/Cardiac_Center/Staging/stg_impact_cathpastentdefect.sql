select
        surg_enc_id,
        case when ipprdef.ip7710 = 1 then 1533
              when ipprdef.ip7710 = 2 then 1534
            else null end as pasdefectloc,
        null as pasostialstenosis,
        ipprdef.ip7720 as pasdisobstruction,
        ipprdef.ip7725 as passidejail,
        ipprdef.ip7730 as passidejailintended,
        case when ipprdef.ip7735 = 1 then 1542
              when ipprdef.ip7735 = 2 then 1543
         else null end as passidejailartery,
        ipprdef.ip7740 as pasdsidejaildecflow,
        cast(ipvent1.ip7745 as numeric(4, 1)) as paspreproxsyspress,
        cast(ipvent1.ip7750 as numeric(4, 1)) as paspredistsyspress,
        cast(ipvent1.ip7755 as numeric(4, 1)) as paspreproxmeanpress,
        cast(ipvent1.ip7760 as numeric(4, 1)) as paspredistmeanpress,
        cast(ipvent1.ip7765 as numeric(3, 1)) as paspreproxdiameter,
        cast(ipvent1.ip7770 as numeric(3, 1)) as paspredistdiameter,
        cast(ipvent1.ip7775 as numeric(3, 1)) as paspremindiameter,
        null as pasdefecttreated,
        cast(ipvent2.ip7785 as numeric(4, 1)) as paspostproxsyspress,
        cast(ipvent2.ip7790 as numeric(4, 1)) as paspostdistsyspress,
        cast(ipvent2.ip7795 as numeric(4, 1)) as paspostproxmeanpress,
        cast(ipvent2.ip7800 as numeric(4, 1)) as paspostdistmeanpress,
        cast(ipvent2.ip7805 as numeric(3, 1)) as paspostproxdiameter,
        cast(ipvent2.ip7810 as numeric(3, 1)) as paspostdistdiameter,
        cast(ipvent2.ip7815 as numeric(3, 1)) as paspostmindiameter,
        row_number() over (partition by surg_enc_id order by ipprdef.seqno) as sort
from
     {{ref('stg_impact_cathstudy')}} as study
     inner join {{source('ccis_ods', 'sensis_ipprdef')}} as ipprdef
         on study.refno = ipprdef.refno
     inner join {{source('ccis_ods', 'sensis_ipvent1')}} as ipvent1
         on study.refno = ipvent1.refno
            and ipprdef.seqno = ipvent1.seqno
     inner join {{source('ccis_ods', 'sensis_ipvent2')}} as ipvent2
         on study.refno = ipvent2.refno
            and ipprdef.seqno = ipvent2.seqno
