with alldata as (
  select
        surg_enc_id,
        ipdevce.ip7089 as defectcounterassn,
        cast(seqno as int) as sort,
        case when impuse in (1, 2, 3) then 2004
          when impuse in (16, 17, 18) then 2009
          when impuse in (13, 14, 15) then 2008
            else impuse end as cathprocid
from
     {{ref('stg_impact_cathstudy')}} as study
     inner join {{source('ccis_ods', 'sensis_ipdevce')}} as ipdevce
         on study.refno = ipdevce.refno
where
     ipdevce.impact is not null
     and impuse in (1, 2, 3, 13, 14, 15, 16, 17, 18)


union all

select
        surg_enc_id,
        ip2dvce.ip7089 as defectcounterassn,
        cast(seqno as int) as sort,
        3625 as cathprocid
from
     {{ref('stg_impact_cathstudy')}} as study
     inner join {{source('ccis_ods', 'sensis_ip2dvce')}} as ip2dvce
         on study.refno = ip2dvce.refno
where ip2dvce.impact is not null
)

select
    surg_enc_id,
    defectcounterassn,
    row_number() over (partition by surg_enc_id order by sort) as sort_order,
    cathprocid
from
    alldata
