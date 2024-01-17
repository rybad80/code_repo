with device as (
select
        study.refno,
        coadfect.seqno,
        surg_enc_id,
        ipdevce.impact as coarcdevid,
        case when coadfect.ip7140 = 1 then 1504
              when coadfect.ip7140 = 2 then 1505
              else null end as coarcdevtype,
        case when coadfect.ip7145 = 1 then 1502
              when coadfect.ip7145 = 2 then 1500
              when coadfect.ip7145 = 3 then 1503
              when coadfect.ip7145 = 4 then 1501
              else null end as coarcballpurp,
        coadfect.ip7150 as coarcballpressure,
        case when coadfect.ip7155 = 1 then 1498
              when coadfect.ip7155 = 2 then 1499
        else null end as coarcballoutcome,
        case when coadfect.ip7160 = 1 then 1512
              when coadfect.ip7160 = 2 then 1513
              when coadfect.ip7160 = 3 then 1514
        else null end as coarcstentoutcome,
        cast(coadfect.ip7165 as numeric(4, 1)) as coarcpostinstentdiameter,
        cast(coadfect.ip7135 as integer) as sortorder,
        case when coadfect.ip7165 is not null
             then '1' else '0' end as coarcpostinstentdiamassessed
from
     {{ref('stg_impact_cathstudy')}} as study
     inner join {{source('ccis_ods', 'sensis_coadfect')}} as coadfect
         on study.refno = coadfect.refno
     inner join {{source('ccis_ods', 'sensis_ipdevce')}} as ipdevce
         on coadfect.refno = ipdevce.refno and coadfect.seqno = ipdevce.seqno
)

select  study.surg_enc_id,
        coarcdevid,
        coarcdevtype,
        coarcballpurp,
        coarcballpressure,
        coarcballoutcome,
        coarcstentoutcome,
        coarcpostinstentdiameter,
        row_number() over (partition by study.refno order by sortorder, seqno) as sort,
        coarcpostinstentdiamassessed
from
    {{ref('stg_impact_cathstudy')}} as study
    inner join device on study.refno = device.refno
