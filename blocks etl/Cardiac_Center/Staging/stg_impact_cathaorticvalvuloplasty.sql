select distinct
        surg_enc_id,
        case when aovdata.ip7200 = 1 then 1494
              when aovdata.ip7200 = 2 then 1496
              when aovdata.ip7200 = 4 then 1497
              else null end as avprocind,
        case when aovdata.ip7205 = 1 then 1482
              when aovdata.ip7205 = 2 then 1483
              when aovdata.ip7205 = 3 then 1484
              when aovdata.ip7205 = 4 then 1485
              when aovdata.ip7205 = 5 then 1486
              else null end as avmorphology,
        case when aovdata.ip7210 = 0 then 1489
              when aovdata.ip7210 = 1 then 1490
              when aovdata.ip7210 = 2 then 1491
              when aovdata.ip7210 = 3 then 1492
              when aovdata.ip7210 = 4 then 1493
              else null end as avpreinsuff,
        cast(aovdata.ip7215 as numeric(4, 1)) as avdiameter,
        cast(aovdata.ip7220 as numeric(5, 1)) as avprepksystgrad,
        ip2dvce.impuse as avdefecttreated
  from
       {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_aovdata')}} as aovdata
           on study.refno = aovdata.refno
       left join {{source('ccis_ods', 'sensis_ip2dvce')}} as ip2dvce
           on study.refno = ip2dvce.refno
