select
       surg_enc_id,
       case when impuse in (1, 2, 3) then 2004
          when impuse in (16, 17, 18) then 2009
          when impuse in (13, 14, 15) then 2008
            else impuse end as cathprocid,
        ipdevce.impact as devid,
        case when ipdevce.ip7090 = 1 then 1467
              when ipdevce.ip7090 in (2, 9, 10) then 1468
              when ipdevce.ip7090 = 3 then 1469
              when ipdevce.ip7090 = 4 then 1498
              when ipdevce.ip7090 = 5 then 1499
              when ipdevce.ip7090 = 6 then 4157
              when ipdevce.ip7090 = 7 then 1536
              when ipdevce.ip7090 = 8 then 4158
              else 1467 end as devoutcome,
        ipdevce.ip7089 as defectcounterassn,
        row_number() over (partition by surg_enc_id order by seqno) as sort
 from
     {{ref('stg_impact_cathstudy')}} as study
     inner join {{source('ccis_ods',  'sensis_ipdevce')}} as ipdevce
        on study.refno = ipdevce.refno
where
     ipdevce.impact is not null
     and impuse in (1, 2, 3, 13, 14, 15, 16, 17, 18)

union all

select
        surg_enc_id,
        3625 as cathprocid,
        ip2dvce.impact as devid,
        case when ip2dvce.i11135 = 1 then 4118
              when ip2dvce.i11135 = 2 then 4119
              when ip2dvce.i11135 = 3 then 4120
              when ip2dvce.i11135 = 4 then 4121
        else ip2dvce.i11135 end as devoutcome,
        ip2dvce.ip7089 as defectcounterassn,
        cast(seqno as int) as sort

 from
     {{ref('stg_impact_cathstudy')}} as study
     inner join {{source('ccis_ods',  'sensis_ip2dvce')}} as ip2dvce
        on study.refno = ip2dvce.refno
where
     ip2dvce.impact is not null
