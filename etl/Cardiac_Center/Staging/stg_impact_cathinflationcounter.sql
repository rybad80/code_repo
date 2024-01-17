with aov_single as (
select
        study.refno,
        surg_enc_id,
        case when sbalteq.refno is not null then 1487
              else null end as balltech,
        singipdevce.impact as singdevid,
        sbalteq.ip7245 as singballstab,
        sbalteq.ip7250 as singballpressure,
        case when singipdevce.ip7090 = 4 then 1498
              when singipdevce.ip7090 = 5 then 1499
          else null end as singballoutcome,
        null as singballpostpksysgrad,
        null as singballpostinsuff,
        null as doubdevid1,
        null as doubballstab1,
        null as doubballpressure1,
        null as doubballoutcome1,
        null as doubdevid2,
        null as doubballstab2,
        null as doubballpressure2,
        null as doubballoutcome2,
        null as doubballpostpksysgrad,
        null as doubballpostinsuff,
        sbalteq.ip7230 as seq,
        cast(sbalteq.ip7260 as numeric(4, 1)) as postdilsysgrad,
        case when ip7265 = 0 then 4024
              when ip7265 = 1 then 4025
              when ip7265 = 2 then 4026
              when ip7265 = 3 then 4027
              when ip7265 = 4 then 4028
              else null end as postdilregurg

  from {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_aovdata')}} as aovdata
           on study.refno = aovdata.refno
       inner join {{source('ccis_ods', 'sensis_sbalteq')}} as sbalteq
           on study.refno = sbalteq.refno
       inner join {{source('ccis_ods', 'sensis_ipdevce')}} as singipdevce
           on sbalteq.refno = singipdevce.refno
             and sbalteq.seqno = singipdevce.seqno
),
aov_double as (
   select
        study.refno,
        surg_enc_id,
        case when dbalteq.refno is not null then 1488
              else null end as balltech,
        null as singdevid,
        null as singballstab,
        null as singballpressure,
        null as singballoutcome,
        null as singballpostpksysgrad,
        null as singballpostinsuff,
        dblipdevce.impact as doubdevid1,
        dbalteq.ip7275 as doubballstab1,
        dbalteq.ip7280 as doubballpressure1,
        case when dblipdevce.ip7090 = 4 then 1498
              when dblipdevce.ip7090 = 5 then 1499
          else null end as doubballoutcome1,
        null as doubdevid2,
        dbalteq.ip7295 as doubballstab2,
        dbalteq.ip7300 as doubballpressure2,
        null as doubballoutcome2,
        null as doubballpostpksysgrad,
        null as doubballpostinsuff,
        dbalteq.ip7230 as seq,
        cast(dbalteq.ip7310 as numeric(4, 1)) as postdilsysgrad,
        case when dbalteq.ip7315 = 0 then 1489
              when dbalteq.ip7315 = 1 then 1490
              when dbalteq.ip7315 = 2 then 1491
              when dbalteq.ip7315 = 3 then 1492
              when dbalteq.ip7315 = 4 then 1493
        else null end as postdilregurg

  from {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_aovdata')}} as aovdata
           on study.refno = aovdata.refno
       inner join {{source('ccis_ods', 'sensis_dbalteq')}} as dbalteq
           on study.refno = dbalteq.refno
       inner join {{source('ccis_ods', 'sensis_ipdevce')}} as dblipdevce
           on dbalteq.refno = dblipdevce.refno
             and dbalteq.seqno = dblipdevce.seqno
),
unionall as (
select *
  from
    aov_single
      union all
select *
  from
    aov_double
)


select
        surg_enc_id,
        balltech,
        singdevid,
        singballstab,
        singballpressure,
        singballoutcome,
        singballpostpksysgrad,
        singballpostinsuff,
        doubdevid1,
        doubballstab1,
        doubballpressure1,
        doubballoutcome1,
        doubdevid2,
        doubballstab2,
        doubballpressure2,
        doubballoutcome2,
        doubballpostpksysgrad,
        doubballpostinsuff,
        row_number() over (partition by surg_enc_id order by seq, singdevid) as sort,
        postdilsysgrad,
        postdilregurg

from
      unionall
 where
      singdevid is not null
