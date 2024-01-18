with max_sbalteq as (
select sensis_sbalteq.refno,
        max(sensis_sbalteq.seqno) as seqno
   from
       {{source('ccis_ods', 'sensis_sbalteq')}} as sensis_sbalteq
group by
      sensis_sbalteq.refno
),

max_dbalteq as (
select sensis_dbalteq.refno,
        max(sensis_dbalteq.seqno) as seqno
   from
       {{source('ccis_ods', 'sensis_dbalteq')}} as sensis_dbalteq
group by
       sensis_dbalteq.refno
),

sbalteq as (
select
        sbalteq.refno,
        ip7230,
        ip7245,
        cpohi,
        ip7250,
        cpoebd,
        ip7260,
        ip7265
from {{source('ccis_ods', 'sensis_sbalteq')}} as sbalteq
     inner join  max_sbalteq
     on max_sbalteq.refno = sbalteq.refno and max_sbalteq.seqno = sbalteq.seqno
),

dbalteq as (
select
        dbalteq.refno,
        ip7230,
        ip7275,
        cpohi,
        ip7280,
        cpoebd,
        ip7295,
        cpohi2,
        ip7300,
        cpoebd2,
        ip7310,
        ip7315
from
     {{source('ccis_ods', 'sensis_dbalteq')}} as dbalteq
     inner join max_dbalteq
      on max_dbalteq.refno = dbalteq.refno and max_dbalteq.seqno = dbalteq.seqno
),

ipdevce as (
select *,
       row_number() over(partition by refno order by seqno) as row
   from
       {{source('ccis_ods', 'sensis_ipdevce')}}
)


select distinct
        surg_enc_id,
        case when pvdata.ip7400 = 1 then 1552
              when pvdata.ip7400 = 2 then 1553
              when pvdata.ip7400 = 3 then 1554
              when pvdata.ip7400 = 4 then 2011
        else null end as pvprocind,
        case when pvdata.ip7405 = 1 then 1555
              when pvdata.ip7405 = 2 then 1556
        else null end as pvmorphology,
        pvdata.ip7410 as pvsubstenosis,
        pvdata.ip7415 as pvdiameter,
        cast(pvdata.ip7420 as numeric(4, 1)) as pvprepksysgrad,
        null as pvdefecttreated,
        case when pvdata.ip7420 is null then 1 else 0 end as pvprepksysgradna,
        case when sbalteq.refno is not null then 1487
              when dbalteq.refno is not null then 1488 end as pvballtech,
        --,ipdindic.ip7525 pvball1devid
        null as pvball1devid, --sbalteq.ip7230 mapped through dicaccdev
        null as pvball2devid, --dbalteq.ip7230 mapped through dicaccdev
        coalesce(dbalteq.ip7275, sbalteq.ip7245) as pvballstab,
        coalesce(dbalteq.ip7280, sbalteq.ip7250) as pvballpressure,
        case when ipdevce.ip7090 = 4 then 1498
              when ipdevce.ip7090 = 5 then 1499
        else null end as pvballoutcome,
        cast(coalesce(dbalteq.ip7310, sbalteq.ip7260) as numeric(4, 1)) as pvpostpksysgrad,
        case when coalesce(dbalteq.ip7310, sbalteq.ip7260) is null then '1'
               else null end as pvpostpksysgradna

   from
     {{ref('stg_impact_cathstudy')}} as study
     inner join {{source('ccis_ods', 'sensis_pvdata')}} as pvdata
         on study.refno = pvdata.refno
     left join {{source('ccis_ods', 'sensis_ipdindic')}} as ipdindic
         on study.refno = ipdindic.refno
     left join sbalteq on sbalteq.refno = pvdata.refno
     left join dbalteq on dbalteq.refno = pvdata.refno
     left join ipdevce on study.refno = ipdevce.refno and row = 1
