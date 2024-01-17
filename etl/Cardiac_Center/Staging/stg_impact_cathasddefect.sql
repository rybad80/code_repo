select
        surg_enc_id,
        case when lower(asdata.ip7025) = 'no' then null
              else cast(asdata.ip7025 as numeric(4, 1)) end as asdsize,
        asdefct.ip7030 as asdballsizperf,
        case when asdefct.ip7040 > 0 then 1 else 2 end as asdstretchdiameter,
        asdefct.ip7040 as asdstretchdiametersize,
        asdefct.ip7045 as asdstopflowtech,
        asdefct.ip7050 as asdstopflowtechsize,
        asdefct.ip7055 as asdrimmeas,
        cast(asdefct.ip7060 as numeric(4, 1)) as asdivcrimlength,
        cast(asdefct.ip7065 as numeric(4, 1)) as asdaortrimlength,
        case when asdefct.ip7080 = 1 then 1480
              when asdefct.ip7080 = 2 then 1481
              else null end as asdresshunt,
        row_number() over (partition by surg_enc_id order by asdefct.seqno) as sort,
        cast(asdefct.ip7066 as numeric(4, 1)) as asdpostrimlength,
        asdata.ip7015 as asdmultifenestrated
  from
       {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_asdefct')}} as asdefct
           on study.refno = asdefct.refno
       inner join {{source('ccis_ods', 'sensis_asdata')}} as asdata
           on study.refno = asdata.refno
