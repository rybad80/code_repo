select
        surg_enc_id,
        case when coadata.ip7100 = 1 then 1506
             when coadata.ip7100 = 2 then 1509
             when coadata.ip7100 = 3 then 1507
             when coadata.ip7100 = 4 then 1510
             when coadata.ip7100 = 5 then 1508
             when coadata.ip7100 = 6 then 1511
             when coadata.ip7100 = 7 then 4160
         else null end as coarcprocind,
        cast(coadata.ip7105 as numeric(4, 1)) as coarcprediameter,
        cast(coadata.ip7110 as numeric(4, 1)) as coarcprepksysgrad,
        coadata.ip7115 as coarcdefecttreated,
        cast(coadata.ip7120 as numeric(4, 1)) as coarcpostdiameter,
        cast(coadata.ip7125 as numeric(4, 1)) as coarcpostpksysgrad,
        case when coadata.cpotoc = 1 then 4029
              when coadata.cpotoc = 2 then 4030
              else null end as coarcnature,
        case when coadata.ip7102 = 1 then 4031
              when coadata.ip7102 = 1 then 4032
              else null end as coarcpriortreat,
        coadata.ip7126 as coarcaddlaortobs,
        coadata.ip7127 as coarcaorticarchinter,
        cast(coadata.ip7128 as numeric(4, 1)) as coarcpresysgradient,
        cast(coadata.ip7129 as numeric(4, 1)) as coarcpostsysgradient,
        case when coadata.ip7105 is null then 1 else 0 end as coarcprediameterna,
        case when coadata.ip7110 is null then 1 else 0 end as coarcprepksysgradna,
        case when coadata.ip7120 is null then 1 else 0 end as coarcpostdiameterna,
        case when coadata.ip7125 is null then 1 else 0 end as coarcpostpksysgradna
from
    {{ref('stg_impact_cathstudy')}} as study
    inner join {{source('ccis_ods', 'sensis_coadata')}} as coadata
        on study.refno = coadata.refno
