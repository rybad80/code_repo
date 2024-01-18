select
        surg_enc_id,
        case when asdata.ip7000 = 1 then 1470
              when asdata.ip7000 = 2 then 1473
              when asdata.ip7000 = 3 then 1476
              when asdata.ip7000 = 4 then 1471
              when asdata.ip7000 = 5 then 1474
              when asdata.ip7000 = 6 then 1477
              when asdata.ip7000 = 7 then 1472
              when asdata.ip7000 = 8 then 1475
              when asdata.ip7000 = 9 then 4159
              else null end as asdprocind,
        cast(asdata.ip7005 as numeric(4, 1)) as asdseptlength,
        asdata.ip7010 as asdaneurysm,
        asdata.ip7015 as asdmultifen,
        case when asdata.ip7005 is null then 1 else 0 end as asdseptlengthna
  from
      {{ref('stg_impact_cathstudy')}} as study
      inner join {{source('ccis_ods', 'sensis_asdata')}} as asdata
          on study.refno = asdata.refno
