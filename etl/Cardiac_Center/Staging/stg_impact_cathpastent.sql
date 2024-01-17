select
        surg_enc_id,
        case when ipppas.ip7700 = 1 then 1537
              when ipppas.ip7700 = 2 then 1540
              when ipppas.ip7700 = 3 then 1538
              when ipppas.ip7700 = 4 then 1541
              when ipppas.ip7700 = 5 then 1539
            else null end as pasprocind
  from
      {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_ipppas')}} as ipppas
          on study.refno = ipppas.refno
