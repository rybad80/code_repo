select distinct
        surg_enc_id,
        pedpr.ip5010 as specificprocid
  from
       {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_pedpr')}} as pedpr
          on study.refno = pedpr.refno
 where
       ip5010 is not null
