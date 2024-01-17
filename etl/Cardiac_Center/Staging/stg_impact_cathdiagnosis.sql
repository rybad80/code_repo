select distinct
        surg_enc_id,
        prdg.ip4000 as preproccarddiagid,
        dicip4000.meaning as diagnosisname
  from
       {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_prdg')}} as prdg
           on study.refno = prdg.refno
       inner join {{source('ccis_ods', 'sensis_dicip4000')}} as dicip4000
           on dicip4000.code = prdg.ip4000
