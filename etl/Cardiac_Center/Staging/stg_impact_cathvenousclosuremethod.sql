select
      surg_enc_id,
      case
         when ip5100 = 772 then 1
         when ip5100 = 773 then 2
         when ip5100 = 774 then 3
         when ip5100 = 775 then 9
         when ip5100 = 776 then 72
         else null end as closure_method,
      row_number() over (partition by surg_enc_id order by ip5100) as sort
from
      {{ref('stg_impact_cathstudy')}} as study
      inner join {{source('ccis_ods', 'sensis_asr')}} as asr
         on study.refno = asr.refno
      left join {{source('ccis_ods', 'sensis_dices1te')}} as site
         on asr.entsit = site.code
where
     ip5100 is not null
     and lower(meaning) like '%vein%'
group by
     surg_enc_id,
     ip5100
