select distinct
        surg_enc_id,
        case when ip2hrsk.ip3161 = 1 then 4005
              when ip2hrsk.ip3161 = 2 then 4006
              when ip2hrsk.ip3161 = 3 then 4007
              when ip2hrsk.ip3161 = 4 then 4008
              when ip2hrsk.ip3161 = 5 then 4009
              when ip2hrsk.ip3161 = 6 then 4010
              when ip2hrsk.ip3161 = 7 then 4011
              when ip2hrsk.ip3161 = 8 then 4012
              when ip2hrsk.ip3161 = 9 then 4013
              when ip2hrsk.ip3161 = 10 then 4014
              when ip2hrsk.ip3161 = 11 then 4015
              when ip2hrsk.ip3161 = 12 then 4016
              when ip2hrsk.ip3161 = 13 then 4017
              when ip2hrsk.ip3161 = 14 then 4018
              when ip2hrsk.ip3161 = 15 then 4020
              when ip2hrsk.ip3161 = 16 then 4021
              else null end as arrhythmiahx,
        dicip3161.meaning as arrhythmiahxterm
  from
       {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_ip2hrsk')}} as ip2hrsk
           on study.refno = ip2hrsk.refno
       inner join {{source('ccis_ods', 'sensis_dicip3161')}} as dicip3161
           on dicip3161.code = ip2hrsk.ip3161
