select
    surg_enc_id,
    coalesce(ippoevnt.ip8000, 2) as carrest,
    coalesce(ippoevnt.ip8005, 2) as postarrhyth,
    case when ippoevnt.ip8005 = 1 then coalesce(ippoevnt.ip8010, 2)
          else null end as postarrhythmed,
    case when ippoevnt.ip8005 = 1 then coalesce(ippoevnt.ip8015, 2)
          else null end as postarrhythcardiovers,
    case when ippoevnt.ip8005 = 1 then coalesce(ippoevnt.ip8020, 2)
          else null end as postarrhythtemppm,
    case when ippoevnt.ip8005 = 1 then coalesce(ippoevnt.ip8050, 2)
          else null end as postarrhythpermpm,
    coalesce(ippoevnt.ip8030, 2) as postnewregurge,
    coalesce(ippoevnt.ip8035, 2) as posttamponade,
    coalesce(ippoevnt.ip8040, 2) as postairembolus,
    coalesce(ippoevnt.ip8045, 2) as postembstroke,
    coalesce(ippoevnt.ip8050, 2) as postdevmalposthrom,
    coalesce(ippoevnt.ip8055, 2) as postdevembol,
    case when ippoevnt.ip8055 = 1 then coalesce(ippoevnt.ip8060, 2)
          else null end as postdevretrievepct,
    case when ippoevnt.ip8055 = 1 then coalesce(ippoevnt.ip8065, 2)
          else null end as postdevretrievesurg,
    coalesce(ippoevnt.ip8070, 2) as postdialysis,
    coalesce(ippoevnt.ip8075, 2) as postintubation,
    coalesce(ippoevnt.ip8080, 2) as postecmo,
    coalesce(ippoevnt.ip8085, 2) as postlvad,
    coalesce(ippoevnt.ip8090, 2) as postbleed,
    case when ippoevnt.ip8090 = 1 then coalesce(ippoevnt.ip8095, 2)
          else null end as postbleedaccesssite,
    case when ippoevnt.ip8090 = 1 then coalesce(ippoevnt.ip8100, 2)
          else null end as  postbleedhematoma,
    case when ippoevnt.ip8090 = 1 then coalesce(ippoevnt.ip8110, 2)
          else null end as  postretrobleed,
    case when ippoevnt.ip8090 = 1 then coalesce(ippoevnt.ip8115, 2)
          else null end as postgibleed,
    case when ippoevnt.ip8090 = 1 then coalesce(ippoevnt.ip8120, 2)
          else null end as postgubleed,
    case when ippoevnt.ip8090 = 1 then coalesce(ippoevnt.ip8125, 2)
          else null end as postotherbleed,
    coalesce(ippoevnt.ip8130, 2) as posttransfusion,
    coalesce(ippoevnt.ip8140, 2) as postothervascomp,
    coalesce(ippoevnt.ip8145, 2) as postotherevents,
    coalesce(ippeven2.ip8155, 2) as postplancardiacsurg,
    coalesce(ippeven2.ip8160, 2) as postunplancardsurg,
    coalesce(ippeven2.ip8165, 2) as postunplanvassurg,
    coalesce(ippeven2.ip8170, 2) as postunplanothersurg,
    case when ippeven2.ip8170 = 1 then coalesce(ippeven2.ip8175, 2)
          else null end as postothersurgcathcomp,
    coalesce(ippeven2.ip8180, 2) as postsubscath,
    case when ippoevnt.ip8005 = 1 then coalesce(ippoevnt.ip8006, 2)
          else null end as postavblock,
    coalesce(ippoevnt.ip8007, 2) as postarrhythresolved,
    coalesce(ippoevnt.ip8071, 2) as postcorarterycomp,
    coalesce(ippoevnt.ip8072, 2) as posterosion,
    coalesce(ippoevnt.ip8073, 2) as postesofistula,
    coalesce(ippoevnt.ip8074, 2) as postlbbb,
    coalesce(ippoevnt.ip8076, 2) as postrbbb,
    case when ippoevnt.ip8130 = 1 then coalesce(ippoevnt.ip8131, 2)
          else null end as postdrophgb,
    case when ippoevnt.ip8130 = 1 then coalesce(ippoevnt.ip8132, 2)
          else null end as  postprioranemia,
    case when ippoevnt.ip8130 = 1 then coalesce(ippoevnt.ip8133, 2)
          else null end as  postbloodloss,
    case when ippoevnt.ip8130 = 1 then coalesce(ippoevnt.ip8134, 2)
          else null end as  postecmobloodreplace,
    coalesce(ippoevnt.ip8200, 2) as postperinerveinjury,
    coalesce(ippoevnt.ip8205, 2) as postphnerveparalysis,
    coalesce(ippoevnt.ip8210, 2) as postpneumothorax,
    coalesce(ippoevnt.ip8215, 2) as postpulembolism,
    coalesce(ippoevnt.ip8220, 2) as postpulveinstenosis,
    coalesce(ippoevnt.ip8225, 2) as postradiationburn,
    coalesce(ippoevnt.ip8230, 2) as postdvt,
    coalesce(ippoevnt.ip8235, 2) as postconduittear,
    case when ippoevnt.ip8236 = 1 then 4091
          when ippoevnt.ip8236 = 2 then 4092
          when ippoevnt.ip8236 = 3 then 4093
          else null end as postconduittearloc,
    case when ippoevnt.ip8237 = 1 then 4094
          when ippoevnt.ip8237 = 2 then 4161
          when ippoevnt.ip8237 = 3 then 4095
          when ippoevnt.ip8237 = 4 then 4096
          when ippoevnt.ip8237 = 5 then 4162
          else null end as postconduitteartreat

from
     {{ref('stg_impact_cathstudy')}} as study
     left join {{source('ccis_ods',  'sensis_ippeven2')}} as ippeven2
         on study.refno = ippeven2.refno
     left join {{source('ccis_ods',  'sensis_ippoevnt')}} as ippoevnt
         on study.refno = ippoevnt.refno
