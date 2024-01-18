select
        surg_enc_id,
        case when hemodyn.sasat is null then 1 else 0 end as systemicartsatna,
        cast(hemodyn.sasat as numeric(4, 1)) as systemicartsat,
        case when hemodyn.mvsat is null then 1 else 0 end as mixvensatna,
        hemodyn.mvsat as mixvensat,
        case when hemodyn.lvs is null then 1 else 0 end as systemventsyspresna,
        cast(hemodyn.lvs as numeric(4, 1)) as systemventsyspres,
        case when hemodyn.lvd is null then 1 else 0 end as systemventenddiapresna,
        hemodyn.lvd as systemventenddiapres,
        case when hemodyn.aos is null then 1 else 0 end as systemsysbpna,
        hemodyn.aos as systemsysbp,
        case when hemodyn.aod is null then 1 else 0 end as systemdiabpna,
        hemodyn.aod as systemdiabp,
        case when hemodyn.aom is null then 1 else 0 end as systemmeanbpna,
        hemodyn.aom as systemmeanbp,
        case when hemodyn.mpas is null then 1 else 0 end as pulmartsyspresna,
        cast(hemodyn.mpas as numeric(4, 1)) as pulmartsyspres,
        case when hemodyn.mpam is null then 1 else 0 end as pulmartmeanpresna,
		cast(case when hemodyn.mpam > 99.9
                  then 99 else hemodyn.mpam end as numeric(3, 1)) as pulmartmeanpres,
        case when hemodyn.rv is null then 1 else 0 end as pulmventsyspresna,
        hemodyn.rv as pulmventsyspres,
        case when hemodyn.pvr is null then 1 else 0 end as pulmvascrestindna,
        cast(hemodyn.pvr as numeric(3, 1)) as pulmvascrestind,
        case when hemodyn.ci is null then 1 else 0 end as cardindna,
        cast(hemodyn.ci as numeric(3, 1)) as cardind,
        case when hemodyn.qpqs is null then 1 else 0 end as qpqsrationa,
        cast(hemodyn.qpqs as numeric(3, 1)) as qpqsratio
  from
       {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods',  'sensis_hemodyn')}} as hemodyn
           on study.refno = hemodyn.refno
