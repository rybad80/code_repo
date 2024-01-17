select
        pat_mrn_id,
        min(gencond.ip3100) as digeorgesynd,
        min(gencond.ip3105) as alagillesynd,
        min(gencond.ip3110) as hernia,
        min(gencond.ip3125) as marfansynd,
        min(gencond.ip3115) as downsynd,
        min(gencond.ip3120) as heterotaxy,
        min(gencond.ip3130) as noonansynd,
        min(gencond.ip3135) as rubella,
        min(gencond.ip3140) as trisomy13,
        min(gencond.ip3145) as trisomy18,
        min(gencond.ip3150) as turnersynd,
        min(gencond.ip3155) as williamsbeurensynd,
        max(brthinf.prmbrth) as premature,
        max(brthinf.gstage) as gestageweeks,
        max(cast(brthinf.brthwt as decimal(5, 3))) as birthwtkg
from
     {{ref('stg_impact_cathstudy')}} as study
      left join {{source('ccis_ods', 'sensis_gencond')}} as gencond
             on study.refno = gencond.refno
      left join {{source('ccis_ods', 'sensis_brthinf')}} as brthinf
             on brthinf.refno = study.refno
where
      coalesce(brthinf.brthwt, 0) < 99
group by
      pat_mrn_id
