select
       coalesce(emrlinkid.casenumber, cases.casenumber) as casenumber,
       primemedname,
       primemeddose,
       primemedvol,
       row_number() over (partition by coalesce(emrlinkid.casenumber, cases.casenumber) order by sort) as sortnum
from
      {{ref('stg_pediperform_medications_union')}} as stg_pediperform_medications_union
      left join {{source('ccis_ods', 'centripetus_cases')}} as cases
          on cases.caselinknum = stg_pediperform_medications_union.log_id
      left join {{source('ccis_ods', 'centripetus_emrlinkid')}} as emrlinkid
          on emrlinkid.emreventid = stg_pediperform_medications_union.log_id
where
     primemeddose is not null
     and primemedname is not null
