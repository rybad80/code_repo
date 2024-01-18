select
    ma.med_ord_key as "Medication Order Key",
    ma.vsi_key as "Visit Stay Info Key",
    ma.seq_num as "MAR Line Count",
    ma.action_dt as "Medication Taken Date",
    ma.saved_dt as "Medication Saved Date",
    ma.cmt as "MAR Comment",
    ma.dose as "MAR Dose",
    dict1.dict_nm as "MAR Dosage Unit",
    dict2.dict_nm as "MAR Reason Not Administered",
    dict3.dict_nm as "MAR Result Of Administration",
    dict4.dict_nm as "MAR Site Administered",
    ma.create_by as "MAR Data Source",
    dim1.ovr_link_stat_nm as "Override Link Status"
from
    {{source('cdw', 'medication_administration')}} ma
    join {{source('cdw', 'dim_override_link_status')}} dim1 on ((dim1.dim_ovr_link_stat_key = ma.dim_ovr_link_stat_key))
    left join {{source('cdw', 'cdw_dictionary')}} dict1 on ((dict1.dict_key = ma.dict_dose_unit_key))
    left join {{source('cdw', 'cdw_dictionary')}} dict2 on ((dict2.dict_key = ma.dict_rsn_key))
    left join {{source('cdw', 'cdw_dictionary')}} dict3 on ((dict3.dict_key = ma.dict_rslt_key))
    left join {{source('cdw', 'cdw_dictionary')}} dict4 on ((dict4.dict_key = ma.dict_site_key))
where
    (ma.create_by = 'CLARITY' :: "VARCHAR")