select
    pat.pat_mrn_id as mrn,
    mo.med_ord_nm as medication_order_name,
    mo.med_ord_desc as medication_order_description,
    v.enc_id as encounter_id,
    md.full_dt as order_date,
    mo.start_dt as medication_start_date,
    mo.end_dt as medication_end_date,
    med.generic_nm as generic_name,
    dict1.dict_nm as order_class,
    dict2.dict_nm as therapeutic_class,
    dict3.dict_nm as pharmacological_class,
    dict4.dict_nm as pharmacological_subclass,
    mo.sig
from
    {{source('cdw', 'medication_order')}} as mo
    join {{source('cdw', 'patient')}} as pat on ((mo.pat_key = pat.pat_key))
    join {{source('cdw', 'visit')}} as v on ((mo.visit_key = v.visit_key))
    join {{source('cdw', 'master_date')}} as md on ((mo.med_ord_dt_key = md.dt_key))
    join {{source('cdw', 'cdw_dictionary')}} as dict1 on ((mo.dict_ord_class_key = dict1.dict_key))
    join {{source('cdw', 'medication')}} as med on ((mo.med_key = med.med_key))
    join {{source('cdw', 'cdw_dictionary')}} as dict2 on ((med.dict_thera_class_key = dict2.dict_key))
    join {{source('cdw', 'cdw_dictionary')}} as dict3 on ((med.dict_pharm_class_key = dict3.dict_key))
    join {{source('cdw', 'cdw_dictionary')}} as dict4 on ((med.dict_pharm_subclass_key = dict4.dict_key))
    join {{source('cdw_customer', 'cohort_cag_mrn')}} as cag_mrn on ((cag_mrn.mrn = pat.pat_mrn_id))
