select
    pat.pat_mrn_id as mrn,
    v.enc_id as encounter_id,
    md.full_dt as contact_date,
    dx.icd9_cd,
    dx.icd10_cd,
    dx.dx_nm as diagnosis_name,
    case
        when (
            dict3.dict_nm ~~ like_escape(
                '%PRIMARY%'::"VARCHAR",
                '\'::"VARCHAR"
            )
        ) then ' YES '::"VARCHAR"
        else ' NO '::"VARCHAR"
    end as primary_diagnosis,
    dict1.dict_nm as encounter_type,
    dep.dept_nm as department_name,
    dict2.dict_nm as diagnosis_data_source
from
    {{source('cdw', 'visit')}} as v
    join {{source('cdw', 'patient')}} as pat on ((pat.pat_key = v.pat_key))
    join {{source('cdw', 'cdw_dictionary')}} as dict1 on ((v.dict_enc_type_key = dict1.dict_key))
    left join {{source('cdw', 'visit_diagnosis')}} as vdx on ((vdx.visit_key = v.visit_key))
    left join {{source('cdw', 'diagnosis')}} as dx on ((vdx.dx_key = dx.dx_key))
    left join {{source('cdw', 'cdw_dictionary')}} as dict2 on ((vdx.dict_dx_type_key = dict2.dict_key))
    left join {{source('cdw', 'cdw_dictionary')}} as dict3 on ((vdx.dict_dx_sts_key = dict3.dict_key))
    join {{source('cdw', 'department')}} as dep on ((v.eff_dept_key = dep.dept_key))
    join {{source('cdw', 'master_date')}} as md on ((v.contact_dt_key = md.dt_key))
    join {{source('cdw_customer', 'cohort_cag_mrn')}} as cag_mrn on ((cag_mrn.mrn = pat.pat_mrn_id))
