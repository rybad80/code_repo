with visitinfo as (
    select
        fve.visit_key as "Encounter Key",
        fve.prev_hosp_visit_key as "Previous Encounter Key",
        d1.dict_nm as "Previous EncLast Stay Class",
        fve.prev_hosp_visit_days as "Days Since Previous Enc",
        case
            when (
                fve.prev_hosp_visit_hours < ('24' :: numeric(2, 0)) :: numeric(2, 0)
            ) then '< 1 Day' :: "VARCHAR"
            when (fve.prev_hosp_visit_days <= 3) then '1 - 3 Days' :: "VARCHAR"
            when (fve.prev_hosp_visit_days <= 5) then '4 - 5 Days' :: "VARCHAR"
            when (fve.prev_hosp_visit_days <= 10) then '6 - 10 Days' :: "VARCHAR"
            when (fve.prev_hosp_visit_days <= 15) then '11 - 15 Days' :: "VARCHAR"
            when (fve.prev_hosp_visit_days <= 30) then '16 - 30 Days' :: "VARCHAR"
            when (fve.prev_hosp_visit_days <= 60) then '31 - 60 Days' :: "VARCHAR"
            when (fve.prev_hosp_visit_days <= 90) then '61 - 90 Days' :: "VARCHAR"
            when (fve.prev_hosp_visit_days > 90) then '> 90 Days' :: "VARCHAR"
            else null :: "VARCHAR"
        end as "Revisit Days Category",
        fve.prev_hosp_visit_hours as "Hours Since Previous Enc",
        fve.next_hosp_visit_key as "Next Encounter Key",
        d2.dict_nm as "Next Enc Last Stay Class",
        fve.next_hosp_visit_days as "Days Until Next Enc",
        case
            when (
                fve.next_hosp_visit_hours < ('24' :: numeric(2, 0)) :: numeric(2, 0)
            ) then '< 1 Day' :: "VARCHAR"
            when (fve.next_hosp_visit_days <= 3) then '1 - 3 Days' :: "VARCHAR"
            when (fve.next_hosp_visit_days <= 7) then '4 - 7 Days' :: "VARCHAR"
            when (fve.next_hosp_visit_days <= 15) then '8 - 15 Days' :: "VARCHAR"
            when (fve.next_hosp_visit_days <= 30) then '16 - 30 Days' :: "VARCHAR"
            when (fve.next_hosp_visit_days <= 60) then '31 - 60 Days' :: "VARCHAR"
            when (fve.next_hosp_visit_days <= 90) then '61 - 90 Days' :: "VARCHAR"
            when (fve.next_hosp_visit_days > 90) then '> 90 Days' :: "VARCHAR"
            else null :: "VARCHAR"
        end as "Next Readmission Days Category",
        fve.next_hosp_visit_hours as "Hours Until Next Enc",
        case
            when (
                (fve.next_hosp_visit_key notnull)
                and (
                    upper(nextadmtyp.dict_nm) = 'ELECTIVE' :: "VARCHAR"
                )
            ) then 'YES' :: "VARCHAR"
            when (fve.next_hosp_visit_key notnull) then 'NO' :: "VARCHAR"
            else null :: "VARCHAR"
        end as "Next Encounter Elective"
    from
        {{ source('cdw', 'fact_visit_extension')}} as fve
        left join {{ source('cdw', 'cdw_dictionary')}} d1 on ((fve.dict_prev_visit_stay_class_key = d1.dict_key))
        left join {{ source('cdw', 'cdw_dictionary')}} d2 on ((fve.dict_next_visit_stay_class_key = d2.dict_key))
        left join {{ source('cdw', 'visit_addl_info')}} vainext on ((fve.next_hosp_visit_key = vainext.visit_key))
        left join {{ source('cdw', 'cdw_dictionary')}} nextadmtyp on ((vainext.dict_hsp_admsn_type_key = nextadmtyp.dict_key))
    where
        (
            (fve.prev_hosp_visit_days notnull)
            or (fve.next_hosp_visit_days notnull)
        )
),
prevencdiag as (
    select
        v."Encounter Key",
        v."Previous Encounter Key",
        dx.icd9_cd as "Previous Enc Disch ICD9",
        dx.icd10_cd as "Previous Enc Disch ICD10",
        dx.dx_nm as "Previous Enc Disch Diag",
        dx.ext_id as "Previous Enc Disch IMO"
    from
        visitinfo as v
        left join {{ source('cdw', 'visit_diagnosis')}} vdx on ((v."Previous Encounter Key" = vdx.visit_key))
        left join {{ source('cdw', 'cdw_dictionary')}} d3 on ((vdx.dict_dx_type_key = d3.dict_key))
        left join {{ source('cdw', 'diagnosis')}} as dx on ((vdx.dx_key = dx.dx_key))
    where
        (
            (vdx.seq_num = 1)
            and (
                d3.dict_nm = 'HSP_ACCT_DX_LIST - ACCT FINAL' :: "VARCHAR"
            )
        )
),
nextencdiag as (
    select
        v."Encounter Key",
        v."Next Encounter Key",
        dx2.icd9_cd as "Next Enc Disch ICD9",
        dx2.icd10_cd as "Next Enc Disch ICD10",
        dx2.dx_nm as "Next Enc Disch Diag",
        dx2.ext_id as "Next Enc Disch IMO"
    from
        visitinfo v
        left join {{ source('cdw', 'visit_diagnosis')}} v2dx on ((v."Next Encounter Key" = v2dx.visit_key))
        left join {{ source('cdw', 'cdw_dictionary')}} d4 on ((v2dx.dict_dx_type_key = d4.dict_key))
        left join {{ source('cdw', 'diagnosis')}} dx2 on ((v2dx.dx_key = dx2.dx_key))
    where
        (
            (v2dx.seq_num = 1)
            and (
                d4.dict_nm = 'HSP_ACCT_DX_LIST - ACCT FINAL' :: "VARCHAR"
            )
        )
)
select
    visitinfo."Encounter Key",
    visitinfo."Previous Encounter Key",
    visitinfo."Previous EncLast Stay Class",
    visitinfo."Days Since Previous Enc",
    visitinfo."Revisit Days Category",
    visitinfo."Hours Since Previous Enc",
    visitinfo."Next Encounter Key",
    visitinfo."Next Enc Last Stay Class",
    visitinfo."Days Until Next Enc",
    visitinfo."Next Readmission Days Category",
    visitinfo."Hours Until Next Enc",
    visitinfo."Next Encounter Elective",
    prevencdiag."Previous Enc Disch ICD9",
    prevencdiag."Previous Enc Disch ICD10",
    prevencdiag."Previous Enc Disch Diag",
    prevencdiag."Previous Enc Disch IMO",
    nextencdiag."Next Enc Disch ICD9",
    nextencdiag."Next Enc Disch ICD10",
    nextencdiag."Next Enc Disch Diag",
    nextencdiag."Next Enc Disch IMO"
from
    visitinfo
    left join prevencdiag on visitinfo."Encounter Key" = prevencdiag."Encounter Key"
    left join nextencdiag on visitinfo."Encounter Key" = nextencdiag."Encounter Key"
