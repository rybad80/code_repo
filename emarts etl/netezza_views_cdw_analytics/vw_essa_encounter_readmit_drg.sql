select
    fact_visit_extension.visit_key as "Encounter Key",
    hsp_drg."DRG Type",
    hsp_drg."DRG Name",
    hsp_drg."DRG Number",
    hsp_drg."Severity of Illness",
    hsp_drg."Risk of Mortality",
    hsp_drg."DRG MDC Value",
    hsp_drg."DRG Expected Length of Stay",
    hsp_drg."DRG Weight",
    hsp_drg."DRG CMI Weight",
    hsp_drg."Hospital DRG Code",
    hsp_drg."Hospital DRG Reimbursement",
    hsp_drg."Hospital DRG Weight",
    hsp_drg."Hospital Account ID",
    hsp_drg."Hospital DRG Line",
    hsp_drg."Latest DRG APR Flag",
    fact_visit_extension.prev_hosp_visit_key as "Previous Encounter Key",
    prev_hsp_drg."DRG Type" as "Prev DRG Type",
    prev_hsp_drg."DRG Name" as "Prev DRG Name",
    prev_hsp_drg."DRG Number" as "Prev DRG Number",
    prev_hsp_drg."Severity of Illness" as "Prev Severity of Illness",
    prev_hsp_drg."Risk of Mortality" as "Prev Risk of Mortality",
    prev_hsp_drg."DRG MDC Value" as "Prev DRG MDC Value",
    prev_hsp_drg."DRG Expected Length of Stay" as "Prev DRG Exp Length of Stay",
    prev_hsp_drg."DRG Weight" as "Prev DRG Weight",
    prev_hsp_drg."DRG CMI Weight" as "Prev DRG CMI Weight",
    prev_hsp_drg."Hospital DRG Code" as "Prev Hospital DRG Code",
    prev_hsp_drg."Hospital DRG Reimbursement" as "Prev Hosp DRG Reimbursement",
    prev_hsp_drg."Hospital DRG Weight" as "Prev Hospital DRG Weight",
    prev_hsp_drg."Hospital Account ID" as "Prev Hospital Account ID",
    prev_hsp_drg."Hospital DRG Line" as "Prev Hospital DRG Line",
    prev_hsp_drg."Latest DRG APR Flag" as "Prev Latest DRG APR Flag",
    fact_visit_extension.next_hosp_visit_key as "Next Encounter Key",
    next_hsp_drg."DRG Type" as "Next DRG Type",
    next_hsp_drg."DRG Name" as "Next DRG Name",
    next_hsp_drg."DRG Number" as "Next DRG Number",
    next_hsp_drg."Severity of Illness" as "Next Severity of Illness",
    next_hsp_drg."Risk of Mortality" as "Next Risk of Mortality",
    next_hsp_drg."DRG MDC Value" as "Next DRG MDC Value",
    next_hsp_drg."DRG Expected Length of Stay" as "Next DRG Exp Length of Stay",
    next_hsp_drg."DRG Weight" as "Next DRG Weight",
    next_hsp_drg."DRG CMI Weight" as "Next DRG CMI Weight",
    next_hsp_drg."Hospital DRG Code" as "Next Hospital DRG Code",
    next_hsp_drg."Hospital DRG Reimbursement" as "Next Hosp DRG Reimbursement",
    next_hsp_drg."Hospital DRG Weight" as "Next Hospital DRG Weight",
    next_hsp_drg."Hospital Account ID" as "Next Hospital Account ID",
    next_hsp_drg."Hospital DRG Line" as "Next Hospital DRG Line",
    next_hsp_drg."Latest DRG APR Flag" as "Next Latest DRG APR Flag"
from
    {{ source('cdw', 'fact_visit_extension') }}
    left join {{ source('cdw', 'hospital_account_visit') }} hv on ((fact_visit_extension.visit_key = hv.visit_key))
    left join {{ source('cdw', 'vw_essa_hospital_account_drg') }} hsp_drg on ((hv.hsp_acct_key = hsp_drg."Hospital Account Key"))
    left join {{ source('cdw', 'hospital_account_visit') }} prev_hv on ((fact_visit_extension.prev_hosp_visit_key = prev_hv.visit_key))
    left join {{ source('cdw', 'vw_essa_hospital_account_drg') }} prev_hsp_drg on ((prev_hv.hsp_acct_key = prev_hsp_drg."Hospital Account Key"))
    left join {{ source('cdw', 'hospital_account_visit') }} next_hv on ((fact_visit_extension.next_hosp_visit_key = next_hv.visit_key))
    left join {{ source('cdw', 'vw_essa_hospital_account_drg') }} next_hsp_drg on ((next_hv.hsp_acct_key = next_hsp_drg."Hospital Account Key"))
where
    (
        (
            (hsp_drg."Hospital Account Key" notnull)
            or (prev_hsp_drg."Hospital Account Key" notnull)
        )
        or (next_hsp_drg."Hospital Account Key" notnull)
    )