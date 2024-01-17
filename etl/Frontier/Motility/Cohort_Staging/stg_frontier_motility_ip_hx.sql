with
pat_dates as (--region
    select
        row_number()over(partition by stg_encounter.mrn order by admit_start_date) as admit_num,
        stg_encounter.mrn,
        stg_encounter.patient_name,
        coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
        stg_encounter_inpatient.admission_service,
        stg_encounter_inpatient.discharge_service,
        stg_encounter_inpatient.ip_enter_date as inpatient_admit_date,
        date(stg_encounter.hospital_admit_date) as admit_start_date,
        case
            when stg_encounter.hospital_discharge_date is null
            then date(stg_encounter.hospital_admit_date)
                else date(stg_encounter.hospital_discharge_date) end
        as discharge_date
    from
        {{ ref('stg_frontier_motility_dx_life') }} as dx_life
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on dx_life.pat_key = stg_encounter.pat_key
        inner join {{ ref('stg_encounter_inpatient') }} as stg_encounter_inpatient
            on stg_encounter.visit_key = stg_encounter_inpatient.visit_key
        left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
            on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    where
        stg_encounter.prov_key != 0
    --end region
),
pat_providers as (--region
    select
        pat_dates.mrn,
        pat_dates.patient_name,
        pat_key,
        visit_key,
        pat_dates.hsp_acct_key,
        inpatient_admit_date,
        admit_num,
        lookup_frontier_program_providers_all.provider_name,
        1 as motility_inpatient_ind,
        admit_start_date,
        discharge_date,
        extract( --noqa: PRS
            epoch from pat_dates.discharge_date - pat_dates.inpatient_admit_date
                ) / 86400.0 as inpatient_los_days,
        pat_dates.admission_service,
        pat_dates.discharge_service,
        row_number()over(partition by pat_dates.mrn, admit_num order by visit_key) as row_num --option 1
    from pat_dates
        left join {{ ref('stg_encounter') }} as stg_encounter
            on pat_dates.mrn = stg_encounter.mrn
                and (date(encounter_date) >= date(admit_start_date)
                    and date(encounter_date) <= date(discharge_date))
        left join {{source('cdw','provider')}} as provider
            on provider.prov_key = stg_encounter.prov_key
        inner join {{ ref('lookup_frontier_program_providers_all') }} as lookup_frontier_program_providers_all
            on provider.prov_id = cast(lookup_frontier_program_providers_all.provider_id as nvarchar(20))
                    and lookup_frontier_program_providers_all.program = 'motility'
    --end region
)
select
*
from pat_providers
where
row_num = 1
