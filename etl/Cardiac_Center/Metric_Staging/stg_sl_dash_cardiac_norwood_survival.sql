with cohort_stage as (
    select
        cardiac_surgery.mrn,
        cardiac_surgery.dob,
        cardiac_surgery.pat_key,
        stg_patient.death_date,
        cardiac_surgery.primary_proc_name as primary_procedure_name,
        cardiac_surgery.surg_date as norwood_date,
        /*note this index procedure indicator is defined differently than the traditional
            index procedure that's used in the cardiac surgery stack.
            index here is at the patient level, while the stack's is at the hospitalization level*/
        row_number() over (
            partition by cardiac_surgery.pat_key order by cardiac_surgery.surg_date
        ) as index_proc_ind,
        case
            when cardiac_surgery.primary_proc_id_32 in ('2160', '2170', '2180') then 1 else 0
        end as hybrid_procedure_ind,
        case
            when cardiac_surgery.primary_proc_id_32 = '870' then 1 else 0
        end as norwood_procedure_ind,
        max(hybrid_procedure_ind) over (
            partition by cardiac_surgery.pat_key
        ) as pat_hybrid_procedure_ind,
        max(norwood_procedure_ind) over (
            partition by cardiac_surgery.pat_key
        ) as pat_norwood_procedure_ind,
        case
            when pat_hybrid_procedure_ind = 1 and pat_norwood_procedure_ind = 1
                then 'Hybrid to Norwood'
            when pat_hybrid_procedure_ind = 0 and pat_norwood_procedure_ind = 1
                then 'Norwood Only'
            when pat_hybrid_procedure_ind = 1 and pat_norwood_procedure_ind = 0
                then 'Hybrid Only'
        end as drill_down
    from
        {{ref('cardiac_surgery')}} as cardiac_surgery
    inner join {{ref('stg_patient')}} as stg_patient
        on cardiac_surgery.pat_key = stg_patient.pat_key
    where
        cardiac_surgery.primary_proc_id_32 in (
            '870', -- norwood procedure
            '2160', -- hybrid approach "stage 1", application of rpa & lpa bands
            '2170', -- hybrid approach "stage 1", stent placement in arterial duct (pda)
            '2180' /* hybrid approach "stage 1", stent placement in arterial duct (pda) +
                    application of rpa & lpa bands */
        )
),

cohort as (
    select
        *
    from
        cohort_stage
    where
        index_proc_ind = 1
),

transplant as (
    select
        cardiac_surgery.pat_key,
        cardiac_surgery.surg_date as transplant_date,
        cardiac_surgery.primary_proc_name as transplant_procedure

    from {{ref('cardiac_surgery')}} as cardiac_surgery

    where
        cardiac_surgery.heart_tx_ind = 1
        or cardiac_surgery.heart_lung_tx_ind = 1
),

encounter_after_one_year as (
    select
        cohort.pat_key,
        1 as encounter_after_one_year_ind

    from {{ref('stg_encounter')}} as stg_encounter
    inner join cohort
        on cohort.pat_key = stg_encounter.pat_key
    inner join {{source('cdw', 'provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ref('lookup_cardiac_norwood_followup_providers')}} as lookup_cardiac_norwood_followup_providers
        -- either telephone encounter with specific providers, or completed visit
        on (provider.prov_id = cast(lookup_cardiac_norwood_followup_providers.provider_id as varchar(10))
            and stg_encounter.encounter_type_id = 70)
                or (stg_encounter.encounter_type_id in (
                        3, --Hospital Encounter
                        101, --Office Visit
                        151, --Inpatient
                        153 --Emergency
                    )
                        and stg_encounter.appointment_status_id in (
                            2, -- completed
                            -2 -- not applicable
                    )
                )
    where
        --encounter occurred at least one year after patient turned 1
        add_months(cohort.dob, 12) <= stg_encounter.encounter_date

    group by
        cohort.pat_key
),

follow_up_after_one_year as (
    select
        cohort.pat_key,
        1 as fu_after_one_year_ind

    from
        cohort
    inner join {{source('cdw', 'registry_sts_followup')}} as registry_sts_followup
        on cohort.pat_key = registry_sts_followup.pat_key

    where
        add_months(cohort.dob, 12) <= registry_sts_followup.r_last_followup_dt

    group by
        cohort.pat_key
)

select
    cohort.mrn,
    cohort.pat_key,
    cohort.pat_key as primary_key,
    cohort.primary_procedure_name,
    cohort.norwood_date,
    cohort.drill_down,
    transplant.transplant_date,
    case
        when add_months(cohort.dob, 12) > transplant_date then 0
            --if transplant occurred within one year of age, does not count in numerator
        when add_months(cohort.dob, 12) > cohort.death_date then 0
            --if death occurred within one year of age, does not count in numerator
        when encounter_after_one_year_ind is null and fu_after_one_year_ind is null then 0
            --if no contact after one year  of age, does not count in numerator	
        else 1
    end as one_year_survival_ind,
    'cardiac_norwood_surv' as metric_id
from
    cohort
left join transplant
    on cohort.pat_key = transplant.pat_key
left join encounter_after_one_year
    on cohort.pat_key = encounter_after_one_year.pat_key
left join follow_up_after_one_year
    on cohort.pat_key = follow_up_after_one_year.pat_key
/*only looking at cases since 2016 with a one year lag*/
where
    norwood_date >= '2016-01-01'
    and norwood_date <= add_months(current_date - 1, -12)
