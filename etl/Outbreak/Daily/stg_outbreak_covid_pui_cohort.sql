with
all_results as (
    select
        stg_outbreak_covid_cohort.mrn,
        stg_outbreak_covid_cohort.pat_key,
        stg_outbreak_covid_cohort.current_status,
        stg_outbreak_covid_cohort.encounter_provider,
        stg_outbreak_covid_cohort.procedure_name,
        stg_outbreak_covid_cohort.emp_tbl_link_ind,
        stg_outbreak_covid_cohort.placed_date as min_specimen_taken_date,
        row_number() over (
            partition by stg_outbreak_covid_cohort.pat_key
                order by
                    stg_outbreak_covid_cohort.current_status desc,
                    stg_outbreak_covid_cohort.placed_date,
                    stg_outbreak_covid_cohort.procedure_name
        ) as status_order
    from
        {{ref('stg_outbreak_covid_cohort')}} as stg_outbreak_covid_cohort
    where
        stg_outbreak_covid_cohort.false_positive_manual_review_ind = 0
),

pui as (
    select
        all_results.mrn,
        all_results.pat_key,
        all_results.current_status,
        all_results.min_specimen_taken_date
    from
        all_results
    where
        all_results.status_order = 1    -- picks most recent test result in case of multiple tests/results
),

cohort as (
    select
        pui.pat_key,
        pui.current_status,
        all_results.encounter_provider,
        all_results.emp_tbl_link_ind,
        all_results.min_specimen_taken_date,
        max(case when lower(all_results.procedure_name) = 'covid-19' then 1 else 0 end) as covid_ind,
        max(case
            when lower(all_results.procedure_name) = 'xpert xpress sars-cov-2' then 1 else 0 end
        ) as xpert_ind,
        max(case
            when lower(all_results.procedure_name) = 'sars cov 2 rna, ql real time rt pcr -q' then 1 else 0 end
        ) as realtime_ind,
        max(case
            when lower(all_results.procedure_name) = 'sars coronavirus w/cov-2 rna, qual rt-pcr -q'
                then 1 else 0 end
        ) as qual_ind,
        max(case
            when lower(all_results.procedure_name) = 'covid-19 (sars-cov-2) hup' then 1 else 0 end
        ) as hup_ind,
        max(case
            when lower(all_results.procedure_name) = 'sars-cov-2 pcr/covid19' then 1 else 0 end
        ) as cv19_ind,
        max(case
            when lower(all_results.procedure_name) = 'diasorin sars-cov-2 ab, igg -lc' then 1 else 0 end
        ) as dia_ind,
        max(case
            when lower(all_results.procedure_name) = 'euroimmun sars-cov-2 ab, igg -lc' then 1 else 0 end
        ) as eur_ind
    from
        pui
        inner join all_results on pui.pat_key = all_results.pat_key
    where
        all_results.status_order = 1
    group by
        pui.pat_key,
        pui.current_status,
        all_results.encounter_provider,
        all_results.emp_tbl_link_ind,
        all_results.min_specimen_taken_date
)

select distinct
    pat_key,
    current_status,
    encounter_provider,
    emp_tbl_link_ind,
    min_specimen_taken_date,
    trim(trailing ', ' from  --noqa: PRS
                          case when covid_ind = 1 then 'covid-19, ' else '' end
                       || case when cv19_ind = 1 then 'sars-cov-2 pcr/covid19, ' else '' end
                       || case when dia_ind = 1 then 'diasorin sars-cov-2 ab, igg -lc, ' else '' end
                       || case when eur_ind = 1 then 'euroimmun sars-cov-2 ab, igg -lc, ' else '' end
                       || case when xpert_ind = 1 then 'xpert xpress sars-cov-2, ' else '' end
                       || case when realtime_ind = 1 then 'sars cov 2 rna, ql real time rt pcr -q, ' else '' end
                       || case when qual_ind = 1 then 'sars coronavirus w/cov-2 rna, qual rt-pcr -q, ' else '' end
                       || case when hup_ind = 1 then 'covid-19 (sars-cov-2) hup, ' else '' end
    ) as covid_test_type
from
    cohort
