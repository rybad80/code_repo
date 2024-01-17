/*
Select all positive blood cultures that do not have results
within 30 days of a previous positive culture for the same
patient and organism
*/

with
inpatient_stays as (--select visits where patient was  
    --on an onco service at some point
    select
        adt_service.pat_key,
        adt_service.hospital_admit_date,
        adt_service.hospital_discharge_date,
        min(adt_service.service) as service
    from
        {{ ref('adt_service') }} as adt_service --noqa: L031
    where
        lower(adt_service.service) in (
            'oncology',
            'bone marrow transplant'
        )
    group by
        adt_service.pat_key,
        adt_service.hospital_admit_date,
        adt_service.hospital_discharge_date
),

positive_cultures as (
    --select all (+) blood cultures for onco pts on onco floors
    select distinct --noqa: L034
        procedure_order_result_clinical.procedure_order_id,
        procedure_order_result_clinical.proc_ord_key,
        procedure_order_result_clinical.pat_key,
        procedure_order_result_clinical.mrn,
        procedure_order_susceptibility.seq_num,
        procedure_order_result_clinical.placed_date,
        procedure_order_result_clinical.specimen_taken_date,
        procedure_order_result_clinical.result_date,
        master_result_organism.organism_nm as organism,
        dict_suscept.dict_nm as susceptibility,
        procedure_order_susceptibility.suscept_value as sensitivity_value,
        procedure_order_result_clinical.visit_key,
        case
            when lower(dict_abx.dict_nm) like 'levofloxacin%'
                then 'LEVOFLOXACIN'
            else dict_abx.dict_nm
        end as antibiotic,
        --inpatient_stays.department_group_name,
        inpatient_stays.service
    from
        {{ ref('procedure_order_result_clinical') }}
        as procedure_order_result_clinical --noqa: L031
    inner join {{ source('cdw', 'procedure_order_susceptibility') }}
        as procedure_order_susceptibility --noqa: L031
        on procedure_order_susceptibility.proc_ord_key
            = procedure_order_result_clinical.proc_ord_key
    inner join {{ source('cdw', 'master_result_organism') }}
        as master_result_organism --noqa: L031
        on master_result_organism.organism_key
            = procedure_order_susceptibility.organism_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_abx
        on dict_abx.dict_key
            = procedure_order_susceptibility.dict_antibiotic_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_suscept
        on dict_suscept.dict_key
            = procedure_order_susceptibility.dict_suscept_key
    inner join inpatient_stays
        on inpatient_stays.pat_key
            = procedure_order_result_clinical.pat_key
            and procedure_order_result_clinical.specimen_taken_date
            between inpatient_stays.hospital_admit_date
            - interval '72 hours'
            and coalesce(inpatient_stays.hospital_discharge_date, current_date)
    where
        (lower(procedure_order_result_clinical.result_component_name)
            like '%blood%culture%'
            or lower(procedure_order_result_clinical.result_component_name)
            like '%culture%blood%') -- all blood culture results
        and lower(procedure_order_result_clinical.result_component_name)
        not like '%no%blood%' -- remove 'not blood' or 'no blood'
        and procedure_order_result_clinical.result_seq_num = 1 --remove dups
        and date(procedure_order_result_clinical.result_date)
        --want to use static date so 30-day index records 
        --do not change over time
        >= '2014-05-01'
),

distinct_records as (--select distinct orders per patient-organism combo
    select distinct
        pat_key,
        organism,
        result_date
    from
        positive_cultures
),

--create indicator for restart of 30 day counting
distinct_recs_w_lookback_ind as (
    /*Lets say a patient-organism combo had results on day 1, 15, and 45.
    We want day 1 results included, day 15 excluded because
    it is w/in 30 days of day 1 results.
    We then want day 45 to count as the new day 1 to start counting to 30 again.
    Result_chain_index_ind will flag the day 1 and day 45 results*/
    select
        distinct_records.pat_key,
        distinct_records.organism,
        distinct_records.result_date,
        max(
            case
                when d_recs_right.result_date is null then 1
            end
        ) as result_chain_index_ind
    from
        distinct_records
    left join distinct_records as d_recs_right
        -- for a given primary key...
        on distinct_records.pat_key = d_recs_right.pat_key
            and distinct_records.organism = d_recs_right.organism
            -- ...look back for any records between 1 and 30 days prior
            and extract(
                epoch from (--noqa: L027
                    d_recs_right.result_date - distinct_records.result_date
                )
            ) < 0
            and extract(
                epoch from (--noqa: L027
                    d_recs_right.result_date - distinct_records.result_date
                )
            ) >= (-30 * 24 * 60 * 60)
    group by
        distinct_records.pat_key,
        distinct_records.organism,
        distinct_records.result_date
),

result_chains as (--create an identifier for result-chains
    /*Using example in distinct_recs_w_lookback_ind CTE:
    We will have two result-chains -
    One starting on day 1, the second starting on day 45*/
    select
        pat_key,
        organism,
        result_date,
        result_chain_index_ind,
        count(result_chain_index_ind) over (
            partition by
                pat_key,
                organism
            order by
                result_date
        ) as result_chain_id
    from distinct_recs_w_lookback_ind
),

result_chain_index_recs as (--create table with only result-chain index records
    /*Using exampe in distinct_recs_w_lookback_ind CTE:
    Dropping day 15 record*/
    select
        result_chains.pat_key,
        result_chains.organism,
        result_chains.result_date,
        result_chains.result_chain_id
    from
        result_chains
    where
        result_chains.result_chain_index_ind = 1
),

create_block_30day as (--divide chains into 30 day blocks
    /*For each record in distinct_records CTE,
    get interval vs. index_result_date
    -- determine x in formula: interval_days = x/31 + remainder
    -- sample output values:
    --    0  >= interval_days < 31 : 0
    --    31 >= interval_days < 62 : 1
    --    62 >= interval_days < 93 : 2
    Using exampe in distinct_recs_w_lookback_ind CTE
    Day 1 and Day 15 = 0, Day 45 = 1
    */
    select
        result_chains.pat_key,
        result_chains.organism,
        result_chains.result_date,
        result_chains.result_chain_index_ind,
        result_chains.result_chain_id,
        result_chain_index_recs.result_date as index_result_date,
        result_chains.result_date
        - result_chain_index_recs.result_date
        as interval_from_index,

        floor(
            extract(
                day from (
                    result_chains.result_date
                    - result_chain_index_recs.result_date
                )
            ) / 31
        ) as block_30day
    from
        result_chains
    inner join result_chain_index_recs
        on result_chains.pat_key = result_chain_index_recs.pat_key
            and result_chains.organism = result_chain_index_recs.organism
            and result_chains.result_chain_id
            = result_chain_index_recs.result_chain_id
),

records_to_keep as (--limit to earliest result from each result-chain and block
    select
        create_block_30day.pat_key,
        create_block_30day.organism,
        create_block_30day.result_chain_id,
        create_block_30day.block_30day,
        min(create_block_30day.result_date) as block_30day_first_result
    from
        create_block_30day
    group by
        create_block_30day.pat_key,
        create_block_30day.organism,
        create_block_30day.result_chain_id,
        create_block_30day.block_30day
)

select
    --distinct identifier per row
    {{
        dbt_utils.surrogate_key([
            'positive_cultures.procedure_order_id',
            'positive_cultures.organism ',
            'positive_cultures.seq_num'
        ])
    }} as positive_culture_key,
    --distinct order and organism used for counting metrics
    positive_cultures.procedure_order_id,
    positive_cultures.proc_ord_key,
    positive_cultures.pat_key,
    positive_cultures.mrn,
    positive_cultures.seq_num,
    positive_cultures.placed_date,
    positive_cultures.specimen_taken_date,
    positive_cultures.result_date,
    positive_cultures.organism,
    positive_cultures.antibiotic,
    positive_cultures.susceptibility,
    positive_cultures.sensitivity_value,
    positive_cultures.visit_key,
    positive_cultures.service,
    round(positive_cultures.procedure_order_id)
    || '-'
    || positive_cultures.organism as procedure_order_organism
from
    positive_cultures
inner join records_to_keep
    on records_to_keep.pat_key = positive_cultures.pat_key
        and records_to_keep.organism = positive_cultures.organism
        and records_to_keep.block_30day_first_result
        = positive_cultures.result_date
