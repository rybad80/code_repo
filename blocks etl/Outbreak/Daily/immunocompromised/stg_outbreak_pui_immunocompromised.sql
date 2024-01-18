{{ config(materialized='table', dist='pat_key') }}

select
    pat_key,
    outbreak_type,
    reason,
    start_date,
    end_date,
    reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_neutropenia') }}
union all
select
    pat_key,
    outbreak_type,
    reason,
    start_date,
    end_date,
    reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_transplant') }}
union all
select
    pat_key,
    outbreak_type,
    reason,
    start_date,
    end_date,
    reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_medication') }}
union all
select
    pat_key,
    outbreak_type,
    reason,
    start_date,
    end_date,
    reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_chronic_dx') }}
union all
select
    pat_key,
    outbreak_type,
    reason,
    start_date,
    end_date,
    reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_treatable_dx') }}
union all
select distinct
    pat_key,
    outbreak_type,
    reason,
    start_date,
    end_date,
    reason_detail
from
    {{ ref('stg_outbreak_pui_immunocompromised_chemo') }}
