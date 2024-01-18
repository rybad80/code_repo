{{ config(meta = {
    'critical': true
}) }}

with outbreak_cohort as (
        select *
        from {{ ref('stg_outbreak_covid_cohort') }} as stg_outbreak_covid_cohort --noqa: L025
),

consult_order as (
        select
            max(
                case when master_question.quest_id in ('123667', '123525') then order_question.ansr end
            ) as order_indication,
            proc_ord_key,
            placed_date,
            mrn,
            'consult' as source_desc
        from {{ ref('procedure_order_clinical') }} as procedure_order_clinical
            left join {{ source('cdw', 'order_question') }} as order_question
                on order_question.ord_key = procedure_order_clinical.proc_ord_key
            left join {{ source('cdw', 'master_question') }} as master_question
                on master_question.quest_key = order_question.quest_key
        where lower(procedure_name) like '%consult%covid%'
        group by
            proc_ord_key,
            placed_date,
            mrn,
            source_desc
),

drive_thru_cohort as (
    select
        stg_outbreak_covid_cohort.order_indication,
        stg_outbreak_covid_cohort.proc_ord_key,
        stg_outbreak_covid_cohort.placed_date,
        stg_outbreak_covid_cohort.mrn,
        'test' as source_desc
    from
            {{ ref('stg_outbreak_covid_cohort') }} as stg_outbreak_covid_cohort
    where
        stg_outbreak_covid_cohort.result_date is not null
        and stg_outbreak_covid_cohort.drive_thru_ind = 1
        and stg_outbreak_covid_cohort.pat_ind in ('ADULT (Possible Employee)', 'LIKELY CHOP PATIENT')
),

combined as (
    select
        consult_order.proc_ord_key,
        consult_order.order_indication,
        consult_order.mrn,
        consult_order.source_desc,
        consult_order.placed_date
    from
        consult_order
    union
    select
        drive_thru_cohort.proc_ord_key,
        drive_thru_cohort.order_indication,
        drive_thru_cohort.mrn,
        drive_thru_cohort.source_desc,
        drive_thru_cohort.placed_date
    from
        drive_thru_cohort
),

prep as (
    select
        combined.proc_ord_key,
        combined.order_indication,
        combined.mrn,
        combined.source_desc,
        combined.placed_date,
        case when (combined.source_desc = 'test' and combined.order_indication is null
                and (lag(combined.source_desc, 1) over(
                    partition by combined.mrn order by combined.placed_date) != 'test')
            ) then lag(combined.order_indication, 1) over(
                partition by combined.mrn order by combined.placed_date
        ) end as ord_ind,
        case when (combined.source_desc = 'test' and combined.order_indication is null
                and (lag(combined.source_desc, 1) over(
                    partition by combined.mrn order by combined.placed_date) != 'test')
            ) then date(combined.placed_date) - date(lag(combined.placed_date, 1) over(
                partition by combined.mrn order by combined.placed_date)) end as days_between
    from
        combined
    order by
        combined.mrn,
        combined.placed_date
)

select
    outbreak_cohort.mrn,
    outbreak_cohort.proc_ord_key,
    coalesce(prep.ord_ind, outbreak_cohort.order_indication) as new_order_indication,
    prep.days_between
from
   outbreak_cohort
   left join prep on prep.proc_ord_key = outbreak_cohort.proc_ord_key
   where outbreak_cohort.drive_thru_ind = 1
