{{
    config(
        materialized = 'incremental',
        unique_key = 'tdl_id'
    )
}}


with max_dates_by_calendar_year as (
	select
		clarity_eap.proc_id,
		clarity_eap.proc_code,
		extract(year from clarity_eap_ot.contact_date) as rvu_calendar_year,
		max(clarity_eap_ot.contact_date) as max_contact_date
	from {{ source('clarity_ods', 'clarity_eap_ot') }} as clarity_eap_ot
	left join {{ source('clarity_ods', 'clarity_eap') }} as clarity_eap
		on clarity_eap.proc_id = clarity_eap_ot.proc_id
	where rvu_calendar_year > 2017
	group by
		clarity_eap.proc_id,
		clarity_eap.proc_code,
		rvu_calendar_year
),

rvu_values_by_calendar_year as (
	select
		max_dates_by_calendar_year.proc_id,
		proc_code,
		rvu_calendar_year,
		max_contact_date,
		clarity_eap_ot.rvu_work_compon as rvu_work
	from max_dates_by_calendar_year
	left join {{ source('clarity_ods', 'clarity_eap_ot') }} as clarity_eap_ot
		on max_dates_by_calendar_year.proc_id = clarity_eap_ot.proc_id
		and max_dates_by_calendar_year.max_contact_date = clarity_eap_ot.contact_date
),

transaction_effective_rvu_value as (
	select
		dim_date.full_date,
		case
			when dim_date.fiscal_quarter > 2 then dim_date.calendar_year - 1
			else dim_date.calendar_year
		end as effective_rvu_year
	from {{ ref('dim_date') }} as dim_date
	-- oldest date for which the Care Network will include productivity data
	where dim_date.full_date between '2018-07-01' and (current_date - 1)
)

select
    stg_all_transactions.tdl_id,
    stg_all_transactions.detail_type,
    stg_all_transactions.post_date,
    to_char(stg_all_transactions.post_date, 'YYYY') || to_char(stg_all_transactions.post_date, 'MM') as period,
    stg_all_transactions.orig_service_date,
    stg_all_transactions.amount,
    stg_all_transactions.billing_prov_id,
    stg_all_transactions.servicing_prov_id,
    transaction_effective_rvu_value.effective_rvu_year,
    stg_all_transactions.proc_id,
    rvu_values_by_calendar_year.proc_code,
    stg_all_transactions.procedure_quantity,
    stg_all_transactions.cpt_code,
    stg_all_transactions.modifier_one,
    stg_all_transactions.modifier_two,
    stg_all_transactions.modifier_three,
    stg_all_transactions.modifier_four,
    stg_all_transactions.loc_id as location_id,
    stg_all_transactions.dept_id as department_id,
    stg_all_transactions.work_rvu,
    rvu_values_by_calendar_year.rvu_work as effective_rvu_work,
    effective_rvu_work * stg_all_transactions.procedure_quantity as calculated_rvu,
    case
        when rvu_values_by_calendar_year.proc_code in (
            '99221',
            '99222',
            '99223',
            '99231',
            '99232',
            '99233',
            '99238',
            '99431',
            '99433',
            '99435',
            '99239',
            '99460',
            '99462',
            '99463',
            '54150'
        ) then 1
        else 0
    end as inpatient_id,
    case
        when detail_type in (1, 10)
        then 'charge_transaction'
        when detail_type in (2, 5, 11, 20, 22, 32, 33)
        then 'payment_transaction'
    end as transaction_type,
    stg_all_transactions.pat_enc_csn_id as csn,
    stg_all_transactions.tdl_extract_date
from {{ ref('stg_all_transactions') }} as stg_all_transactions
inner join transaction_effective_rvu_value
    on transaction_effective_rvu_value.full_date = stg_all_transactions.post_date
inner join rvu_values_by_calendar_year
    on rvu_values_by_calendar_year.proc_id = stg_all_transactions.proc_id
    and rvu_values_by_calendar_year.rvu_calendar_year = transaction_effective_rvu_value.effective_rvu_year
where period >= '201807'
and stg_all_transactions.detail_type in (
    1, 10, 2, 5, 11, 20, 22, 32, 33
)
and stg_all_transactions.loc_id in (
    1017, 1018
)
and {{ limit_dates_for_dev(ref_date = 'stg_all_transactions.post_date') }}
{% if is_incremental() %}
    and DATE(
        stg_all_transactions.tdl_extract_date
    ) >= (select max(date(tdl_extract_date) ) from {{ this }})
{% endif %}
