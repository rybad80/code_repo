{{ config(meta = {
    'critical': true
}) }}

select
    charge_aggregation.pat_key,
	charge_aggregation.service_date,
    patient_address_hist.county,
    patient_address_hist.state,
    patient_address_hist.seq_num,
    strleft(patient_address_hist.zip, 5) as zip,
	strleft(coalesce(patient_address_hist.zip, patient_address_hist_extend.zip), 5) as zip_extend,
     --sequence of all the encounters with CHOP providers
    row_number() over (
        partition by
            charge_aggregation.pat_key || charge_aggregation.service_date
        order by
            charge_aggregation.service_date
            - coalesce(patient_address_hist.eff_start_dt, patient_address_hist_extend.eff_start_dt)
    ) as line_most_recent_address,
    --prioritizing encounters in 
    --the sequence that happens at a CHOP location over external encounters
     row_number() over (
        partition by
            charge_aggregation.pat_key
        order by
            coalesce(patient_address_hist.seq_num, 99),
            charge_aggregation.service_date
            - coalesce(patient_address_hist.eff_start_dt, patient_address_hist_extend.eff_start_dt)
    ) as line_most_recent_chop_address
from
    {{ref('stg_charges_row_num')}} as charge_aggregation
    left join {{source('cdw', 'patient_address_hist')}} as patient_address_hist
        on patient_address_hist.pat_key = charge_aggregation.pat_key
            and charge_aggregation.service_date
            between
            patient_address_hist.eff_start_dt
            and coalesce(patient_address_hist.eff_end_dt - 1, current_date + 5 * 365)
			and patient_address_hist.zip is not null
    left join {{source('cdw', 'patient_address_hist')}} as patient_address_hist_extend
        on patient_address_hist_extend.pat_key = charge_aggregation.pat_key
            and charge_aggregation.service_date
            between
            patient_address_hist_extend.eff_start_dt - 30
            and coalesce(patient_address_hist_extend.eff_end_dt + 30, current_date + 5 * 365)
			and patient_address_hist_extend.zip is not null
