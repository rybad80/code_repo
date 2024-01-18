select distinct
cohort.pat_mrn_id,
cohort.pat_key,
cohort.census_dt,
--,date(census_dt) as end_dt
--,date(census_dt)-3 as end_dt_m72h
--,date(rec_dt) as rec_dt
meas_val,
1 as inv_vent_ind

from {{ ref('stg_picu_central_line_cohort') }} as cohort
	inner join {{ source('cdw', 'patient') }} as p on p.pat_key = cohort.pat_key
	inner join {{ source('cdw', 'patient_lda') }} as lda on lda.pat_key = p.pat_key
    inner join {{ source('cdw', 'visit_stay_info_rows') }} as vsi on lda.pat_lda_key = vsi.pat_lda_key
    inner join {{ source('cdw', 'flowsheet_record') }} as flow_rec on flow_rec.vsi_key = vsi.vsi_key
    inner join {{ source('cdw', 'flowsheet_measure') }} as flow_mea on flow_mea.fs_rec_key = flow_rec.fs_rec_key
    inner join {{ source('cdw', 'flowsheet') }} as flow on flow.fs_key = flow_mea.fs_key

where meas_val in('Invasive ventilation', 'Ventilation~ Invasive')
					and date(rec_dt) between date(census_dt) - 3 and date(census_dt)
					