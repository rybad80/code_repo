select
	cardiac_surgery.visit_key,
	cardiac_valve_center.mrn,
    patient_all.patient_name,
	cardiac_surgery.cardiac_study_id,
	cardiac_surgery.surgeon,
	cardiac_surgery.surg_date,
	year(add_months(cardiac_surgery.surg_date, 6)) as surgery_fiscal_year,
	case
		when  regexp_like(lower(cardiac_surgery.proc_name), 'aortic|truncal')
		then 'Aortic Valve'
		when lower(cardiac_surgery.proc_name) like '%mitral%'
		then 'Mitral Valve'
		when lower(cardiac_surgery.proc_name) like '%tricuspid%'
		then 'Tricuspid Valve'
		when lower(cardiac_surgery.proc_name) like '%common av%'
			or lower(cardiac_surgery.proc_name) like '%common at%'
		then 'Common Av Valve'
		when lower(cardiac_surgery.proc_name) like '%pulmonary%'
		then 'Pulmonary Valve'
		when lower(cardiac_surgery.proc_name) like 'ross%procedure%'
		then 'Ross Procedure'
		when lower(cardiac_surgery.proc_name) like 'norwood%'
		then 'Norwood Procedure'
		else 'check' end
	as surgery_type,
	cardiac_surgery.proc_name
from {{ ref('cardiac_valve_center') }} as cardiac_valve_center
	left join {{ ref('patient_all') }} as patient_all
		on cardiac_valve_center.mrn = patient_all.mrn
	inner join {{ ref('cardiac_surgery') }} as cardiac_surgery
		on cardiac_valve_center.mrn = cardiac_surgery.mrn
where
	year(add_months(surg_date, 6)) > '2020'
	and regexp_like(lower(cardiac_surgery.surgeon), 'nuri|chen|mavroudis|fuller')
	and regexp_like(
        lower(cardiac_surgery.proc_name),
                    'truncal valve repair|
                    |valve replacement|
                    |valvuloplasty|
                    |valve repair|
                    |aortic valve repair|
                    |valve surgery|
                    |ozaki|
                    ross|procedure'
                    )
