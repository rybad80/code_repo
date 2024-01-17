{{ config(meta = {
    'critical': true
}) }}

select
	visit_type_id,
	visit_type_nm as visit_type,
	upd_dt as update_date,
	upd_by as updated_by,
	visit_type_key
from
	{{source('cdw_analytics', 'master_access_intake_new_patient_appt_visit_type')}}
