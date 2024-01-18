select
    stg_diabetes_patient.diabetes_reporting_month,
    stg_diabetes_patient.report_card_4mo_pat_category,
    stg_diabetes_patient.patient_key,
    cast(stg_patient.mrn as varchar(10)) as mrn,
    cast(stg_patient.patient_name as varchar(225)) as patient_name,
    stg_patient.dob,
    stg_diabetes_patient.last_visit_type,
    stg_diabetes_patient.last_encounter_type,
    stg_diabetes_patient.last_prov,
    stg_diabetes_patient.last_prov_type,
    stg_diabetes_patient.last_visit_date,
    stg_diabetes_patient.last_15mo_md_visit_ind,
    stg_diabetes_patient.last_4mo_mdnp_visit_ind,
    stg_diabetes_patient.last_15mo_edu_visit_ind,
    case
        when ((diabetes_t1y1.ip_diagnosis_ind = 1 --onset as IP at chop:
			and diabetes_t1y1.new_diabetes_date between
                stg_diabetes_patient.diabetes_reporting_month - interval('15 month')
                and stg_diabetes_patient.diabetes_reporting_month - 1)
            or (diabetes_t1y1.new_transfer_ind = 1 --transferred to CHOP:
            and date(stg_diabetes_patient.dx_date) between
                stg_diabetes_patient.diabetes_reporting_month - interval('15 month')
                and stg_diabetes_patient.diabetes_reporting_month - 1)
            )
            and stg_diabetes_patient.diabetes_type in
                ('Antibody negative Type 1',
                'Antibody positive Type 1',
                'Type 1 unknown antibody status')
        then 1 else 0
    end as t1y1_ind,
    case
        when ((diabetes_t1y1.ip_diagnosis_ind = 1 --onset as IP at chop:
			and diabetes_t1y1.new_diabetes_date between
                stg_diabetes_patient.diabetes_reporting_month - interval('15 month')
                and stg_diabetes_patient.diabetes_reporting_month - 1)
            or (diabetes_t1y1.new_transfer_ind = 1 --transferred to CHOP:
            and date(stg_diabetes_patient.dx_date) between
                stg_diabetes_patient.diabetes_reporting_month - interval('15 month')
                and stg_diabetes_patient.diabetes_reporting_month - 1)
            )
            and stg_diabetes_patient.diabetes_type in ('Type 2')
        then 1 else 0
    end as t2y1_ind,
	cast(stg_diabetes_patient.diabetes_type as varchar(100)) as diabetes_type,
	case
        when date(stg_diabetes_patient.dx_date) > date(diabetes_t1y1.new_diabetes_date)
        then diabetes_t1y1.new_diabetes_date else date(stg_diabetes_patient.dx_date)
    end as first_dx_date,
    diabetes_t1y1.new_diabetes_date,
    coalesce(year(first_dx_date), stg_diabetes_patient.dx_year) as first_dx_year,
    coalesce(round(months_between(stg_diabetes_patient.diabetes_reporting_month, first_dx_date) / 12, 2),
        stg_diabetes_patient.duration_year) as dx_duration_year,
    stg_diabetes_patient.last_seen_team_group,
    cast(stg_diabetes_patient.last_seen_team_detail as varchar(50)) as last_seen_team_detail,
    cast(stg_diabetes_patient.last_seen_np as varchar(100)) as last_seen_np,
    stg_diabetes_patient.last_seen_np_ad_login,
    cast(stg_diabetes_patient.diab_regimen as varchar(100)) as diab_regimen,
    --Last edit on report point:
    stg_diabetes_risk_scores.control_risk_score,
    --Last edit on report point:
    stg_diabetes_risk_scores.complications_risk_score,
    stg_patient_payor.payor_group, --most recent
    diabetes_t1y1.ip_diagnosis_ind,
    diabetes_t1y1.new_transfer_ind,
    stg_diabetes_patient.pat_key,
    stg_patient.pat_id,
    stg_diabetes_patient.usnwr_submission_year
from
    {{ ref('stg_diabetes_patient') }} as stg_diabetes_patient
    left join {{ref('stg_diabetes_risk_scores')}} as stg_diabetes_risk_scores
        on stg_diabetes_risk_scores.pat_key = stg_diabetes_patient.pat_key
            and stg_diabetes_risk_scores.diabetes_reporting_month = stg_diabetes_patient.diabetes_reporting_month
    left join {{ref('stg_patient')}} as stg_patient
        on stg_patient.patient_key = stg_diabetes_patient.patient_key
    left join {{ref('stg_patient_payor')}} as stg_patient_payor
        on stg_patient_payor.pat_key = stg_patient.pat_key
    left join {{ ref('diabetes_t1y1') }} as diabetes_t1y1
        on diabetes_t1y1.patient_key = stg_diabetes_patient.patient_key --patient-level
