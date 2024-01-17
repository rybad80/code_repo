with
--identify visits with depression & related performance metric from Endo PHQ flowsheets (PHQ2 & Action Taken)
phq2_flo as (
select
	diabetes_visit_cohort.patient_key,
	diabetes_visit_cohort.mrn,
	diabetes_visit_cohort.encounter_key,
    1 as depression_screened,
    flowsheet_all.encounter_date, --AS screened_dt
    max(
        case when flowsheet_all.flowsheet_id = 15430 and flowsheet_all.meas_val_num >= 3 then 1 end
    ) as positive_depression,
    max(
        case
            when
                flowsheet_all.flowsheet_id = 15430 and flowsheet_all.meas_val_num >= 3
                then flowsheet_all.encounter_date
        end
    ) as positive_depression_dt,
    --1=Referred to social worker
    max(case when flowsheet_all.flowsheet_id = 15429 and flowsheet_all.meas_val_num > 0
                                                       --2=Referred to psychologist
                                                       --3=Sent to ED for safety concerns
        then 1 end) as depression_action_taken
from
	{{ref('diabetes_visit_cohort')}} as diabetes_visit_cohort
    inner join {{ref('flowsheet_all')}} as flowsheet_all
			on flowsheet_all.encounter_key = diabetes_visit_cohort.encounter_key
where
	--depression screened:
    flowsheet_all.flowsheet_id in (
			10060253, --'Date Completed' (Depression)
            10060254, --'Little interest or pleasure in doing things?' (PHQ2 #1)
            10060255, --'Feeling down, depressed, or hopeless?' (PHQ2 #2)
            15430, --'Depression Score' (PHQ2 Total = 10060254 + 10060255)
            15429 --'Action taken for Depression Score 3+'
            )
    and flowsheet_all.meas_val is not null
group by
	diabetes_visit_cohort.patient_key,
	diabetes_visit_cohort.mrn,
	diabetes_visit_cohort.encounter_key,
    depression_screened,
    flowsheet_all.encounter_date
),
--explore PHQ-9 SDE linked to BPA for depression score or suicide risk warning alert (1382294) in visit level:
phq89_sde as (
			--BPA steup positive alert: phq9_score or phg_8 score >= 11 or suicide_risk_score > 0
			--combine PHQ-8 SDE: alt of PHQ-9, delete Q#9 (a suicide question)
select
	smart_data_element_all.pat_key,
	case when smart_data_element_all.element_value is not null then 1 end as phq_screened_ind,
	case when (--suicide_risk_score:
				smart_data_element_all.concept_id in (
                     'EPIC#34490', -- PHQ9 suicide questionnaire 1
                     'EPIC#34491' -- PHQ9 suicide questionnaire 2
                     )
                and cast(substring(smart_data_element_all.element_value from 1 for 2) as int) > 0
            ) then 1 end as positive_suicide_risk_ind,
	case when ( --PHQ-89 total score:
				smart_data_element_all.concept_id in ('CHOP#2594', -- 'PHQ-9A TOTAL'
                                   'CHOPBH#041'  -- 'PHQ-8 TOTAL'
									)
				and lower(smart_data_element_all.element_value) not in ('in', 'incomplete')
				and cast(smart_data_element_all.element_value as int) >= 11
			)
			or positive_suicide_risk_ind = 1 -- PHQ screened positive included positive suicide screened
			then 1 end as positive_phq_ind,
	smart_data_element_all.encounter_date
from
	{{ref('diabetes_patient_all')}} as diabetes_patient_all
    inner join {{ ref('smart_data_element_all') }} as smart_data_element_all
            on smart_data_element_all.pat_key = diabetes_patient_all.pat_key
where
	smart_data_element_all.concept_id in ('CHOP#2594', -- 'PHQ-9A TOTAL'
					'CHOPBH#041'  -- 'PHQ-8 TOTAL'
					)
    or smart_data_element_all.concept_id between 'EPIC#34474' and 'EPIC#34491'
                                        -- PHQ9 PATIENT DEPRESSION QUESTIONNAIRE
group by
	smart_data_element_all.pat_key,
	phq_screened_ind,
	positive_phq_ind,
	positive_suicide_risk_ind,
	smart_data_element_all.encounter_date
),
phq9_bpa_action as (--BPA action taken when a positive PHQ9 score comes up. 
select
	alert.pat_id,
	smart_data_element_all.encounter_date,
	1 as phq_bpa_action_taken
from
	{{source('ods','alert')}} as alert
	inner join
        {{ ref('smart_data_element_all') }} as smart_data_element_all on smart_data_element_all.csn = alert.pat_csn
	inner join {{source('ods','alt_com_action')}} as alt_com_action on alert.alt_id = alt_com_action.alert_id
where
	alert.bpa_locator_id = 2583 --CHOP BASE CN BPA PHQ-9 DEPRESSION SCREEN SCORE WARNING (PRIMARY CARE)
	--category lookup in clarity ZC_ALT_ACTION_TAKE:
	and alt_com_action.action_taken_c in (41, --'ACTIVITY LINK'
										18, --'SINGLE ORDER'
										11 --'ACKNOWLEDGE/OVERRIDE WARNING'
										)
group by
	alert.pat_id,
	smart_data_element_all.encounter_date
)
select
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
--depression screening metrics: 
    max(phq2_flo.depression_screened) as phq2_screened,
    max(phq2_flo.positive_depression) as positive_phq2,
    max(phq2_flo.encounter_date) as phq2_screened_dt,
    max(phq89_sde.phq_screened_ind) as phq89_screened,
    max(phq89_sde.positive_phq_ind) as positive_phq89,
    max(phq89_sde.encounter_date) as phq89_screened_dt,
    coalesce(
        greatest(phq89_screened_dt, phq2_screened_dt), phq89_screened_dt, phq2_screened_dt
    ) as depression_screened_dt,
    coalesce(phq89_screened, phq2_screened, 0) as depression_screened_ind,
    coalesce(positive_phq89, positive_phq2, 0) as positive_depression_ind,
    max(coalesce(phq89_sde.positive_suicide_risk_ind, 0)) as positive_suicide_ind,
    max(phq2_flo.depression_action_taken) as flo_action_taken,
    max(phq9_bpa_action.phq_bpa_action_taken) as bpa_action_taken,
--under treatment: if SW and pysch seen within the positive screened report period (15 months)
	max(case when diabetes_psychology_visit.psychology_visit_key is not null then 1 end) as psych_enc_ind,
	max(case when diabetes_sw_visit.last_15mo_sw_visit_ind = 1
			then 1 end) as sw_enc_ind,
	coalesce(psych_enc_ind, sw_enc_ind) as treatment_ind
from
	{{ref('diabetes_patient_all')}} as diabetes_patient_all
	left join {{ref('diabetes_sw_visit')}} as diabetes_sw_visit
        on diabetes_sw_visit.patient_key = diabetes_patient_all.patient_key
			and diabetes_sw_visit.diabetes_reporting_month = diabetes_patient_all.diabetes_reporting_month
	left join {{ref('diabetes_psychology_visit')}} as diabetes_psychology_visit
        on diabetes_psychology_visit.patient_key = diabetes_patient_all.patient_key
			and diabetes_psychology_visit.diabetes_reporting_month = diabetes_patient_all.diabetes_reporting_month
	left join phq89_sde
		on phq89_sde.pat_key = diabetes_patient_all.pat_key
			and phq89_sde.encounter_date < diabetes_patient_all.diabetes_reporting_month
			and phq89_sde.encounter_date >= diabetes_patient_all.diabetes_reporting_month - interval('15 month')
    left join phq9_bpa_action
		on phq9_bpa_action.pat_id = diabetes_patient_all.pat_id
            and phq9_bpa_action.encounter_date < diabetes_patient_all.diabetes_reporting_month
			and phq9_bpa_action.encounter_date >= diabetes_patient_all.diabetes_reporting_month - interval('15 month')
    left join phq2_flo
		on phq2_flo.patient_key = diabetes_patient_all.patient_key
            and phq2_flo.encounter_date < diabetes_patient_all.diabetes_reporting_month
			and phq2_flo.encounter_date >= diabetes_patient_all.diabetes_reporting_month - interval('15 month')
group by
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key
