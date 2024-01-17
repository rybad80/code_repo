with
complaint as (

select
    encounter_ed.visit_key,
    case when
        visit_reason.seq_num = 1
        and (upper(master_reason_for_visit.rsn_nm) like 'SICKLE%CELL%'
            or upper(master_reason_for_visit.rsn_nm) like '%SCD%')
        then 1 else 0 end as scd_cheif_complaint_ind,
    case when upper(rsn_nm) like '%FEVER%' and rsn_nm not in ('HAY FEVER') then 1 else 0 end as fever_complaint_ind
from
    {{ ref('encounter_ed') }} as encounter_ed
	inner join {{ source('cdw', 'visit_reason') }} as visit_reason
        on encounter_ed.visit_key = visit_reason.visit_key
	inner join {{ source('cdw', 'master_reason_for_visit') }} as master_reason_for_visit
        on master_reason_for_visit.rsn_key = visit_reason.rsn_key

),

fever_temp as (

select distinct
    encounter_ed.visit_key,
    1 as  fever_temp_ind
from
    {{ ref('encounter_ed') }} as encounter_ed
    inner join {{ ref('flowsheet_all') }} as flowsheet_all on encounter_ed.visit_key = flowsheet_all.visit_key
where
    flowsheet_id = 6 --'Temp'
    and flowsheet_all.meas_val_num is not null
    and flowsheet_all.recorded_date <= encounter_ed.ed_discharge_date
    and flowsheet_all.meas_val_num >= 101.3
),

dx as (

select
    encounter_ed.visit_key,
    max(case when icd10_code in ('R50.81', 'R50.9') and (ed_primary_ind = 1 or ed_other_ind = 1)
        then 1 else 0 end) as fever_dx_ind,
    max(case when icd10_code in (
                'D57',
                'D57.0',
                'D57.00',
                'D57.01',
                'D57.02',
                'D57.1',
                'D57.2',
                'D57.20',
                'D57.21',
                'D57.211',
                'D57.212',
                'D57.219',
                'D57.4',
                'D57.40',
                'D57.41',
                'D57.411',
                'D57.412',
                'D57.419',
                'D57.8',
                'D57.80',
                'D57.81',
                'D57.811',
                'D57.812',
                'D57.819'
                ) then 1 else 0 end) as scd_dx_ind
from
    {{ ref('encounter_ed') }} as encounter_ed
    inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on diagnosis_encounter_all.visit_key = encounter_ed.visit_key
group by
    encounter_ed.visit_key
),

iv_abx as (

select distinct
    encounter_ed.visit_key,
    1 as iv_abx_ind
from
    {{ ref('encounter_ed') }} as encounter_ed
    inner join {{ ref('medication_order_administration') }} as medication_order_administration
        on medication_order_administration.visit_key = encounter_ed.visit_key
where
    therapeutic_class_id = 1001
    and administration_seq_number > 0
    and admin_route in (
        'Intramuscular',
        'Intravenous',
        'Intravenous (Continuous Infusion)',
        'Central venous catheter',
        'CVP line',
        'Implanted Port',
        'Peripheral line',
        'Peripheral venous catheter',
        'PICC line'
        )

)

select distinct
    encounter_ed.visit_key,
    encounter_ed.pat_key,
    'SCD_FEVER' as cohort,
    null as subcohort
from
    {{ ref('encounter_ed') }} as encounter_ed
    left join complaint             on complaint.visit_key = encounter_ed.visit_key
    left join fever_temp            on fever_temp.visit_key = encounter_ed.visit_key
    left join dx                    on dx.visit_key = encounter_ed.visit_key
    inner join iv_abx                on iv_abx.visit_key = encounter_ed.visit_key
where
    year(encounter_ed.encounter_date) >= year(current_date) - 5
    and (coalesce(scd_dx_ind, 0) + coalesce(scd_cheif_complaint_ind, 0)) >= 1
    and iv_abx.iv_abx_ind = 1
    and (coalesce(fever_temp.fever_temp_ind, 0)
        + coalesce(complaint.fever_complaint_ind, 0)
        + coalesce(dx.fever_dx_ind, 0)
        ) >= 1
