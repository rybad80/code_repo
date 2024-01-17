with ed_visit as (
    select
        encounter_ed.visit_key,
        encounter_ed.pat_key,
        encounter_ed.ed_arrival_date,
        encounter_ed.ed_discharge_date,
        encounter_ed.hospital_discharge_date,
        encounter_ed.inpatient_ind
    from
        {{ ref('encounter_ed') }} as encounter_ed
    where
        year(encounter_ed.encounter_date) >= year(current_date) - 5
),

fever_temp as (
    select distinct
        ed_visit.visit_key,
        1 as fever_temp_ind
    from
        ed_visit
        inner join {{ ref('flowsheet_all') }} as flowsheet_all
            on ed_visit.visit_key = flowsheet_all.visit_key
    where
        flowsheet_id = 6 --'Temp'
        and flowsheet_all.meas_val_num is not null
        and flowsheet_all.recorded_date <= ed_visit.ed_discharge_date
        and flowsheet_all.meas_val_num >= 100.4
),

fever_complaint as (

select
    ed_visit.visit_key,
	max(case when seq_num = 1 then rsn_nm end ) as chief_complaint,
    max(case when upper(rsn_nm) like '%FEVER%' then 1 else 0  end ) as fever_complaint_ind,
    max(
        case when upper(rsn_nm) like '%ONCO%' and upper(rsn_nm) not like '%NON-ONCO%' then 1 else 0 end
    ) as onco_complaint_ind
from
    ed_visit
	left join {{ source('cdw', 'visit_reason') }} as visit_reason on ed_visit.visit_key = visit_reason.visit_key
	left join {{ source('cdw', 'master_reason_for_visit') }} as master_reason_for_visit
        on master_reason_for_visit.rsn_key = visit_reason.rsn_key
group by
    ed_visit.visit_key

),

fever_dx as (
    select distinct
        ed_visit.visit_key,
        1 as fever_dx_ind
    from
        ed_visit
        inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
            on ed_visit.visit_key = diagnosis_encounter_all.visit_key
    where
        (ed_primary_ind = 1
        or ed_other_ind = 1
        or ip_admit_primary_ind = 1
        or ip_admit_other_ind = 1
        )
        and icd10_code like 'R50%'
),

all_central_lines as ( -- finding active LDA
    select
        ed_visit.visit_key,
        ed_visit.ed_arrival_date,
        ed_visit.ed_discharge_date,
        patient_lda.lda_id,
        patient_lda.place_dt,
        patient_lda.remove_dt,
        lda_type.dict_nm as line_type,
        min(stg_encounter.encounter_date) as first_doc_enc_date,
        coalesce(place_dt, first_doc_enc_date) as place_date_fixed
    from
        ed_visit
        inner join {{ source('cdw', 'patient_lda') }} as patient_lda on ed_visit.pat_key = patient_lda.pat_key
        inner join {{ source('cdw', 'visit_stay_info_rows') }} as visit_stay_info_rows
            on patient_lda.pat_lda_key = visit_stay_info_rows.pat_lda_key
        inner join {{ source('cdw', 'flowsheet_template_group') }} as flowsheet_template_group
            on visit_stay_info_rows.fs_key = flowsheet_template_group.fs_key
        inner join {{ source('cdw', 'flowsheet_template') }} as flowsheet_template
            on flowsheet_template_group.fs_temp_key = flowsheet_template.fs_temp_key
        inner join {{ source('cdw', 'flowsheet_lda_group') }} as flowsheet_lda_group
            on visit_stay_info_rows.fs_key = flowsheet_lda_group.fs_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as lda_type
            on flowsheet_lda_group.dict_lda_type_key = lda_type.dict_key
        inner join {{ source('cdw', 'visit_stay_info') }} as visit_stay_info
            on visit_stay_info_rows.vsi_key = visit_stay_info.vsi_key
        inner join {{ ref('stg_encounter') }} as stg_encounter
            on stg_encounter.visit_key = visit_stay_info.visit_key
    where
        lda_type.src_id in (1, 2, 10)  -- picc (1), cvc (2), port(10)
        -- eliminate lines removed before this encounter, in-place lines have a remove date of 2157
        and patient_lda.remove_dt > ed_visit.ed_arrival_date
        and (
            patient_lda.place_dt < ed_visit.ed_discharge_date
            or patient_lda.place_dt is null -- *** will need a second CTE to address these ones ***
            )
    group by
        ed_visit.visit_key,
        ed_visit.ed_arrival_date,
        ed_visit.ed_discharge_date,
        patient_lda.lda_id,
        patient_lda.place_dt,
        patient_lda.remove_dt,
        lda_type.dict_nm
),

central_line_summary as (
    select
        visit_key,
        1 as centrl_line,
        max(case when line_type = 'Port' then 1 else 0 end) as port_ind,
        max(case when line_type = 'PICC Line' then 1 else 0 end) as picc_line_ind,
        max(case when line_type = 'CVC Line' then 1 else 0 end) as cvc_line_ind
    from
        all_central_lines
    where
        place_date_fixed < ed_arrival_date
    group by
        visit_key
),

service as (

select
    ed_visit.visit_key,
    max(case when upper(adt_department.initial_service) like '%ONCO%' then 1 else 0 end) as ever_on_onco_service,
    max(case when
        lower(adt_department.initial_service) like '%rheum%'
        or lower(adt_department.initial_service) like '%pulmon%'
        or lower(adt_department.initial_service) like '%orthoped%'
        or lower(adt_department.initial_service) like '%endocrinol%'
        or lower(adt_department.initial_service) like '%cardiology%'
        or lower(adt_department.initial_service) like '%bone marrow%'
        or lower(adt_department.initial_service) like '%neurology%'
        or lower(adt_department.initial_service) like '%immunology%' then 1 else 0 end
    ) as exclusion_service,
    max(case when lower(adt_department.initial_service) not like '%general pediatrics%'
		and lower(adt_department.initial_service) not like '%emergency%'
		and (
            lower(
                adt_department.initial_service
            ) not like '%not applicable%' and department_name not like '%emergency%'
		) then 2
        when lower(adt_department.initial_service) = 'general pediatrics' then 1 else 0 end) as gen_peds_exclusion
    --visits that were specific only to ED and Gen-peds
    --(For ED, adt_service is sometimes N/A so considered dept_name as well)
from
    ed_visit
    inner join {{ ref('adt_department') }} as adt_department
        on adt_department.visit_key = ed_visit.visit_key
group by
    ed_visit.visit_key
),

onco_cohort as (
    select
        cancer_center_patient.pat_key,
        1 as onco_pat
    from
        {{ ref('cancer_center_patient') }} as cancer_center_patient
    where
        cancer_center_patient.high_touch_ind
        + cancer_center_patient.medium_touch_ind
        + cancer_center_patient.low_touch_ind >= 1
),

complex_care as (
select
	ed_visit.pat_key,
	ed_visit.visit_key,
	max(case when lower(provider.full_nm) like '%complex%' then 1 else 0 end) as complex_care_ind
from
	ed_visit
	inner join {{ source('cdw', 'patient_care_team_edit_history') }} as patient_care_team_edit_history on
		patient_care_team_edit_history.pat_key = ed_visit.pat_key
	inner join {{ source('cdw', 'provider') }} as provider on
		provider.prov_key = patient_care_team_edit_history.prov_key
group by ed_visit.pat_key, ed_visit.visit_key
),

transfers as (
	select
		ed_visit.visit_key,
		max(case when (lower(transport_type) = 'inbound' and lower(receiving_facility) like '%chop%') then 1 else 0 end)
        as transfer_in_ind
        -- to exclude any transfers coming into ed
	from
		ed_visit
	left join {{ ref('transport_encounter_all') }} as transport_encounter_all on
		transport_encounter_all.admit_visit_key = ed_visit.visit_key
	group by ed_visit.visit_key
),

antibiotics as (
select
	ed_visit.visit_key,
	max(case when upper(abx_route) = 'IV'
    and (abx_admin_datetime >= ed_arrival_date or abx_admin_datetime <= ed_discharge_date) then 1 else 0 end)
            as iv_antibiotic_ind
from
	ed_visit
left join {{ ref('stg_asp_inpatient_abx_all') }} as stg_asp_inpatient_abx_all on
	ed_visit.visit_key = stg_asp_inpatient_abx_all.visit_key
group by ed_visit.visit_key
)

select
    ed_visit.visit_key,
    ed_visit.pat_key,
    'FEVER_CVC' as cohort,
    null as subcohort
from
    ed_visit
    inner join central_line_summary on ed_visit.visit_key = central_line_summary.visit_key
    left join fever_temp on ed_visit.visit_key = fever_temp.visit_key
    left join fever_complaint on ed_visit.visit_key = fever_complaint.visit_key
    left join fever_dx on ed_visit.visit_key = fever_dx.visit_key
    left join service on ed_visit.visit_key = service.visit_key
    left join onco_cohort on onco_cohort.pat_key = ed_visit.pat_key
    left join transfers on transfers.visit_key = ed_visit.visit_key
    left join complex_care on complex_care.visit_key = ed_visit.visit_key
    left join antibiotics on antibiotics.visit_key = ed_visit.visit_key
where
    (fever_dx_ind = 1
    or fever_complaint_ind = 1
    or fever_temp_ind = 1
    )
    and coalesce(fever_complaint.onco_complaint_ind, 0) = 0
    and service.ever_on_onco_service = 0
    and service.exclusion_service = 0
    and coalesce(onco_cohort.onco_pat, 0) = 0
    and not (complex_care_ind = 0 and gen_peds_exclusion = 1)
    --Exclude gen-peds patients unless followed by complex care
    and transfer_in_ind = 0
    and not (ed_visit.inpatient_ind = 0 and iv_antibiotic_ind = 1)
    -- Exclude patients that did not have inpatient stays unless
    -- they had an IV antiviotic administered during their ED stay
