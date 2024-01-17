with
fever_vital_or_compliant as (

select
    cohort.visit_key,
    max(case when (flowsheet_id = 6 --'Temp'
            and flowsheet_all.meas_val_num >= 100.4
            and flowsheet_all.recorded_date <= cohort.disch_ed_dt
            or upper(stg_encounter_ed.primary_reason_for_visit_name) like '%FEVER%')
                then 1 else 0 end) as fever_recorded_or_compliant_ind
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
	inner join {{ ref('flowsheet_all') }} as flowsheet_all
            on cohort.visit_key = flowsheet_all.visit_key
    inner join {{ ref('stg_encounter_ed') }} as stg_encounter_ed
            on cohort.visit_key = stg_encounter_ed.visit_key
group by
    cohort.visit_key
),

smartset_uti as (
select
    cohort.visit_key,
    max(case
        when ept_sel_smartsets.selected_sset_id = 300082
        then 1 else 0 end) as smartset_uti_used
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    inner join {{ source('clarity_ods', 'ept_sel_smartsets') }} as ept_sel_smartsets
        on ept_sel_smartsets.pat_enc_csn_id = cohort.enc_id
group by
    cohort.visit_key
),

pathway_abx_ind as (

select
    cohort.visit_key,
    max(case
        when
        upper(medication_order_administration.therapeutic_class) = 'ANTI-INFECTIVE AGENTS'
        and upper(lookup.drug_category) = 'ANTIBACTERIAL'
        and medication_order_administration.administration_date is not null
        and medication_order_administration.discharge_med_ind = 0
        then 1 else 0 end) as asp_antibiotics_admin_ind,
    max(case
        when
        upper(medication_order_administration.therapeutic_class) = 'ANTI-INFECTIVE AGENTS'
        and upper(lookup.drug_category) = 'ANTIBACTERIAL'
        and medication_order_administration.discharge_med_ind = 1
        then 1 else 0 end) as asp_antibiotics_ind_disch,
    max(case
        when
        upper(medication_order_administration.therapeutic_class) = 'ANTI-INFECTIVE AGENTS'
        and upper(lookup.drug_category) = 'ANTIBACTERIAL'
        and upper(stg_asp_outpatient_sig.abx_name) = 'CEPHALEXIN'
        and medication_order_administration.discharge_med_ind = 1
        then sig_num else null end) as ceph_signum,
    max(case
        when
        upper(medication_order_administration.therapeutic_class) = 'ANTI-INFECTIVE AGENTS'
        and upper(lookup.drug_category) = 'ANTIBACTERIAL'
        and upper(stg_asp_outpatient_sig.abx_name) = 'CEPHALEXIN'
        and medication_order_administration.discharge_med_ind = 1
        then sig_unit else null end) as ceph_sigunit
    from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
   inner join {{ ref('medication_order_administration') }} as medication_order_administration
        on cohort.visit_key = medication_order_administration.visit_key
    left join {{ ref('stg_asp_abx_all') }} as lookup
        on lookup.med_ord_key = medication_order_administration.med_ord_key
    left join {{ ref('stg_asp_outpatient_sig') }} as stg_asp_outpatient_sig
        on stg_asp_outpatient_sig.med_ord_key = medication_order_administration.med_ord_key
group by
    cohort.visit_key

),

revisit_with_uti_dx_72hrs as (

select
    cohort.visit_key,
    1 as ed_return_72hrs_utidx_ind
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
	inner join {{ref('stg_encounter')}} as revisit
        on revisit.pat_key = cohort.pat_key
	left join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on revisit.visit_key = diagnosis_encounter_all.visit_key
where
    revisit.hospital_admit_date > cohort.depart_ed_dt
	and (revisit.hospital_admit_date - cohort.depart_ed_dt) <= interval '72:00:00'
	and (diagnosis_encounter_all.icd9_code = '599.0'
		or upper(diagnosis_encounter_all.icd10_code) in ('N39.0', --UTI--
                                                 'N30.01', --Acute Cystitis
                                                 'N10', --Acute Pyelonephritis
                                                 'N12')) --Interstitial Nephritis / Pyelonephritis--
	--uti codes--
	and (diagnosis_encounter_all.hsp_acct_final_primary_ind = 1
    or diagnosis_encounter_all.hsp_acct_final_other_ind = 1)
 group by
    cohort.visit_key

),

uti_diagnosis as (

select
    cohort.visit_key,
    max(case
        when
        (diagnosis_encounter_all.icd9_code = '599.0'
        or upper(diagnosis_encounter_all.icd10_code) in ('N39.0', --UTI--
                                                 'N10', --Acute Pyelonephritis
                                                 'N30.01', --Acute Cystitis
                                                 'N12')) --Interstitial Nephritis / Pyelonephritis--
        and diagnosis_encounter_all.visit_diagnosis_ind = 1
        and upper(diagnosis_encounter_all.source_summary) not like '%PROBLEM_LIST%'
        then 1 else 0 end) as uti_dx_ind
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    inner join {{ ref('diagnosis_encounter_all') }} as diagnosis_encounter_all
        on cohort.visit_key = diagnosis_encounter_all.visit_key
group by
    cohort.visit_key
),

urine_culture_ua as (
select
	cohort.visit_key,
	max(case when upper(ed_metric.urine_culture_first_specimen) = 'URINE, CLEAN CATCH' then 1 else 0 end)
    as cleancatch_ind,
	max(case when upper(ed_metric.urine_culture_first_specimen) = 'URINE, CATHETER' then 1 else 0 end)
    as catheter_ind,
	ed_metric.urine_culture_first_result,
    max(case when btrim(ed_metric.urine_wbc_first_result)
    in('5-10', '10-15', '10-25', '15-20', '20-30', '25-40', '25-50', '30-45', '45-62',
    '50-75', '75-100', 'TNTC')
    then 1
    when btrim(ed_metric.urine_wbc_first_result)
    in('0-2', '3-5', '1-5')
    then 2 else 0 end) as ua_wbc,
    max(case when upper(btrim(ed_metric.poc_urine_nitrite_first_result)) like '%POS%'
    then 1 else 0 end) as poc_nitrite,
    max(case when upper(btrim(ed_metric.urine_nitrite_first_result)) like '%POS%'
    then 1 else 0 end) as ua_nitrite,
	max(case when(upper(ed_metric.leukocyte_esterase_first_result) like '%LARGE%'
    or upper(ed_metric.leukocyte_esterase_first_result) like '%MODERATE%')
	then 1
    when upper(ed_metric.leukocyte_esterase_first_result) like '%TRACE%'
    or upper(ed_metric.leukocyte_esterase_first_result) like '%SMALL%'
    then 2 else 0 end) as ua_leukocytes,
    max(case when(upper(ed_metric.poc_leukocyte_esterase_first_result) like '%LARGE%'
    or upper(ed_metric.poc_leukocyte_esterase_first_result) like '%MODERATE%')
	then 1
    when upper(ed_metric.poc_leukocyte_esterase_first_result) like '%TRACE%'
    or upper(ed_metric.poc_leukocyte_esterase_first_result) like '%SMALL%'
    then 2 else 0 end) as poc_leukocytes,
    max(case when(upper(btrim(ed_metric.urine_bacteria_first_result)) like '%LARGE%'
    or upper(btrim(ed_metric.urine_bacteria_first_result)) like '%MOD%')
    then 1 else 0 end) as ua_bacteruria
from
	{{ref('ed_encounter_metric_procedure_details')}} as ed_metric
    inner join {{ref('stg_ed_encounter_cohort_all')}} as cohort on
    ed_metric.visit_key = cohort.visit_key
group by
	cohort.visit_key,
	ed_metric.urine_culture_first_result
),

urine_culture_ua_final as (
select
	urine_culture_ua.visit_key,
    urine_culture_ua.cleancatch_ind,
    urine_culture_ua.catheter_ind,
	urine_culture_ua.urine_culture_first_result,
    case
    when urine_culture_ua.ua_wbc = 1 and urine_culture_ua.ua_bacteruria = 1 then 1
    when urine_culture_ua.ua_nitrite = 1 then 1
    when urine_culture_ua.ua_leukocytes = 1 then 1
    when urine_culture_ua.ua_wbc = 1
    and urine_culture_ua.ua_bacteruria = 0 then 0
    when urine_culture_ua.ua_wbc = 2 then 2
    when urine_culture_ua.ua_wbc = 1 and urine_culture_ua.ua_leukocytes = 2 then 1
    when urine_culture_ua.ua_leukocytes = 2 then 2
    else 0 end as urinalysis_result_ind,
    case
    when urine_culture_ua.poc_nitrite = 1 then 1
    when urine_culture_ua.poc_leukocytes = 1 then 1
    when urine_culture_ua.poc_leukocytes = 2 then 2
    else 0 end as poc_result_ind,
    max(case
		when upper(urine_culture_ua.urine_culture_first_result) like '%MIXED%ORGANISM%'
        then 0
        when (urine_culture_ua.cleancatch_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%>100,000 CFU/ML%'
        or urine_culture_ua.catheter_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%>100,000 CFU/ML%'
        or urine_culture_ua.catheter_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%50,000-100,000 CFU/ML%'
        or urine_culture_ua.catheter_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%50,000 - 100,000 CFU/ML%') then 1
        when urine_culture_ua.cleancatch_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%50,000 - 100,000 CFU/ML%'
        or urine_culture_ua.cleancatch_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%50,000-100,000 CFU/ML%'
        or urine_culture_ua.catheter_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%>10,000 CFU/ML%'
        or urine_culture_ua.catheter_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%10,000 - 50,000 CFU/ML%'
        or urine_culture_ua.catheter_ind = 1 and upper(urine_culture_ua.urine_culture_first_result)
        like '%10,000-49,000 CFU/ML%'
        then 2 else 0 end) as urine_culture_result
from
	urine_culture_ua
group by
	urine_culture_ua.visit_key,
	urine_culture_ua.cleancatch_ind,
	urine_culture_ua.catheter_ind,
	urine_culture_ua.urine_culture_first_result,
    urine_culture_ua.ua_wbc,
	urine_culture_ua.ua_nitrite,
	urine_culture_ua.ua_leukocytes,
	urine_culture_ua.ua_bacteruria,
    urine_culture_ua.poc_nitrite,
    urine_culture_ua.poc_leukocytes

)

select
    cohort.visit_key,
    coalesce(fever_vital_or_compliant.fever_recorded_or_compliant_ind, 0) as fever_recorded_or_compliant_ind,
    coalesce(pathway_abx_ind.asp_antibiotics_admin_ind, 0) as asp_antibiotics_admin_ind,
    coalesce(pathway_abx_ind.asp_antibiotics_ind_disch, 0) as asp_antibiotics_ind_disch,
    pathway_abx_ind.ceph_signum,
    pathway_abx_ind.ceph_sigunit,
    coalesce(uti_diagnosis.uti_dx_ind, 0) as uti_dx_ind,
    coalesce(revisit_with_uti_dx_72hrs.ed_return_72hrs_utidx_ind, 0) as ed_return_72hrs_utidx_ind,
    coalesce(urine_culture_ua.cleancatch_ind, 0) as cleancatch_ind,
    coalesce(urine_culture_ua.catheter_ind, 0) as catheter_ind,
    coalesce(urine_culture_ua_final.urine_culture_result, 0) as urine_culture_result,
    coalesce(urine_culture_ua_final.urinalysis_result_ind, 0) as urinalysis_result_ind,
    coalesce(urine_culture_ua_final.poc_result_ind, 0) as poc_result_ind,
    coalesce(smartset_uti.smartset_uti_used, 0) as smartset_uti_used
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    left join fever_vital_or_compliant
        on fever_vital_or_compliant.visit_key = cohort.visit_key
   left join pathway_abx_ind
        on pathway_abx_ind.visit_key = cohort.visit_key
    left join revisit_with_uti_dx_72hrs
        on revisit_with_uti_dx_72hrs.visit_key = cohort.visit_key
    left join uti_diagnosis
        on uti_diagnosis.visit_key = cohort.visit_key
    left join urine_culture_ua
        on urine_culture_ua.visit_key = cohort.visit_key
    left join urine_culture_ua_final
        on urine_culture_ua_final.visit_key = cohort.visit_key
    left join smartset_uti
        on smartset_uti.visit_key = cohort.visit_key
