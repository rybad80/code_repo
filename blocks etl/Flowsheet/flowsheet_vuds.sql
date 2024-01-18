with flowsheet_vuds_info as (
    select
        flowsheet_measure.rec_dt as recorded_date,
        flowsheet_record.fs_rec_id as flowsheet_record_id,
        flowsheet_record.vsi_key,
        --region Today's Visit
		max(case when flowsheet.fs_id = '12082' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as study_type,
		max(case when flowsheet.fs_id = '12083' then cast(flowsheet_measure.meas_val as nvarchar(200)) end
		) as reason_for_evaluation_today,
		--end region
		--region History
		max(case when flowsheet.fs_id = '12093' then cast(flowsheet_measure.meas_val as nvarchar(20)) end
		) as vuds_history,
		--end region
		--region Neurogenic Bladder History
		max(case when flowsheet.fs_id = '12782' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as diagnosis,
		max(case when flowsheet.fs_id = '13437' then cast(flowsheet_measure.meas_val as nvarchar(250)) end
		) as other_diagnosis,
		max(case when flowsheet.fs_id = '12095' then cast(flowsheet_measure.meas_val as nvarchar(20)) end
		) as spina_bifida_type,
		max(case when flowsheet.fs_id = '12100' then cast(flowsheet_measure.meas_val as nvarchar(50)) end
		) as spina_bifida_level,
		max(case when flowsheet.fs_id = '12101' then cast(flowsheet_measure.meas_val as nvarchar(20)) end
		) as spina_bifida_closure_history,
		max(case when flowsheet.fs_id = '12102' then cast(flowsheet_measure.meas_val as nvarchar(20)) end
		) as hospital_where_treated_for_spina_bifida,
		--end region
		--region Surgical History
		max(case when flowsheet.fs_id = '12105' then cast(flowsheet_measure.meas_val as nvarchar(250)) end
		) as bladder_surgical_history,
		max(case when flowsheet.fs_id = '12784' then cast(flowsheet_measure.meas_val as nvarchar(200)) end
		) as bowel_surgical_history,
		max(case when flowsheet.fs_id = '12107' then cast(flowsheet_measure.meas_val as nvarchar(500)) end
		) as other_surgical_history,
		--end region
		--region Voiding history
		max(case when flowsheet.fs_id = '12109' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as voids,
		max(case when flowsheet.fs_id = '12111' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as catheter_location,
		max(case when flowsheet.fs_id = '12113' then flowsheet_measure.meas_val_num end
		) as catheter_size_mm,
		max(case when flowsheet.fs_id = '12115' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as catheter_lubrication,
		max(case when flowsheet.fs_id = '12117' then cast(flowsheet_measure.meas_val as nvarchar(200)) end
		) as catheter_schedule,
		max(case when flowsheet.fs_id = '12119' then cast(flowsheet_measure.meas_val as nvarchar(200)) end
		) as catheter_dry_interval,
		max(case when flowsheet.fs_id = '12121' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as night_time_drainage,
		max(case when flowsheet.fs_id = '12123' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as catheter_irrigation,
		max(case when flowsheet.fs_id = '12786' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as anticholinergic_therapy,
		max(case when flowsheet.fs_id = '13463' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as other_anticholinergic_therapy,
		--end region
		--region UTI history
		max(case when flowsheet.fs_id = '12129' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as history_of_uti,
		max(case when flowsheet.fs_id = '12131' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as any_uti_since_last_visit,
		max(case when flowsheet.fs_id = '12133' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as febrile,
		max(case when flowsheet.fs_id = '12141' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as hospitalized,
		max(case when flowsheet.fs_id = '12143' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as antibiotic_prophylaxis,
		max(case when flowsheet.fs_id = '12145' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as antibiotic_prophylaxis_comment,
		--end region
		--region Bowel history
		max(case when flowsheet.fs_id = '14203' then cast(flowsheet_measure.meas_val as nvarchar(250)) end
		) as bowel_history_meds,
		max(case when flowsheet.fs_id = '12159' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as compliance_with_therapy,
		max(case when flowsheet.fs_id = '12160' then cast(flowsheet_measure.meas_val as nvarchar(25)) end
		) as fecal_continence,
		--end region
		--region Neurologic history
		max(case when flowsheet.fs_id = '12162' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as neurologic_history,
		max(case when flowsheet.fs_id = '12163' then cast(flowsheet_measure.meas_val as nvarchar(250)) end
		) as date_of_shunt_placement,
		--end region
		--region Orthopaedic History
		max(case when flowsheet.fs_id = '12165' then cast(flowsheet_measure.meas_val as nvarchar(25)) end
		) as ambulatory,
		max(case when flowsheet.fs_id in ('12166', '14210')  then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as assistive_devices,
		--end region
		--region Radiologic History
		max(case when flowsheet.fs_id = '12181' then cast(flowsheet_measure.meas_val as nvarchar(25)) end
		) as bladder,
		max(case when flowsheet.fs_id = '12790' then cast(flowsheet_measure.meas_val as nvarchar(150)) end
		) as kidney,
		--end region
		--region Starting Uroflow
		max(case when flowsheet.fs_id = '12183' then cast(flowsheet_measure.meas_val as nvarchar(10)) end
		) as starting_uroflow,
		max(case when flowsheet.fs_id = '12185' then cast(flowsheet_measure.meas_val as nvarchar(50)) end
		) as type,
		max(case when flowsheet.fs_id = '12187' then flowsheet_measure.meas_val_num end
		) as volume_ml,
		max(case when flowsheet.fs_id = '12189' then flowsheet_measure.meas_val_num end
		) as max_flow_ml_per_min,
		max(case when flowsheet.fs_id = '12191' then flowsheet_measure.meas_val_num end
		) as total_time_sec,
		max(case when flowsheet.fs_id = '12192' then flowsheet_measure.meas_val_num end
		) as pvr_ml,
		max(case when flowsheet.fs_id = '12194' then flowsheet_measure.meas_val_num end
		) as starting_residual_ml,
		max(case when flowsheet.fs_id = '12195' then cast(flowsheet_measure.meas_val as nvarchar(30)) end
		) as catheterization,
		max(case when flowsheet.fs_id = '12196' then cast(flowsheet_measure.meas_val as nvarchar(30)) end
		) as catheter_size,
		--end region
		--region Rate of Fill
		max(case when flowsheet.fs_id = '12614' then flowsheet_measure.meas_val_num end
		) as patient_weight_kg,
		max(case when flowsheet.fs_id = '12566' then flowsheet_measure.meas_val_num end
		) as expected_bladder_capacity_ml,
		max(case when flowsheet.fs_id = '12568' then flowsheet_measure.meas_val_num end
		) as calculated_rate_of_fill_ml_per_min,
		max(case when flowsheet.fs_id = '12762' then flowsheet_measure.meas_val_num end
		) as actual_rate_of_fill_ml_per_min,
		max(case when flowsheet.fs_id = '12610' then flowsheet_measure.meas_val_num end
		) as number_of_cycles,
		max(case when flowsheet.fs_id = '12618' then cast(flowsheet_measure.meas_val as nvarchar(5)) end
		) as volume_at_25_percent_ebc_achieved,
		max(case when flowsheet.fs_id = '12616' then flowsheet_measure.meas_val_num end
		) as volume_at_25_percent_ebc_ml,
		max(case when flowsheet.fs_id = '12622' then flowsheet_measure.meas_val_num end
		) as storage_pressure_at_25_percent_cm_h2o,
		max(case when flowsheet.fs_id = '12620' then cast(flowsheet_measure.meas_val as nvarchar(5)) end
		) as volume_at_50_percent_ebc_achieved,
		max(case when flowsheet.fs_id = '12621' then flowsheet_measure.meas_val_num end
		) as volume_at_50_percent_ebc_ml,
		max(case when flowsheet.fs_id = '12623' then flowsheet_measure.meas_val_num end
		) as storage_pressure_at_50_percent_ebc_cm_h2o,
		max(case when flowsheet.fs_id = '12645' then cast(flowsheet_measure.meas_val as nvarchar(5)) end
		) as volume_at_75_percent_ebc_achieved,
		max(case when flowsheet.fs_id = '12647' then flowsheet_measure.meas_val_num end
		) as volume_at_75_percent_ebc_ml,
		max(case when flowsheet.fs_id = '12648' then flowsheet_measure.meas_val_num end
		) as storage_pressure_at_75_percent_ebc_cm_h2o,
		max(case when flowsheet.fs_id = '12650' then flowsheet_measure.meas_val_num end
		) as storage_pressure_at_ebc_cm_h2o,
		max(case when flowsheet.fs_id = '12649' then flowsheet_measure.meas_val_num end
		) as actual_capacity_reached_ml,
		max(case when flowsheet.fs_id = '12651' then flowsheet_measure.meas_val_num end
		) as pressure_reached_at_actual_capacity_cm_h2o,
		max(case when flowsheet.fs_id = '12652' then cast(flowsheet_measure.meas_val as nvarchar(50)) end
		) as compliance,
		max(case when flowsheet.fs_id = '12654' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as first_sensation,
		max(case when flowsheet.fs_id in ('12655', '14212')  then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as is_there_a_leak,
		max(case when flowsheet.fs_id = '12657' then cast(flowsheet_measure.meas_val as nvarchar(30)) end
		) as leak_type,
		max(case when flowsheet.fs_id = '12656' then flowsheet_measure.meas_val_num end
		) as volume_at_first_leak_ml,
		max(case when flowsheet.fs_id = '12658' then flowsheet_measure.meas_val_num end
		) as pressure_at_leak_cm_h2o,
		max(case when flowsheet.fs_id = '12660' then cast(flowsheet_measure.meas_val as nvarchar(5)) end
		) as true_contraction,
		max(case when flowsheet.fs_id = '12661' then flowsheet_measure.meas_val_num end
		) as pressure_at_peak_contraction_cm_h2o,
		max(case when flowsheet.fs_id = '12662' then cast(flowsheet_measure.meas_val as nvarchar(5)) end
		) as sustained_contraction_leading_to_empty_bladder,
		max(case when flowsheet.fs_id in ('12682', '15687')  then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as uninhibited_bladder_contractions,
		max(case when flowsheet.fs_id = '12683' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as detruser_external_sphincter_dyssenergia,
		max(case when flowsheet.fs_id = '12686' then cast(flowsheet_measure.meas_val as nvarchar(25)) end
		) as post_fill_uroflow,
		--end region
		--region Bladder Emptying during study
		max(case when flowsheet.fs_id = '12688' then flowsheet_measure.meas_val_num end
		) as void_volume_ml,
		max(case when flowsheet.fs_id = '12689' then flowsheet_measure.meas_val_num end
		) as cath_volume_ml,
		max(case when flowsheet.fs_id = '12690' then flowsheet_measure.meas_val_num end
		) as pvr,
		max(case when flowsheet.fs_id = '12691' then flowsheet_measure.meas_val_num end
		) as post_obstructive_diuresis_ml,
		max(case when flowsheet.fs_id in ('12692', '14215')  then cast(flowsheet_measure.meas_val as varchar(250)) end
		) as reflux_on_current_vuds_imaging,
		max(case when flowsheet.fs_id = '13364' then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as vuds_bladder_shape,
		max(case when flowsheet.fs_id in ('12697', '14222')  then cast(flowsheet_measure.meas_val as nvarchar(100)) end
		) as vuds_bladder_neck,
		--end region
		--region Risk Stratification
		max(case when flowsheet.fs_id = '12698' then flowsheet_measure.meas_val_num end
		) as volume_at_safe_bladder_capacity_ml,
		max(case when flowsheet.fs_id = '12699' then flowsheet_measure.meas_val_num end
		) as pressure_at_safe_bladder_capacity_cm_h2o,
		max(case when flowsheet.fs_id = '12700' then cast(flowsheet_measure.meas_val as nvarchar(5)) end
		) as care_modification,
		max(case when flowsheet.fs_id = '12701' then cast(flowsheet_measure.meas_val as nvarchar(250)) end
		) as care_modifications,
		max(case when flowsheet.fs_id = '15691' then cast(flowsheet_measure.meas_val as nvarchar(500)) end
		) as risk_stratification
		--end region
    from
        {{source('cdw', 'flowsheet_record')}} as flowsheet_record
        inner join {{source('cdw', 'flowsheet_measure')}} as flowsheet_measure
            on flowsheet_measure.fs_rec_key = flowsheet_record.fs_rec_key
        inner join {{source('cdw', 'flowsheet')}} as flowsheet
            on flowsheet.fs_key = flowsheet_measure.fs_key
    where
        flowsheet.fs_id in (
			--Today's Visit
			'12082',            --Study type
			'12083',           --Reason for Evaluation today
			--History
			'12093',           --Vuds History
			--Neurogenic Bladder History
			'12782',           --Diagnosis
			'13437',           --Other Diagnosis
			'12095',           --Spina Bifida Type
			'12100',           --Spina Bifida Level
			'12101',           --Spina Bifida Closure History
			'12102',           --Hospital where treated for Spina Bifida
			--Surgical History
			'12105',           --Bladder surgical history
			'12784',          --Bowel surgical history
			'12107',           --Other surgical history
			--Voiding history
			'12109',           --Voids
			'12111',           --Catheter location
			'12113',           --Catheter size
			'12115',           --Catheter lubrication
			'12117',          --Catheter schedule
			'12119',           --Catheter dry interval
			'12121',           --Night time drainage
			'12123',           --Catheter irrigation
			'12786',           --Anticholinergic therapy
			'13463',           --Other anticholinergic therapy
			--UTI history
			'12129',           --History of UTI
			'12131',           --Any UTIs since last visit
			'12133',           --Febrile?
			'12141',           --Hospitalized
			'12143',           --Antibiotic prophylaxis
			'12145',           --Comment
			--Bowel history
			'14203',           --Bowel History Meds
			'12159',           --Compliance with therapy
			'12160',           --Fecal continence
			--Neurologic history
			'12162',           --Neurologic history
			'12163',           --Date of shunt placement
			--Orthopaedic History
			'12165',           --Ambulatory
			'12166',           --Assistive Devices
			'14210',
			--Radiologic History
			'12181',           --Bladder
			'12790',           --Kidney
			--Starting Uroflow
			'12183',           --Starting Uroflow
			'12185',           --Type
			'12187',           --Volume - mls
			'12189',           --Max flow - mls/min
			'12191',           --Total Time - secs
			'12192',           --PVR - mls
			'12194',           --Starting Residual - mls
			'12195',           --Catheterization
			'12196',           --Catheter Size
			--Rate of Fill
			'12614',           --Patient's Weight in kg
			'12566',           --Expected Bladder Capacity - mls
			'12568',           --Calculated Rate of Fill - mls/min
			'12762',           --Actual Rate of Fill - mls/min
			'12610',           --Number of cycles
			'12618',           --Volume at 25% EBC Achieved?
			'12616',           --Volume at 25% EBC - mls
			'12622',           --Storage Pressure at 25% - cm/h20
			'12620',           --Volume at 50% EBC Achieved?
			'12621',           --Volume at 50% EBC - mls
			'12623',           --Storage Pressure at 50% of EBC cm/H2O
			'12645',           --Volume at 75% EBC achieved?
			'12647',           --Volume at 75% EBC - mls
			'12648',           --Storage pressure at 75% EBC - cm/H2O
			'12650',           --Storage pressure at EBC - cm/H2O
			'12649',           --Actual capacity reached - mls
			'12651',           --Pressure reached at actual capacity - cm/H2O
			'12652',           --Compliance
			'12654',           --First Sensation
			'12655',		   --Is there a leak?
			'14212',
			'12657',           --Leak Type?
			'12656',           --Volume at first leak mls?
			'12658',           --Pressure at leak- cm/H2O?
			'12660',           --True Contraction?
			'12661',           --Pressure at peak contraction - cm/H2O
			'12662',           --Sustained contraction leading to empty bladder?
			'12682',		   --Uninhibited Bladder Contractions
			'15687',
			'12683',           --Detruser External Sphincter Dyssenergia
			'12686',           --Post-Fill Uroflow
			--Bladder Emptying during study
			'12688',           --Void Volume - mls
			'12689',           --Cath Volume - mls
			'12690',           --PVR
			'12691',           --Post obstructive Diuresis - mls
			'12692',		   --Reflux on Current VUDS Imaging
			'14215',
			'13364',           --VUDS Bladder Shape
			'12697',		   --VUDS Bladder Neck
			'14222',
			--Risk Stratification
			'12698',           --Volume at SAFE Bladder Capacity - mls
			'12699',           --Pressure at SAFE Bladder Capacity - cm/H2O
			'12700',           --Care Modification
			'12701',           --Care Modifications
			'15691'            --Risk Stratification
		--end region
        )
    group by
        flowsheet_measure.rec_dt,
        flowsheet_record.fs_rec_id,
        flowsheet_record.vsi_key
)
select
    flowsheet_vuds_info.recorded_date,
    flowsheet_vuds_info.flowsheet_record_id,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    flowsheet_vuds_info.study_type,
	flowsheet_vuds_info.reason_for_evaluation_today,
	flowsheet_vuds_info.vuds_history,
	flowsheet_vuds_info.diagnosis,
	flowsheet_vuds_info.other_diagnosis,
	flowsheet_vuds_info.spina_bifida_type,
	flowsheet_vuds_info.spina_bifida_level,
	flowsheet_vuds_info.spina_bifida_closure_history,
	flowsheet_vuds_info.hospital_where_treated_for_spina_bifida,
	flowsheet_vuds_info.bladder_surgical_history,
	flowsheet_vuds_info.bowel_surgical_history,
	flowsheet_vuds_info.other_surgical_history,
	flowsheet_vuds_info.voids,
	flowsheet_vuds_info.catheter_location,
	flowsheet_vuds_info.catheter_size_mm,
	flowsheet_vuds_info.catheter_lubrication,
	flowsheet_vuds_info.catheter_schedule,
	flowsheet_vuds_info.catheter_dry_interval,
	flowsheet_vuds_info.night_time_drainage,
	flowsheet_vuds_info.catheter_irrigation,
	flowsheet_vuds_info.anticholinergic_therapy,
	flowsheet_vuds_info.other_anticholinergic_therapy,
	flowsheet_vuds_info.history_of_uti,
	flowsheet_vuds_info.any_uti_since_last_visit,
	flowsheet_vuds_info.febrile,
	flowsheet_vuds_info.hospitalized,
	flowsheet_vuds_info.antibiotic_prophylaxis,
	flowsheet_vuds_info.antibiotic_prophylaxis_comment,
	flowsheet_vuds_info.bowel_history_meds,
	flowsheet_vuds_info.compliance_with_therapy,
	flowsheet_vuds_info.fecal_continence,
	flowsheet_vuds_info.neurologic_history,
	flowsheet_vuds_info.date_of_shunt_placement,
	flowsheet_vuds_info.ambulatory,
	flowsheet_vuds_info.assistive_devices,
	flowsheet_vuds_info.bladder,
	flowsheet_vuds_info.kidney,
	flowsheet_vuds_info.starting_uroflow,
	flowsheet_vuds_info.type,
	flowsheet_vuds_info.volume_ml,
	flowsheet_vuds_info.max_flow_ml_per_min,
	flowsheet_vuds_info.total_time_sec,
	flowsheet_vuds_info.pvr_ml,
	flowsheet_vuds_info.starting_residual_ml,
	flowsheet_vuds_info.catheterization,
	flowsheet_vuds_info.catheter_size,
	flowsheet_vuds_info.patient_weight_kg,
	flowsheet_vuds_info.expected_bladder_capacity_ml,
	flowsheet_vuds_info.calculated_rate_of_fill_ml_per_min,
	flowsheet_vuds_info.actual_rate_of_fill_ml_per_min,
	flowsheet_vuds_info.number_of_cycles,
	flowsheet_vuds_info.volume_at_25_percent_ebc_achieved,
	flowsheet_vuds_info.volume_at_25_percent_ebc_ml,
	flowsheet_vuds_info.storage_pressure_at_25_percent_cm_h2o,
	flowsheet_vuds_info.volume_at_50_percent_ebc_achieved,
	flowsheet_vuds_info.volume_at_50_percent_ebc_ml,
	flowsheet_vuds_info.storage_pressure_at_50_percent_ebc_cm_h2o,
	flowsheet_vuds_info.volume_at_75_percent_ebc_achieved,
	flowsheet_vuds_info.volume_at_75_percent_ebc_ml,
	flowsheet_vuds_info.storage_pressure_at_75_percent_ebc_cm_h2o,
	flowsheet_vuds_info.storage_pressure_at_ebc_cm_h2o,
	flowsheet_vuds_info.actual_capacity_reached_ml,
	flowsheet_vuds_info.pressure_reached_at_actual_capacity_cm_h2o,
	flowsheet_vuds_info.compliance,
	flowsheet_vuds_info.first_sensation,
	flowsheet_vuds_info.is_there_a_leak,
	flowsheet_vuds_info.leak_type,
	flowsheet_vuds_info.volume_at_first_leak_ml,
	flowsheet_vuds_info.pressure_at_leak_cm_h2o,
	flowsheet_vuds_info.true_contraction,
	flowsheet_vuds_info.pressure_at_peak_contraction_cm_h2o,
	flowsheet_vuds_info.sustained_contraction_leading_to_empty_bladder,
	flowsheet_vuds_info.uninhibited_bladder_contractions,
	flowsheet_vuds_info.detruser_external_sphincter_dyssenergia,
	flowsheet_vuds_info.post_fill_uroflow,
	flowsheet_vuds_info.void_volume_ml,
	flowsheet_vuds_info.cath_volume_ml,
	flowsheet_vuds_info.pvr,
	flowsheet_vuds_info.post_obstructive_diuresis_ml,
	flowsheet_vuds_info.reflux_on_current_vuds_imaging,
	flowsheet_vuds_info.vuds_bladder_shape,
	flowsheet_vuds_info.vuds_bladder_neck,
	flowsheet_vuds_info.volume_at_safe_bladder_capacity_ml,
	flowsheet_vuds_info.pressure_at_safe_bladder_capacity_cm_h2o,
	flowsheet_vuds_info.care_modification,
	flowsheet_vuds_info.care_modifications,
	flowsheet_vuds_info.risk_stratification,
	stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    stg_encounter.visit_key
from
    flowsheet_vuds_info
    inner join {{source('cdw', 'visit_stay_info')}} as visit_stay_info
        on visit_stay_info.vsi_key = flowsheet_vuds_info.vsi_key
    inner join {{ref('stg_encounter')}} as stg_encounter
        on stg_encounter.visit_key = visit_stay_info.visit_key
	left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
		on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
    