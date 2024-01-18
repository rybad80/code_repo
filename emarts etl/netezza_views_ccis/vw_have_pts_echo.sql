with have as
(select 
	   pat_mrn_id,
	   '' redcap_event_name,
	   last_nm,
	   first_nm,
	   dob,
	   decode(sex,'M','Male','F','Female') gender,
	   racedict.dict_nm race,
	   case when upper(racedict.dict_nm) = 'OTHER' then ethdict.dict_nm else '' end as race_if_other,
	   case when upper(ethdict.dict_nm) = 'HISPANIC OR LATINO' then 'Yes' else 'No' end as ethnicity_hispanic_latino,
	   pt.zip,
	   'Yes' consent,
	   visit.eff_dt date_of_visit,
	   visit.visit_key,
	   pt.pat_key,
	   vsi.vsi_key,
	   visit.bmi,
	   visit.bp_sys,
	   visit.bp_dias,
	   visit.wt_kg,
	   visit.ht_cm
from 
    {{ source('cdw', 'visit')}} 
    join {{ source('cdw', 'visit_stay_info')}} vsi on visit.visit_key = vsi.visit_key
    join {{ source('cdw', 'department')}} on visit.dept_key = department.dept_key
    join {{ source('cdw', 'cdw_dictionary')}} enctype on enctype.dict_key = visit.dict_enc_type_key
    join {{ source('cdw', 'patient')}} pt on pt.pat_key = visit.pat_key
    left join {{ source('cdw', 'patient_race_ethnicity')}} race on pt.pat_key = race.pat_key and race.race_ind = 1
    left join {{ source('cdw', 'patient_race_ethnicity')}} eth on pt.pat_key = eth.pat_key and eth.ethnic_ind = 1
    left join {{ source('cdw', 'cdw_dictionary')}} ethdict on eth.dict_race_ethnic_key = ethdict.dict_key
    left join {{ source('cdw', 'cdw_dictionary')}} racedict on race.dict_race_ethnic_key = racedict.dict_key
    left join {{ source('cdw', 'cdw_dictionary')}} apptstat on apptstat.dict_key = visit.dict_appt_stat_key
where 1=1
  and department.dept_id in (89220005, 101012114)
  --and contact_dt_key between 20170101 and 20180116
  and appt_block = 'VASCULAR'
  and visit.dict_appt_stat_key = 202
  )
  
select pat_mrn_id,
       redcap_event_name,
	   last_nm,
	   first_nm,
	   dob ,
	   gender,
	   race,
	   race_if_other,
	   ethnicity_hispanic_latino,
	   zip,
	   consent,
	   '' as complete,
	   date_of_visit,
	   age_at_visit,
	   wt,
	   wt_pct,
	   ht,
	   ht_pct,
	   bmi ,
	   bmi_pct,
	   bp_sys,
	   bp_sys_pct,
	   bp_dia,
	   bp_dia_pct,
       lipidcheck,
	   total_chlstrl ,
	   triglycrds ,
	   ldl ,
       hdl ,
	   echoperformed,
	   study_date,
		left_ventricle_size_fx, 
		left_ventricle_structure_severity,
		left_ventricle_comment,
		left_ventricle_systolic_function,
		left_ventricle_mmode_ivs_d_avg,
		left_ventricle_mmode_ivs_d_zscore,
		left_ventricle_mmode_lvid_d_avg,
		left_ventricle_mmode_lvid_d_zscore,
		left_ventricle_mmode_lvid_s_avg,
		left_ventricle_mmode_lvid_s_zscore,
		left_ventricle_mmode_lvpwd_avg,
		left_ventricle_mmode_lvpwd_zscore,
		left_ventricle_mass_index,
		left_ventricle_mmode_lvsf,
		left_ventricle_ef_a4c_avg,
		left_ventricle_ef_bip,
		mitral_e_v_max_avg,
		mitral_a_v_max_avg,
		mitral_e_a_inflow_avg,
		diastolic_function_medial_e_prime_avg,
		diastolic_function_e_e_prime_medial_avg
from
  

		(select cdw.pat_mrn_id,
		       cdw.redcap_event_name,
			   upper(cdw.last_nm) last_nm,
			   upper(cdw.first_nm) first_nm,
			   to_char(cdw.dob,'MM/DD/YYYY') dob ,
			   cdw.gender,
			   cdw.race,
			   cdw.race_if_other,
			   cdw.ethnicity_hispanic_latino,
			   cdw.zip,
			   cdw.consent,
			   '' as complete,
			   to_char(cdw.date_of_visit,'MM/DD/YYYY') date_of_visit,
			   round(extract(epoch from date_of_visit-dob)/31557600.0,2) age_at_visit,
			   cast(coalesce(cast(visit_wt_kg as varchar(20)),cdw.wt) as numeric (4,1)) wt,
			   cdw.wt_pct,
			   cast(coalesce(visit_ht_cm ,cdw.ht) as numeric(4,1)) ht,
			   cdw.ht_pct,
			   coalesce(visit_bmi,cdw.bmi) bmi ,
			   cdw.bmi_pct,
			   coalesce(cast(visit_bp_sys as varchar(10)),cdw.bp_sys) bp_sys,
			   cdw.bp_sys_pct,
			   coalesce(cast(visit_bp_dias as varchar(10)),cdw.bp_dia) bp_dia,
			   cdw.bp_dia_pct,
		       cdw.lipidcheck,
			   cdw.total_chlstrl ,
			   cdw.triglycrds ,
			   cdw.ldl ,
		       cdw.hdl ,
			   coalesce(echo.echoperformed,'No') echoperformed,
			   to_char(to_timestamp(echo.study_date,'YYYYMMDD'),'MM/DD/YYYY') study_date,
				left_ventricle_size_fx, 
				left_ventricle_structure_severity,
				left_ventricle_comment,
				left_ventricle_systolic_function,
				left_ventricle_mmode_ivs_d_avg,
				left_ventricle_mmode_ivs_d_zscore,
				left_ventricle_mmode_lvid_d_avg,
				left_ventricle_mmode_lvid_d_zscore,
				left_ventricle_mmode_lvid_s_avg,
				left_ventricle_mmode_lvid_s_zscore,
				left_ventricle_mmode_lvpwd_avg,
				left_ventricle_mmode_lvpwd_zscore,
				left_ventricle_mass_index,
				left_ventricle_mmode_lvsf,
				left_ventricle_ef_a4c_avg,
				left_ventricle_ef_bip,
				mitral_e_v_max_avg,
				mitral_a_v_max_avg,
				mitral_e_a_inflow_avg,
				diastolic_function_medial_e_prime_avg,
				diastolic_function_e_e_prime_medial_avg,
				row_number() over (partition by pat_mrn_id, date_of_visit order by extract(epoch from cdw.date_of_visit-to_timestamp(echo.study_date,'YYYYMMDD')) ) echo_order,
				extract(epoch from cdw.date_of_visit-to_timestamp(echo.study_date,'YYYYMMDD')) datediff
		from

			(select pat_mrn_id,
			       pat_key,
			       redcap_event_name,
				   last_nm,
				   first_nm,
				   dob,
				   gender,
				   race,
				   race_if_other,
				   ethnicity_hispanic_latino,
				   zip,
				   consent,
				   date_of_visit,
				   visit_bmi,
				   visit_bp_sys,
				   visit_bp_dias,
				   visit_wt_kg,
				   visit_ht_cm,
				   wt,
				   wt_pct,
				   ht,
				   ht_pct,
				   bmi,
				   bmi_pct,
				   bp_sys,
				   bp_sys_pct,
				   bp_dia,
				   bp_dia_pct,
			       rslt_dt,
				   coalesce(lipidcheck,'No') lipidcheck,
				   total_chlstrl ,
				   triglycrds ,
				   ldl ,
			       hdl   
			from
				(select *	       
					   ,row_number() over (partition by pt_ht_wt_bp.have_visit_key order by lipids.rslt_dt desc) lipid_order
				from
					(select *
					from
						(select pt_ht_wt1.*
						        ,bp.*
								,row_number() over (partition by pt_ht_wt1.have_vsi_key order by bp.rec_dt desc) bp_order
						from
							(select *
							from
								(select ht_wt.*
								       ,row_number() over (partition by ht_wt.have_vsi_key order by ht_wt.rec_dt desc) ht_wt_order
								  from
								      (select *
								       from
									    (select
										       have.pat_mrn_id,
											   have.redcap_event_name,
											   have.last_nm,
											   have.first_nm,
											   have.dob,
											   have.gender,
											   have.race,
											   have.race_if_other,
											   have.ethnicity_hispanic_latino,
											   have.zip,
											   have.consent,
											   have.date_of_visit,
										       have.visit_key have_visit_key,
										       have.vsi_key have_vsi_key,
											   have.pat_key,
											   have.bmi visit_bmi,
											   have.bp_sys visit_bp_sys,
											   have.bp_dias visit_bp_dias,
											   have.wt_kg visit_wt_kg,
											   have.ht_cm visit_ht_cm,
											   fm.rec_dt,
											   min(case when fm.fs_key = 135871 then cast(meas_val as varchar(10)) end) as wt,
											   min(null) wt_pct,
											   min(case when fm.fs_key = 121163 then round(meas_val_num*2.54,0) end) as ht,
											   min(null) ht_pct,
											   min(case when fm.fs_key = 127338 then round(meas_val_num,0) end) as bmi,
											   min(case when fm.fs_key = 145547 then round(meas_val_num,0) end) as bmi_pct
										from 
                                            have
                                            left join {{ source('cdw', 'flowsheet_record')}} fr on have.vsi_key = fr.vsi_key
											left join {{ source('cdw', 'flowsheet_measure')}} fm on fr.fs_rec_key = fm.fs_rec_key and fm.fs_key in (135871,121163,127338,145547)
											left join {{ source('cdw', 'flowsheet')}} f on fm.fs_key = f.fs_key
										where 1=1
										  
										 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
										)a
								--	 where wt is not null
									 ) ht_wt
								)pt_ht_wt
							where 1=1
							  and ht_wt_order = 1
							)pt_ht_wt1
							
							left join

								(select 
								       have.visit_key,
								       have.vsi_key bp_vsi_key,
									   fm.rec_dt,
									   min(case when fm.fs_key = 121134 then cast(meas_val as varchar(100)) end) as site,
									   min(case when fm.fs_key = 127337 then cast(meas_val as varchar(10)) end) as bp_sys,
									   min(case when fm.fs_key = 166142 then cast(meas_val as varchar(10)) end) as bp_sys_pct,
									   min(case when fm.fs_key = 121154 then cast(meas_val as varchar(10)) end) as bp_dia,
									   min(case when fm.fs_key = 166141 then cast(meas_val as varchar(10)) end) as bp_dia_pct

								from have  join {{ source('cdw', 'flowsheet_record')}} fr on have.vsi_key = fr.vsi_key
												join {{ source('cdw', 'flowsheet_measure')}} fm on fr.fs_rec_key = fm.fs_rec_key and fm.fs_key in (121134,127337,166141,121154,166142)
												join {{ source('cdw', 'flowsheet')}} f on fm.fs_key = f.fs_key
								where 1=1  
								 group by 1,2,3
								  ) bp
							  
							  on pt_ht_wt1.have_vsi_key = bp.bp_vsi_key and bp.site = 'RIGHT ARM'
						)bp1
					where bp_order = 1
					) pt_ht_wt_bp
					
					 left join
					  
					 (select ord.pat_key ord_pat_key,
						       ord.rslt_dt,
							   cast(ord.rslt_dt as date) rslt_dt_date,
							   'Yes' as lipidcheck,
							   min(case when res.rslt_comp_key = 52252 then rslt_val  end) as total_chlstrl ,
						       min(case when res.rslt_comp_key = 49466 then rslt_val  end) as triglycrds ,
							   min(case when res.rslt_comp_key = 51529 then rslt_val  end) as ldl ,
							   min(case when res.rslt_comp_key = 51723 then rslt_val  end) as hdl   
						from 
                            have
                            join {{ source('cdw', 'procedure_order')}} ord on have.pat_key = ord.pat_key  
							join {{ source('cdw', 'procedure_order_result')}} res on ord.proc_ord_key = res.proc_ord_key
						    join {{ source('cdw', 'result_component')}} rescomp on rescomp.rslt_comp_key = res.rslt_comp_key
						where 1=1
						 and proc_ord_nm = 'Lipid Panel'
						group by 1,2
					 )lipids

					on pt_ht_wt_bp.pat_key = lipids.ord_pat_key and lipids.rslt_dt_date <= pt_ht_wt_bp.date_of_visit

				) data
			where 1=1
			  and lipid_order = 1
			)cdw

		left join

			(

			select

			'Yes' as echoperformed,
		    echo_study.study_date_key  as study_date,
			patient_key,
			study_type,
			left_ventricle_size_fx, 
			left_ventricle_structure_severity,
			left_ventricle_comment,
			left_ventricle_systolic_function,
			left_ventricle_mmode_ivs_d_avg,
			left_ventricle_mmode_ivs_d_zscore,
			left_ventricle_mmode_lvid_d_avg,
			left_ventricle_mmode_lvid_d_zscore,
			left_ventricle_mmode_lvid_s_avg,
			left_ventricle_mmode_lvid_s_zscore,
			left_ventricle_mmode_lvpwd_avg,
			left_ventricle_mmode_lvpwd_zscore,
			left_ventricle_mass_index,
			left_ventricle_mmode_lvsf,
			left_ventricle_ef_a4c_avg,
			left_ventricle_ef_bip,
			mitral_e_v_max_avg,
			mitral_a_v_max_avg,
			mitral_e_a_inflow_avg,
			diastolic_function_medial_e_prime_avg,
			diastolic_function_e_e_prime_medial_avg
		 --select *
			from 
		        {{ source('cdw', 'echo_study')}}
				left join {{ source('cdw', 'echo_study_quality_improvement')}} on echo_study_quality_improvement.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_atria')}} on echo_study_atria.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_comments')}} on echo_study_comments.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_inlets')}} on echo_study_inlets.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_tricuspid_calcs')}} on echo_study_tricuspid_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_mitral_calcs')}} on echo_study_mitral_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_ventricles')}} on echo_study_ventricles.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_right_ventricle_calcs')}} on echo_study_right_ventricle_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_left_ventricle_calcs')}} on echo_study_left_ventricle_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_outlets')}} on echo_study_outlets.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_rvot_calcs')}} on echo_study_rvot_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_pulmonary_valve_calcs')}} on echo_study_pulmonary_valve_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_lvot_calcs')}} on echo_study_lvot_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_aortic_valve_calcs')}} on echo_study_aortic_valve_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_arteries')}} on echo_study_arteries.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_aorta_calcs')}} on echo_study_aorta_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_pulmonary_arteries_calcs')}} on echo_study_pulmonary_arteries_calcs.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_other')}} on echo_study_other.echo_study_id = echo_study.echo_study_id
				left join {{ source('cdw', 'echo_study_surgeries')}} on echo_study_surgeries.echo_study_id = echo_study.echo_study_id
			where 1=1 
			and study_type <> 'Vascular' 
			) echo
		on cdw.pat_key = echo.patient_key 
		 and extract(epoch from cdw.date_of_visit-to_timestamp(echo.study_date,'YYYYMMDD')) < 15724800
		 and extract(epoch from cdw.date_of_visit-to_timestamp(echo.study_date,'YYYYMMDD')) >= 0
		 --within last 6 months
		order by pat_mrn_id, date_of_visit
		) alldata
where echo_order = 1	
