with meds_granularity as (select cohort.pat_mrn_id,
      mo.med_ord_nm,
  --region tpa
     case when (lower(mo.med_ord_nm) like 'alteplase inj 0.% mg'
				or lower(mo.med_ord_nm) like 'alteplase inj 0.% ml'
				or lower(mo.med_ord_nm) like 'alteplase inj 1.% ml'
				or lower(mo.med_ord_nm) like 'alteplase inj 2.% ml'
				or lower(mo.med_ord_nm) in('alteplase inj 1 mg',
				'alteplase inj 1 ml',
				'alteplase inj 2 ml',
				'alteplase inj 3 ml',
				'alteplase catheter clearance',
				'.alteplase 1 mg/ml',
				'alteplase injection')
				)
				and lower(mo.med_ord_nm) not like '%stroke%'
				and lower(mo.med_ord_nm) not like '%bolus%'
				and d_admin.med_admin_rslt_id != 43.0020
--end region
		then 1 else 0 end as tpa_ind,
		case
            when
                (
                    lower(mo.med_ord_nm) like '%tpn%' or lower(mo.med_ord_nm) like '%parenteral nutrition%'
                ) and d_admin.med_admin_rslt_id != 43.0020 then 1
            else 0
		end as tpn_ind,
		case
            when
                (
                    d_ord_rte.dict_nm in (
                        'Central venous catheter',
                        'ECMO circuit',
                        'Intravenous',
                        'Peripheral venous catheter',
                        'Intravenous (Continuous Infusion)'
                    )
         and med.route != 'Injection'
     ) or med.route in ('Intravenous') or regexp_like(mo.med_ord_desc, '\binfus', 'i') then 1
 else 0 end as route_iv_ind,
		case
            when
                d_admin.med_admin_rslt_id != 43.0020 and (
                    upper(
                        mo.med_ord_desc
                    ) like '%DOPAMINE%' or upper(
                        mo.med_ord_desc
                    ) like '%EPINEPHRINE%' or upper(mo.med_ord_desc) like '%NOREPINEPHRINE%'
	or upper(
        mo.med_ord_desc
	) like '%DOBUTAMINE%'	or upper(
        mo.med_ord_desc
	) like '%VASOPRESSIN%' or upper(
        mo.med_ord_desc
	) like '%MILRINONE%' or upper(mo.med_ord_desc) like '%ISOPROTERENOL%'
	or upper(mo.med_ord_desc) like '%PHENYLEPHRINE%') and route_iv_ind = 1 then 1 else 0 end as vaso_ind,

		case
            when
                d_admin.med_admin_rslt_id != 43.0020 and (
                    lower(
                        med_ord_desc
                    ) like '%risperidone%' or lower(
                        med_ord_desc
                    ) like '%olanzapine%' or lower(med_ord_desc) like  '%haloperidol%'
				or lower(med_ord_desc) like '%quetiapine%') then 1 else 0 end as bh_meds_ind,

		case
            when
                (
                    (
                        lower(
                            med_nm
                        ) like '%risperidone%' or lower(
                            med_nm
                        ) like '%olanzapine%' or lower(med_nm) like  '%haloperidol%'
				or lower(med_nm) like '%quetiapine%')
				and lower(med_ord_desc) like '%prn%'
				and d_admin.med_admin_rslt_id in(43.0020, --active, complete
                                     5.0000)) then 1 else 0 end as bh_prn_meds_ind,
		action_dt,
		census_dt,
		cohort.pat_key,
		date(census_dt) - date(action_dt) as time_to_census,
		case when time_to_census between 0 and 3 then 1 else 0 end as keep_ind
	from {{ source('cdw', 'medication_order') }} as mo
		inner join {{ source('cdw', 'medication_administration') }} as mar 	on mo.med_ord_key = mar.med_ord_key
		inner join
            {{ source('cdw', 'dim_medication_administration_result') }} as d_admin 		on
                mar.dim_med_admin_rslt_key = d_admin.dim_med_admin_rslt_key
		inner join {{ source('cdw', 'medication') }} as med 				on med.med_key = mo.med_key
		inner join {{ source('cdw', 'cdw_dictionary') }} as d_ord_mode    	on mo.dict_ord_mode_key = d_ord_mode.dict_key
		inner join {{ source('cdw', 'cdw_dictionary') }} as d_ord_rte    	on mo.dict_med_rte_key = d_ord_rte.dict_key
		inner join {{ source('cdw', 'cdw_dictionary') }} as d_medad_rte		on d_medad_rte.dict_key = mar.dict_rte_key
		inner join {{ ref('stg_picu_central_line_cohort') }} as cohort on mo.visit_key = cohort.visit_key
	where date(action_dt) >= '20180731'
          and (
                  lower(med_ord_desc) like '%alteplase%'
				or lower(mo.med_ord_nm) like '%tpn%'
                  or lower(mo.med_ord_nm) like '%parenteral nutrition%'
				or (
                    upper(
                        mo.med_ord_desc
                    ) like '%DOPAMINE%' or upper(
                        mo.med_ord_desc
                    ) like '%EPINEPHRINE%' or upper(mo.med_ord_desc) like '%NOREPINEPHRINE%'
				or upper(
                    mo.med_ord_desc
				) like '%DOBUTAMINE%'	or upper(
                    mo.med_ord_desc
				) like '%VASOPRESSIN%' or upper(
                    mo.med_ord_desc
				) like '%MILRINONE%' or upper(mo.med_ord_desc) like '%ISOPROTERENOL%'
				or upper(mo.med_ord_desc) like '%PHENYLEPHRINE%')
				or (
                    lower(
                        med_ord_desc
                    ) like '%risperidone%' or lower(
                        med_ord_desc
                    ) like '%olanzapine%' or lower(med_ord_desc) like  '%haloperidol%'
				or lower(med_ord_desc) like '%quetiapine%')
			)
          --standard code src ids
          and 	d_admin.med_admin_rslt_id in (
              105, 102, 116, 12, 119, 122.0020, 9, 6, 103, 1, 127, 7, 115, 106, 112, 117,
          43.0020)
          and keep_ind = 1
)

select pat_mrn_id,
	pat_key,
	census_dt,
	max(tpa_ind) as tpa_ind,
	max(tpn_ind) as tpn_ind,
	max(vaso_ind) as vaso_ind,
	max(bh_meds_ind) as bh_meds_ind,
	max(bh_prn_meds_ind) as bh_prn_meds_ind
	from meds_granularity
	group by pat_mrn_id, pat_key, census_dt
	