with given_admin as (
    select
        medication_administration.med_ord_key,
        medication_administration.seq_num,
        medication_administration.action_dt as administration_date,
        medication_administration.dose as admin_dose,
        dict_admin_dose_unit.dict_nm as admin_dose_unit,
        medication_administration.infsn_rate as admin_infusion_rate,
        dict_admin_rte.dict_nm as admin_route,
        dim_medication_administration_result.med_admin_rslt_nm
        as administration_type,
        dim_medication_administration_result.med_admin_rslt_id
        as administration_type_id,
        department.dept_nm as administration_department,
        medication_administration.med_dept_key
        as medication_administration_dept_key,
        case --There is a different admin result row for the start of these meds
            when dim_medication_administration_result.med_admin_rslt_id in (
                '9', --rate change
                '14' --rate verify
            ) then 0 else 1
        end as new_administration_ind
    from
        {{ source('cdw', 'medication_administration') }}
        as medication_administration --noqa:L031
    left join {{ source('cdw', 'cdw_dictionary') }} as dict_admin_dose_unit
        on dict_admin_dose_unit.dict_key
            = medication_administration.dict_dose_unit_key
    --noqa:L031
    left join {{ source('cdw', 'cdw_dictionary') }} as dict_admin_rte
        on dict_admin_rte.dict_key = medication_administration.dict_rte_key
    left join {{ source('cdw', 'department') }} as department --noqa:L031
        on medication_administration.med_dept_key = department.dept_key
    left join
        {{ source('cdw', 'dim_medication_administration_result') }}
        as dim_medication_administration_result--noqa:L031
        on dim_medication_administration_result.dim_med_admin_rslt_key
            = medication_administration.dim_med_admin_rslt_key
    where
        dim_medication_administration_result.med_admin_rslt_id in (
            '1', --given
            '6', --new bag
            '7', --restarted
            '9', --rate change
            '12', --bolus
            '13', --push
            '14',  --rate verify
            '102', --pt/caregiver admin - non high alert
            '103', --pt/caregiver admin - high alert
            '105', --given by other
            '106', --new syringe
            '112', --iv started
            '115', --iv restarted
            '116', --divided dose
            '117', --started by other
            '119', --neb restarted
            '122.0020', --performed
            '123', --added to bicarbonate concentrate
            '127', --bolus from bag/bottle/syringe
            '133', --downtime admin
            '135', --patch applied
            '139', --instill
            '141', --gravity/alternate infusion method
            '142'  --downtime admin via pump
        )
)

select
    visit.visit_key,
    medication_order.med_ord_key,
    stg_encounter.patient_name,
    stg_encounter.mrn,
    stg_encounter.dob,
    stg_encounter.csn,
    stg_encounter.encounter_date,
    visit.hosp_admit_dt as hospital_admit_date,
    visit.hosp_dischrg_dt as hospital_discharge_date,
    medication.med_nm as medication_name,
    medication.med_id as medication_id,
    medication.form as medication_form,
    medication.strength as medication_strength,
    dict_thera_class.dict_nm as therapeutic_class,
    dict_thera_class.src_id as therapeutic_class_id,
    dict_pharm_class.dict_nm as pharmacy_class,
    dict_pharm_class.src_id as pharmacy_class_id,
    dict_pharm_subclass.dict_nm as pharmacy_sub_class,
    dict_pharm_subclass.src_id as pharmacy_sub_class_id,
    medication_order.med_ord_create_dt as medication_order_create_date,
    coalesce(medication_order.med_ord_nm, medication_order.med_ord_desc) as medication_order_name,
    medication_order.start_dt as medication_start_date,
    medication_order.end_dt as medication_end_date,
    dict_ord_stat.dict_nm as order_status,
    medication_order.discr_dose as order_dose,
    dict_dose_unit.dict_nm as order_dose_unit,
    dict_med_rte.dict_nm as order_route,
    order_route_groupers.route_group as order_route_group,
    dict_med_rte.dict_nm as medication_route,
    master_frequency.freq_nm as order_frequency,
    master_frequency.freq_nm as medication_frequency,
    dict_ord_mode.dict_nm as order_mode, --- TO BE DEPRECIATED
    dict_ord_class.dict_nm as order_class,
    protocol.ptcl_nm as orderset_name, --- TO BE DEPRECIATED
    ord_prov.full_nm as ordering_provider_name,
    auth_prov.full_nm as authorizing_provider_name,
    order_dept.dept_nm as ordering_department,
    given_admin.administration_date,
    given_admin.admin_dose,
    given_admin.admin_dose_unit,
    given_admin.admin_infusion_rate,
    given_admin.admin_route,
    admin_route_groupers.route_group as admin_route_group,
    given_admin.administration_type,
    given_admin.administration_type_id,
    given_admin.administration_department,
    medication.med_key,
    medication_order.med_ord_id as medication_order_id,
    medication_order.pat_loc_dept_key as medication_order_dept_key,
    medication_order.med_ord_dt_key,
    auth_prov.prov_key as authorizing_prov_key,
    ord_prov.prov_key as ordering_prov_key,
    given_admin.medication_administration_dept_key,
    stg_encounter.pat_key,
    coalesce(stg_hsp_acct_xref.hsp_acct_key, 0) as hsp_acct_key,
    coalesce(given_admin.seq_num, 0) as administration_seq_number,
    case
        when
            lower(
                generic.dict_nm
            ) != 'not applicable' then upper(generic.dict_nm)
        when medication.generic_nm is not null then upper(medication.generic_nm)
        else upper(medication.med_nm)
    end as generic_medication_name,
    case
        when lookup_medication_specialty.medication_id is not null then 1 else 0
    end as specialty_medication_ind,
    case when
        dict_ord_mode.src_id = 1
        and (medication_order.discont_dt is null
            or timezone(
                medication_order.discont_dt, 'UTC', 'America/New_York'
            ) > coalesce(visit.hosp_dischrg_dt, medication_order.start_dt)
        )
        and (
            visit.hosp_dischrg_dt is not null
            or dict_enc_type.dict_nm != 'HOSPITAL ENCOUNTER'
        )
        then 1 else 0 end as discharge_med_ind,
    coalesce(given_admin.new_administration_ind, 0) as new_administration_ind
from
    {{ ref('stg_encounter') }} as stg_encounter --noqa: L031
inner join {{ source('cdw', 'visit') }} as visit --noqa:L031
    on stg_encounter.visit_key = visit.visit_key
inner join {{ source('cdw', 'cdw_dictionary') }} as dict_enc_type
    on dict_enc_type.dict_key = visit.dict_enc_type_key
--noqa:L031
inner join {{ source('cdw', 'medication_order') }}
    as medication_order --noqa:L031
    on visit.visit_key = medication_order.visit_key
inner join {{ source('cdw', 'medication') }} as medication --noqa:L031
    on medication.med_key = medication_order.med_key
inner join {{ source('cdw', 'cdw_dictionary') }} as generic
    on generic.dict_key = medication.dict_generic_key
left join {{ref('stg_hsp_acct_xref')}} as stg_hsp_acct_xref
    on stg_hsp_acct_xref.encounter_key = stg_encounter.encounter_key
left join {{ source('cdw', 'cdw_dictionary') }} as dict_thera_class
    on dict_thera_class.dict_key = medication.dict_thera_class_key
left join {{ source('cdw', 'cdw_dictionary') }} as dict_pharm_class
    on dict_pharm_class.dict_key = medication.dict_pharm_class_key
left join {{ source('cdw', 'cdw_dictionary') }} as dict_pharm_subclass
    on dict_pharm_subclass.dict_key = medication.dict_pharm_subclass_key
inner join {{ source('cdw', 'protocol') }} as protocol --noqa:L031
    on protocol.ptcl_key = medication_order.ptcl_key
inner join {{ source('cdw', 'cdw_dictionary') }} as dict_dose_unit
    on dict_dose_unit.dict_key = medication_order.dict_dose_unit_key
inner join {{ source('cdw', 'cdw_dictionary') }} as dict_med_rte
    on dict_med_rte.dict_key = medication_order.dict_med_rte_key
left join {{ source('cdw', 'cdw_dictionary') }} as dict_ord_mode
    on dict_ord_mode.dict_key = medication_order.dict_ord_mode_key
left join {{ source('cdw', 'cdw_dictionary') }} as dict_ord_stat
    on dict_ord_stat.dict_key = medication_order.dict_ord_stat_key
left join {{ source('cdw', 'cdw_dictionary') }} as dict_ord_class
    on dict_ord_class.dict_key = medication_order.dict_ord_class_key
--noqa:L031
inner join {{ source('cdw', 'master_frequency') }}
    as master_frequency --noqa:L031
    on master_frequency.freq_key = medication_order.discr_freq_key
left join {{ source('cdw', 'department') }} as order_dept --noqa:L031
    on medication_order.pat_loc_dept_key = order_dept.dept_key
inner join {{ source('cdw', 'provider') }} as auth_prov
    on auth_prov.prov_key = medication_order.auth_prov_key
inner join {{ source('cdw', 'provider') }} as ord_prov
    on ord_prov.prov_key = medication_order.med_ord_prov_key
left join given_admin
    on given_admin.med_ord_key = medication_order.med_ord_key
left join {{ ref('lookup_medication_specialty') }}
    as lookup_medication_specialty --noqa:L031
    on lookup_medication_specialty.medication_id = medication.med_id
left join {{ ref('lookup_medication_route_groupers') }} as order_route_groupers
    on lower(order_route_groupers.route) = lower(dict_med_rte.dict_nm)
left join {{ ref('lookup_medication_route_groupers') }} as admin_route_groupers
    on lower(admin_route_groupers.route) = lower(given_admin.admin_route)
