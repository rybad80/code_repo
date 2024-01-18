with get_inf_abs as (
    select
        registry_data_id,
        pat_id,
        inf_date,
        associated_loc_nhsn_def_id,
        inf_class_c,
        inf_proc_code_id,
        cur_stat_c,
        cur_stat_user_id,
        cur_stat_dttm,
        or_log_id,
        coalesce(
            perm_ln_insrt_department_id,
            temp_ln_insrt_department_id,
            line_insertion_department_id
        ) as all_line_insertion_department_id

    from
        {{source('clarity_ods', 'infection_abstns')}}
    where 1 = 1
    group by
        registry_data_id,
        pat_id,
        inf_date,
        associated_loc_nhsn_def_id,
        inf_class_c,
        inf_proc_code_id,
        cur_stat_c,
        cur_stat_user_id,
        cur_stat_dttm,
        or_log_id,
        line_insertion_department_id,
        perm_ln_insrt_department_id,
        temp_ln_insrt_department_id
),

current_stg as (
    select
        get_inf_abs.registry_data_id,
        get_inf_abs.cur_stat_c,
        zc_unos_stage.name

    from
        get_inf_abs
        left join {{source('clarity_ods', 'zc_unos_stage')}} as zc_unos_stage
            on zc_unos_stage.unos_stage_c = get_inf_abs.cur_stat_c

    group by
        get_inf_abs.registry_data_id,
        get_inf_abs.cur_stat_c,
        zc_unos_stage.name
),

surgery as (
    select
        get_inf_abs.registry_data_id,
        clarity_concept.abbreviation as c28_proc_code,
        or_log_all_proc.proc_display_name as c29_proc_desc,
        or_log.sched_start_time as c30_surgery_start_date_time,
        case
            when nhsn_proc_abstns.nhsn_outpat_yn is null then 0
            when nhsn_proc_abstns.nhsn_outpat_yn = 'Y' then 1
            when nhsn_proc_abstns.nhsn_outpat_yn = 'N' then 0
        end as c38_outpatient,
        case
            when nhsn_proc_abstns.nhsn_emerg_yn is null then 0
            when nhsn_proc_abstns.nhsn_emerg_yn = 'Y' then 1
            when nhsn_proc_abstns.nhsn_emerg_yn = 'N' then 0
        end as c39_emergency,
        case
            when nhsn_proc_abstns.nhsn_gen_anesth_yn is null then 0
            when nhsn_proc_abstns.nhsn_gen_anesth_yn = 'Y' then 1
            when nhsn_proc_abstns.nhsn_gen_anesth_yn = 'N' then 0
        end as c40_general_anesthesia,
        case
            when nhsn_proc_abstns.nhsn_trauma_case_yn is null then 0
            when nhsn_proc_abstns.nhsn_trauma_case_yn = 'Y' then 1
            when nhsn_proc_abstns.nhsn_trauma_case_yn = 'N' then 0
        end as c41_trauma,
        case
            when nhsn_proc_abstns.nhsn_scope_used_yn is null then 0
            when nhsn_proc_abstns.nhsn_scope_used_yn = 'Y' then 1
            when nhsn_proc_abstns.nhsn_scope_used_yn = 'N' then 0
        end as c42_endoscope,
        null as c43_transplant,
        case
            when or_log_all_proc.proc_display_name is not null
            then dense_rank() over(
                partition by get_inf_abs.registry_data_id
                order by or_log.sched_start_time asc, or_log_all_proc.all_procs_panel asc, or_log_all_proc.line asc
            )
        end as c59_rank_surgery

    from
        get_inf_abs
        left join {{source('clarity_ods', 'clarity_concept')}} as clarity_concept
            on get_inf_abs.inf_proc_code_id = clarity_concept.internal_id
        left join {{source('clarity_ods', 'nhsn_proc_abstns')}} as nhsn_proc_abstns
            on get_inf_abs.or_log_id = nhsn_proc_abstns.log_id
        left join {{source('clarity_ods', 'or_log_all_proc')}} as or_log_all_proc
            on get_inf_abs.or_log_id = or_log_all_proc.log_id
        left join {{source('clarity_ods', 'or_log')}} as or_log
            on get_inf_abs.or_log_id = or_log.log_id
),

-- organisms / virals will be abstracted and result differently depending on multiple characteristics
-- these organisms / virals are standardized in this cte for later joining with their appropriate labwork
nhsn_organism as (

    -- get all organisms (non-viral) associated by an infection preventionist
    -- these are the majority of infection cases - an organism that is tracked by nhsn
    select
        get_inf_abs.registry_data_id,
        nhsn_ps_form.nhsn_pathogen_id,
        null as lrr_based_organ_id,
        null as component_id,
        clarity_concept.name,
        'Associated Organisms' as source

    from
        get_inf_abs
        inner join {{source('clarity_ods', 'nhsn_ps_form')}} as nhsn_ps_form
            on get_inf_abs.registry_data_id = nhsn_ps_form.registry_data_id
        left join {{source('clarity_ods', 'clarity_concept')}} as clarity_concept
            on nhsn_ps_form.nhsn_pathogen_id = clarity_concept.internal_id
        left join {{ref('bugsy_viral_suppliment_lookup')}} as bugsy_viral_suppliment_lookup
            on nhsn_ps_form.nhsn_pathogen_id = bugsy_viral_suppliment_lookup.nhsn_pathogen_id

    where
        -- ensure no virals are included in this source to solve duplication (but allow organisms tested by PCR)
        bugsy_viral_suppliment_lookup.name is null
        or bugsy_viral_suppliment_lookup.viral_ind = 0

    group by
        get_inf_abs.registry_data_id,
        nhsn_ps_form.nhsn_pathogen_id,
        lrr_based_organ_id,
        component_id,
        clarity_concept.name,
        source

    union all

    -- get all organisms that result positive but could not be associated (and are included in organism lookup table)
    -- these are organisms that are not attributable by an infection preventionist because they are not tracked by nhsn
    select
        get_inf_abs.registry_data_id,
        null as nhsn_pathogen_id,
        order_results.lrr_based_organ_id,
        null as component_id,
        bugsy_nhsn_suppliment_lookup.name,
        'Unassociated Organisms' as source

    from
        get_inf_abs
        left join {{source('clarity_ods', 'assocd_lab')}} as assocd_lab
            on get_inf_abs.registry_data_id = assocd_lab.registry_data_id
        left join {{source('clarity_ods', 'order_proc')}} as order_proc
            on assocd_lab.assocd_ord_id = order_proc.order_proc_id
        left join {{source('clarity_ods', 'order_results')}} as order_results
            on order_proc.order_proc_id = order_results.order_proc_id
        left join {{source('clarity_ods', 'clarity_organism')}} as clarity_organism
            on order_results.lrr_based_organ_id = clarity_organism.organism_id
        inner join {{ref('bugsy_nhsn_suppliment_lookup')}} as bugsy_nhsn_suppliment_lookup
            on order_results.lrr_based_organ_id = bugsy_nhsn_suppliment_lookup.lrr_based_organ_id

    group by
        get_inf_abs.registry_data_id,
        nhsn_pathogen_id,
        order_results.lrr_based_organ_id,
        component_id,
        bugsy_nhsn_suppliment_lookup.name,
        source

    union all

    -- get all virals that result positive and are associated but are not in nhsn organism list (and are included in viral lookup table)
    -- these are virals that are attributable but not linkable to labwork on an organism id because they are not organisms
    -- viral lookup table also includes organisms tested for with PCR because they have the same data model as virals
    select
        get_inf_abs.registry_data_id,
        nhsn_ps_form.nhsn_pathogen_id,
        null as lrr_based_organ_id,
        bugsy_viral_component_lookup.component_id,
        bugsy_viral_suppliment_lookup.name,
        'Associated Virals' as source

    from
        get_inf_abs
        left join {{source('clarity_ods', 'nhsn_ps_form')}} as nhsn_ps_form
            on get_inf_abs.registry_data_id = nhsn_ps_form.registry_data_id
        left join {{source('clarity_ods', 'assocd_lab')}} as assocd_lab
            on get_inf_abs.registry_data_id = assocd_lab.registry_data_id
        left join {{source('clarity_ods', 'order_proc')}} as order_proc
            on assocd_lab.assocd_ord_id = order_proc.order_proc_id
        left join {{source('clarity_ods', 'order_results')}} as order_results
            on order_proc.order_proc_id = order_results.order_proc_id
        inner join {{ref('bugsy_viral_component_lookup')}} as bugsy_viral_component_lookup
            on order_results.component_id = bugsy_viral_component_lookup.component_id
        inner join {{ref('bugsy_viral_suppliment_lookup')}} as bugsy_viral_suppliment_lookup
            on nhsn_ps_form.nhsn_pathogen_id = bugsy_viral_suppliment_lookup.nhsn_pathogen_id

    -- deduplicate associated virals and positive results using result component lookup
    where
        order_results.result_flag_c = 2
        and bugsy_viral_suppliment_lookup.name = bugsy_viral_component_lookup.viral_name

    group by
        get_inf_abs.registry_data_id,
        nhsn_ps_form.nhsn_pathogen_id,
        lrr_based_organ_id,
        bugsy_viral_component_lookup.component_id,
        bugsy_viral_suppliment_lookup.name,
        source
),

-- join standardized organisms / virals to labwork
all_organism as (
    select distinct
        get_inf_abs.registry_data_id,
        order_parent_info.ordering_dttm as c46_collect_date,
        order_proc.description as c47_order,
        clarity_eap.order_display_name as c48_specimen_source,
        zc_specimen_type.name as c49_specimen_source_category,
        zc_specimen_source.name as c50_test,
        nhsn_organism.name as c51_org_name,
        null as c52_lab_result,
        -- when a case has multiple organisms, order by organism name
        dense_rank() over (
                partition by get_inf_abs.registry_data_id
                order by nhsn_organism.name
            )
        as c60_rank_micro

    from
        get_inf_abs
        left join nhsn_organism
            on get_inf_abs.registry_data_id = nhsn_organism.registry_data_id
        left join {{source('clarity_ods', 'assocd_lab')}} as assocd_lab
            on get_inf_abs.registry_data_id = assocd_lab.registry_data_id
        left join {{source('clarity_ods', 'order_parent_info')}} as order_parent_info
            on assocd_lab.assocd_ord_id = order_parent_info.order_id
        left join {{source('clarity_ods', 'order_proc')}} as order_proc
            on assocd_lab.assocd_ord_id = order_proc.order_proc_id
        left join {{source('clarity_ods', 'order_results')}} as order_results
            on order_proc.order_proc_id = order_results.order_proc_id
        left join {{source('clarity_ods', 'clarity_eap')}} as clarity_eap
            on order_proc.proc_id = clarity_eap.proc_id
        left join {{source('clarity_ods', 'zc_specimen_type')}} as zc_specimen_type
            on order_proc.specimen_type_c = zc_specimen_type.specimen_type_c
        left join {{source('clarity_ods', 'zc_specimen_source')}} as zc_specimen_source
            on order_proc.specimen_source_c = zc_specimen_source.specimen_source_c
        left join {{source('clarity_ods', 'clarity_organism')}} as clarity_organism
            on order_results.lrr_based_organ_id = clarity_organism.organism_id
        left join {{source('clarity_ods', 'llo_nhsn_mapping')}} as llo_nhsn_mapping
            on order_results.lrr_based_organ_id = llo_nhsn_mapping.organism_id
        left join {{source('clarity_ods', 'zc_inf_class')}} as zc_inf_class
            on get_inf_abs.inf_class_c = zc_inf_class.inf_class_c

    where
        -- deduplicate rows so that associated organisms or virals align with appropriate lab results
        (
        -- align associated organisms with positive labwork using lab-assigned organism id in the nhsn organism list
            nhsn_organism.source = 'Associated Organisms'
            and nhsn_organism.nhsn_pathogen_id = llo_nhsn_mapping.override_code_id
        )
        -- align unassociated organisms with positive labwork that has the same lab-assigned organism id (not in the nhsn organism list)
        or (
            nhsn_organism.source = 'Unassociated Organisms'
            and nhsn_organism.lrr_based_organ_id = order_results.lrr_based_organ_id
        )
        -- align associated virals with positive labwork using component id (virals do not have a lab-assigned organism id)
        or (
            nhsn_organism.source = 'Associated Virals'
            and nhsn_organism.component_id = order_results.component_id
        )

    union all

    -- if a case is not an HAI it may have organisms or virals associated, but they cannot be relied on
    -- non-HAI cases that have organisms or virals associated will be included in nhsn_organism
    -- non-HAI cases that have no organisms or virals are included here to get all other lab order details 
    select
        get_inf_abs.registry_data_id,
        order_parent_info.ordering_dttm as c46_collect_date,
        order_proc.description as c47_order,
        clarity_eap.order_display_name as c48_specimen_source,
        zc_specimen_type.name as c49_specimen_source_category,
        zc_specimen_source.name as c50_test,
        null as c51_org_name,
        null as c52_lab_result,
        1 as c60_rank_micro

    from
        get_inf_abs
        left join nhsn_organism
            on get_inf_abs.registry_data_id = nhsn_organism.registry_data_id
        left join current_stg
            on get_inf_abs.registry_data_id = current_stg.registry_data_id
        left join {{source('clarity_ods', 'assocd_lab')}} as assocd_lab
            on get_inf_abs.registry_data_id = assocd_lab.registry_data_id
        left join {{source('clarity_ods', 'order_parent_info')}} as order_parent_info
            on assocd_lab.assocd_ord_id = order_parent_info.order_id
        left join {{source('clarity_ods', 'order_proc')}} as order_proc
            on assocd_lab.assocd_ord_id = order_proc.order_proc_id
        left join {{source('clarity_ods', 'clarity_eap')}} as clarity_eap
            on order_proc.proc_id = clarity_eap.proc_id
        left join {{source('clarity_ods', 'zc_specimen_type')}} as zc_specimen_type
            on order_proc.specimen_type_c = zc_specimen_type.specimen_type_c
        left join {{source('clarity_ods', 'zc_specimen_source')}} as zc_specimen_source
            on order_proc.specimen_source_c = zc_specimen_source.specimen_source_c
        left join {{source('clarity_ods', 'zc_inf_class')}} as zc_inf_class
            on get_inf_abs.inf_class_c = zc_inf_class.inf_class_c

    where
        -- get results only if not already joined using nhsn_organism and not HAI or incomplete
        nhsn_organism.source is null
        and (
            zc_inf_class.name != 'Healthcare-Associated Infection'
            or lower(current_stg.name) not in ('exported', 'ready for export', 'completed')
        )
    group by
        get_inf_abs.registry_data_id,
        order_parent_info.ordering_dttm,
        order_proc.description,
        clarity_eap.order_display_name,
        zc_specimen_type.name,
        zc_specimen_source.name
)

select
    patient.pat_mrn_id as c02_mrn,
    get_inf_abs.inf_date as c05_infection_date,
    clarity_dep.dept_abbreviation as c07_attributed_location,
    nhsn_definition.nhsn_def_name as c09_nhsn_export_location_code,
    get_inf_abs.cur_stat_user_id as c10_assigned_to_icp,
    get_inf_abs.cur_stat_dttm as c11_confirmation_date,
    case
        when zc_inf_class.name in ('Healthcare-Associated Infection', 'MDRO - Hospital Onset')
            then 'Hospital-associated'
        when zc_inf_class.name = 'Not Healthcare-Associated Infection'
            then 'Community-acquired'
        when zc_inf_class.name = 'MDRO - Internal' then null
        when zc_inf_class.name = 'MDRO - Community Onset' then null
        else 'Undetermined'
    end as c12_infection_acquisition_type,
    case
        when zc_inf_class.name = 'Healthcare-Associated Infection'
            then 'Hospital Onset'
        when zc_inf_class.name = 'Not Healthcare-Associated Infection'
            then 'Community Onset'
        when zc_inf_class.name = 'MDRO - Internal'
            then null
        when zc_inf_class.name = 'MDRO - Hospital Onset'
            then null
        when zc_inf_class.name = 'MDRO - Community Onset'
            then null
    end as c13_infection_onset,
    case
        when zc_inf_class.name in ('Not Healthcare-Associated Infection', 'MDRO - Community Onset')
            then 'Yes' else 'No'
    end as c14_present_on_admission,
    rdi_pat_csn.pat_csn as c26_account_number,
    surgery.c28_proc_code,
    surgery.c29_proc_desc,
    surgery.c30_surgery_start_date_time,
    surgery.c38_outpatient,
    surgery.c39_emergency,
    surgery.c40_general_anesthesia,
    surgery.c41_trauma,
    surgery.c42_endoscope,
    surgery.c43_transplant,
    all_organism.c46_collect_date,
    all_organism.c47_order,
    all_organism.c48_specimen_source,
    all_organism.c49_specimen_source_category,
    all_organism.c50_test,
    all_organism.c51_org_name,
    all_organism.c52_lab_result,
    300000 + get_inf_abs.registry_data_id as c54_td_ica_surv_id,
    case
        when lower(current_stg.name) in ('exported', 'ready for export', 'completed') then 'Complete'
        else current_stg.name
    end as c57_work_list_status,
    null as c58_ica_surv_type,
    surgery.c59_rank_surgery,
    all_organism.c60_rank_micro,
    null as c61_rank_encounter,
    get_inf_abs.or_log_id as c63_surgery_security_control,
    case
        when clarity_dep_insert_location.dept_abbreviation = 'MIR' then 'IR'
        else clarity_dep_insert_location.dept_abbreviation
    end as c64_insert_location

from
    get_inf_abs
    left join current_stg
        on current_stg.registry_data_id = get_inf_abs.registry_data_id
    left join surgery
        on get_inf_abs.registry_data_id = surgery.registry_data_id
    left join all_organism
        on get_inf_abs.registry_data_id = all_organism.registry_data_id
    left join {{source('clarity_ods', 'rdi_pat_csn')}} as rdi_pat_csn
        on rdi_pat_csn.registry_data_id = get_inf_abs.registry_data_id
    left join {{source('clarity_ods', 'patient')}} as patient
        on get_inf_abs.pat_id = patient.pat_id
    left join {{source('clarity_ods', 'pat_enc_5')}} as pat_enc_5
        on pat_enc_5.pat_id = get_inf_abs.pat_id and rdi_pat_csn.pat_csn = pat_enc_5.pat_enc_csn_id
    left join {{source('clarity_ods', 'assocd_dep')}} as assocd_dep
        on assocd_dep.registry_data_id = get_inf_abs.registry_data_id
    left join {{source('clarity_ods', 'clarity_dep')}} as clarity_dep
        on clarity_dep.department_id = assocd_dep.assocd_dep_id
    left join {{source('clarity_ods', 'clarity_dep')}} as clarity_dep_insert_location
        on clarity_dep_insert_location.department_id = get_inf_abs.all_line_insertion_department_id
    left join {{source('clarity_ods', 'nhsn_definition')}} as nhsn_definition
        on nhsn_definition.nhsn_def_id = get_inf_abs.associated_loc_nhsn_def_id
    left join {{source('clarity_ods', 'zc_inf_class')}} as zc_inf_class
        on zc_inf_class.inf_class_c = get_inf_abs.inf_class_c
