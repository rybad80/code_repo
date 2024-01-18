with flowsheet_lda_types as (
    select
        id,
        lda_type_ot_c

    from
        {{source('clarity_ods', 'ip_flo_lda_types')}}

    group by
        id,
        lda_type_ot_c
),

-- underlying conditions for MBI-LCBI
mbi_lcbi as (
    select
        registry_data_id,
        max(
            case
                when
                    smrtdta_elem_data.element_id in (
                        'EPIC#4867',     -- Allo-SCT with at least Grade 3 GI GVHD
                        'EPIC#RSGI0006', -- Allo-SCT with diarrhea
                        'EPIC#2371'      -- Neutropenia
                    )
                    and smrtdta_elem_data.context_name = 'REGISTRY'
                    and smrtdta_elem_value.smrtdta_elem_value = 1
                then 'Y'
        end) as mbi_lcbi_yn

    from
        {{source('clarity_ods', 'infection_abstns')}} as infection_abstns
        inner join {{source('clarity_ods', 'smrtdta_elem_data')}} as smrtdta_elem_data
            on smrtdta_elem_data.record_id_numeric = infection_abstns.registry_data_id
        inner join {{source('clarity_ods', 'smrtdta_elem_value')}} as smrtdta_elem_value
            on smrtdta_elem_value.hlv_id = smrtdta_elem_data.hlv_id

    group by
        registry_data_id
)

select
    300000 + infection_abstns.registry_data_id as c54_td_ica_surv_id,
    infection_abstns.pat_id as pat_id,
    infection_abstns.inf_date as inf_date,
    patient.pat_mrn_id as pat_mrn_id,
    patient.pat_name as pat_name,
    zc_inf_class.name as infection_event,
    zc_primary_inf.name as infection_class,
    zc_primary_inf.abbr as eventtype,
    zc_lines_group.name as lda_type,
    infection_abstns.bsi_hemodialysis_line_yn,       -- HD catheter present?
    infection_abstns.nhsn_ecmo_present_yn,           -- ECMO present?
    infection_abstns.nhsn_vad_present_yn,            -- VAD present?
    infection_abstns.nhsn_munchausen_syndrome_yn,    -- MSBP?
    infection_abstns.nhsn_patient_self_injection_yn, -- Self-injection?
    infection_abstns.nhsn_epidermolysis_bullosa_yn,  -- Epiderolysis bullosa?
    -- exclude case as valid CLABSI if any of the following are selected
    case
        when
            infection_abstns.bsi_hemodialysis_line_yn = 'Y'
            or infection_abstns.nhsn_ecmo_present_yn = 'Y'
            or infection_abstns.nhsn_vad_present_yn = 'Y'
            or infection_abstns.nhsn_munchausen_syndrome_yn = 'Y'
            or infection_abstns.nhsn_patient_self_injection_yn = 'Y'
            or infection_abstns.nhsn_epidermolysis_bullosa_yn = 'Y'
        then 1 else 0
    end as bsi_exclusion_ind,
    case
        when zc_lines_group.title in ('UMBILICAL VENOUS CATHETER') then'Y'
        else 'N'
    end as umbcatheter,
    case
        when zc_uti_catheter_status.abbr = 'In Place' then 'IN PLACE'
        when zc_uti_catheter_status.abbr = 'Removed' then 'REMOVE'
    end as urinarycath,
    case
        when zc_primary_inf.abbr = 'VAE' then 'Y'
    end as ventused,
    infection_abstns.infection_contributed_death_yn as contribdeath,
    infection_abstns.patient_died_yn as died,
    zc_primary_inf.abbr as eventtypedesc,
    ncdr_icd_lab.device_implanted_yn as implant,
    case
        when zc_lines_group.title in ('PICC LINE', 'CVC LINE', 'PORT', 'UMBILICAL VENOUS CATHETER') then 'Y'
        else 'N'
    end as centralline,
    case
        when infection_abstns.perm_ln_insrt_dttm is not null then 'Y'
    end as permcentralline,
    case
        when infection_abstns.temp_ln_insrt_dttm is not null then 'Y'
    end as tempcentralline,
    case
        when assocd_lda.assocd_lda_id is not null then 'Y'
    end as lda_associated_yn,
    ip_lda_noaddsingle.placement_instant as placement_insertion_dttm,
    ip_lda_noaddsingle.removal_instant as removal_insertion_dttm,
    clarity_concept.abbreviation as proc_code,
    infection_abstns.inf_proc_dt as inf_proc_dt,
    case
        when infection_abstns.inf_proc_dt is not null then 'Y'
    end as associated_procedure_yn,
    zc_ssi_event_type.name as ssi_event_type,
    infection_abstns.ssi_present_at_surg_yn as ssi_patos_yn,
    infection_abstns.ssi_detected_during_c as ssi_detected_during,
    mbi_lcbi.mbi_lcbi_yn

from
    {{source('clarity_ods', 'infection_abstns')}} as infection_abstns
    left join {{source('clarity_ods', 'patient')}} as patient
        on patient.pat_id = infection_abstns.pat_id
    left join {{source('clarity_ods', 'zc_primary_inf')}} as zc_primary_inf
        on zc_primary_inf.primary_inf_c = infection_abstns.primary_inf_c
    left join {{source('clarity_ods', 'zc_uti_catheter_status')}} as zc_uti_catheter_status
        on zc_uti_catheter_status.uti_catheter_status_c = infection_abstns.uti_catheter_status_c
    left join {{source('clarity_ods', 'zc_inf_class')}} as zc_inf_class
        on zc_inf_class.inf_class_c = infection_abstns.inf_class_c
    left join {{source('clarity_ods', 'zc_ssi_event_type')}} as zc_ssi_event_type
        on zc_ssi_event_type.ssi_event_type_c = infection_abstns.ssi_event_type_c
    left join {{source('clarity_ods', 'ncdr_icd_lab')}} as ncdr_icd_lab
        on ncdr_icd_lab.registry_data_id = infection_abstns.registry_data_id
    left join {{source('clarity_ods', 'assocd_lda')}} as assocd_lda
        on assocd_lda.registry_data_id = infection_abstns.registry_data_id and assocd_lda.line = 1
    left join {{source('clarity_ods', 'clarity_concept')}} as clarity_concept
        on clarity_concept.internal_id = infection_abstns.inf_proc_code_id
    left join {{source('clarity_ods', 'ip_lda_noaddsingle')}} as ip_lda_noaddsingle
        on ip_lda_noaddsingle.ip_lda_id = assocd_lda.assocd_lda_id
    left join {{source('clarity_ods', 'flowsheet_lda_types')}} as flowsheet_lda_types
        on flowsheet_lda_types.id = ip_lda_noaddsingle.flo_meas_id
    left join {{source('clarity_ods', 'zc_lines_group')}} as zc_lines_group
        on zc_lines_group.lines_group_c = flowsheet_lda_types.lda_type_ot_c
    left join mbi_lcbi
        on mbi_lcbi.registry_data_id = infection_abstns.registry_data_id
