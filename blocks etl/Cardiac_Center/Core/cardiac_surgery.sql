/*
Cardiac Surgery Stack
Granularity: one row per surgery.
    surgeries can include multiple procedures.
    hospitalizations can include multiple surgeries.
*/

with sts_procedure_level as ( --noqa: PRS
    select -- noqa: L034
        registry_sts_surgery.r_surg_id as casenum,
        registry_sts_procedure.r_proc_term_34 as proc_short_term_34,
        registry_sts_procedure.r_proc_id_32 as proc_id_32,
        registry_sts_procedure.r_proc_m_cat as proc_m_category,
        registry_sts_procedure.r_proc_s_cat as proc_s_category,
        case when lower(registry_sts_surgery.r_reop_rsn) = 'no'
            then 1 else 0 end as index_ind,
        case when registry_sts_surgery_procedure.seq_num = 1
            then 1 else 0 end as primary_proc_ind,
        case when registry_sts_procedure.r_proc_id in (
                    2246, -- transplant, lungs, bilateral (sequential), cadaveric lobe -- noqa: L016
                    2243, -- transplant, lung, single-modifier, with cpb
                    2230, -- transplant, lung(s)
                    2247, -- transplant, lungs, bilateral (sequential), cadaveric lung --noqa: L016
                    2245 -- transplant, lungs, bilateral (sequential)
                ) then 'TRANSPLANT, LUNG(S)'
            when registry_sts_procedure.r_proc_id in (
                2201, -- transplant, heart
                2208 -- transplant, heart, orthotopic: allograft
            ) then 'TRANSPLANT, HEART'
            -- transplant, heart and lung(s), heart / double lung
            when registry_sts_procedure.r_proc_id = 2203
                then 'TRANSPLANT, HEART AND LUNG(S)'
            else registry_sts_procedure.r_proc_nm
        end as proc_name,
        case
            when registry_sts_surgery_procedure.seq_num = 1
                then registry_sts_procedure.r_proc_nm
        end as primary_proc_name,
        case
            when registry_sts_surgery_procedure.seq_num = 1
                then registry_sts_procedure.r_proc_id_32
        end as primary_proc_id_32,
        case
            when registry_sts_surgery_procedure.seq_num = 1
                then registry_sts_procedure.r_proc_id_34
        end as primary_proc_id_34,
        registry_sts_procedure.r_proc_id_34 as proc_id_34,
        case
            when registry_sts_procedure.r_proc_id_32 in ('20', '30')
                or registry_sts_procedure.r_proc_id_34 in
                ('20', '30') then 1 else 0
        end as asd_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '110'
                or registry_sts_procedure.r_proc_id_34 = '110' then 1 else 0
        end as vsd_ind,
        case
            when registry_sts_procedure.r_proc_id_32 in ('350', '360', '370')
                or registry_sts_procedure.r_proc_id_34 in
                ('350', '360', '370') then 1 else 0
        end as tof_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '170'
                or registry_sts_procedure.r_proc_id_34 = '170' then 1 else 0
        end as avc_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '1110'
                or registry_sts_procedure.r_proc_id_34 = '1110' then 1 else 0
        end as aso_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '1120'
                or registry_sts_procedure.r_proc_id_34 = '1120' then 1 else 0
        end as aso_vsd_ind,
        case
            when registry_sts_procedure.r_proc_id_32 in
                ('1670', '1680', '1690', '1700', '2130')
                or registry_sts_procedure.r_proc_id_34 in
                ('1670', '1680', '1690', '1700', '2130') then 1 else 0
        end as glenn_hemi_ind,
        case
            when registry_sts_procedure.r_proc_id_32 in (
                '970', '980', '1000', '1010', '2780', '2790'
            )
            or registry_sts_procedure.r_proc_id_34 in (
                '970', '980', '1000', '1010', '2780', '2790'
            ) then 1 else 0
        end as fontan_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '230'
                or registry_sts_procedure.r_proc_id_34 = '230' then 1 else 0
        end as truncus_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '870'
                or registry_sts_procedure.r_proc_id_34 = '870' then 1 else 0
        end as norwood_ind,
        case
            when registry_sts_procedure.r_proc_id_32 in (
                '1210', '1220', '1230', '1240', '1250', '1280')
                or registry_sts_procedure.r_proc_id_34 in (
                    '1210', '1220', '1230', '1240', '1250', '1280')
                then 1 else 0
        end as coarc_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '890'
                or registry_sts_procedure.r_proc_id_34 = '890' then 1 else 0
        end as heart_tx_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '900'
                or registry_sts_procedure.r_proc_id_34 = '900' then 1 else 0
        end as heart_lung_tx_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '1410'
                or registry_sts_procedure.r_proc_id_34 = '1410' then 1 else 0
        end as lung_tx_ind,
        case
            when registry_sts_procedure.r_proc_id_32 in ('2380', '1920')
                or registry_sts_procedure.r_proc_id_34 in ('2380', '1920')
                then 1 else 0
        end as vad_implant_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '2360'
                or registry_sts_procedure.r_proc_id_34 = '2360' then 1 else 0
        end as ecmo_cannulation_ind,
        case
            when registry_sts_procedure.r_proc_id_32 = '1960'
                or registry_sts_procedure.r_proc_id_34 = '1960' then 1 else 0
        end as delayed_sternal_closure_ind,
        case
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 in ('20', '30')
                    or registry_sts_procedure.r_proc_id_34 in
                    ('20', '30')) then 'ASD'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 = '110'
                    or registry_sts_procedure.r_proc_id_34 = '110') then 'VSD'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 in
                    ('350', '360', '370')
                    or registry_sts_procedure.r_proc_id_34 in
                    ('350', '360', '370'))
                then 'TOF'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 = '170'
                    or registry_sts_procedure.r_proc_id_34 = '170') then 'AVC'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 = '1110'
                    or registry_sts_procedure.r_proc_id_34 = '1110') then 'ASO'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 = '1120'
                    or registry_sts_procedure.r_proc_id_34 = '1120')
                then 'ASO + VSD'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 in
                    ('1670', '1680', '1690', '1700', '2130')
                    or registry_sts_procedure.r_proc_id_34 in
                    ('1670', '1680', '1690', '1700', '2130'))
                then 'GlennHemi'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 in
                    ('970', '980', '1000', '1010', '2780', '2790')
                    or registry_sts_procedure.r_proc_id_34 in
                    ('970', '980', '1000', '1010', '2780', '2790'))
                then 'Fontan'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 = '230'
                    or registry_sts_procedure.r_proc_id_34 = '230')
                then 'Truncus'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 = '870'
                    or registry_sts_procedure.r_proc_id_34 = '870')
                then 'Norwood'
            when index_ind = 1 and primary_proc_ind = 1
                and (registry_sts_procedure.r_proc_id_32 in
                    ('1210', '1220', '1230', '1240', '1250', '1280')
                    or registry_sts_procedure.r_proc_id_34 in
                    ('1210', '1220', '1230', '1240', '1250', '1280'))
                and lower(registry_sts_surgery.r_op_type)
                = 'no cpb cardiovascular'
                then 'Coarc'
        end as benchmark_name,
        case
            when index_ind = 1 and primary_proc_ind = 1
                then registry_sts_procedure.r_stat_cat
        end as stat_level

    from
        {{ source('cdw', 'registry_sts_surgery') }}
        as registry_sts_surgery --noqa: L031
    inner join {{ source('cdw', 'registry_sts_surgery_procedure') }}
        as registry_sts_surgery_procedure --noqa: L031
        on registry_sts_surgery.r_surg_key
            = registry_sts_surgery_procedure.r_surg_key
    inner join {{ source('cdw', 'registry_sts_procedure') }}
        as registry_sts_procedure --noqa: L031
        on registry_sts_surgery_procedure.r_proc_key
            = registry_sts_procedure.r_proc_key
    where
        registry_sts_surgery.cur_rec_ind = 1 -- no soft deletes
        and registry_sts_surgery_procedure.cur_rec_ind = 1 -- no soft deletes
        and registry_sts_procedure.cur_rec_ind = 1 -- no soft deletes
    group by
        registry_sts_surgery.r_surg_id,
        registry_sts_surgery.r_reop_rsn,
        registry_sts_surgery_procedure.seq_num,
        registry_sts_procedure.r_proc_nm,
        registry_sts_procedure.r_proc_id_32,
        registry_sts_procedure.r_proc_id,
        registry_sts_procedure.r_proc_term_34,
        registry_sts_procedure.r_proc_id_34,
        registry_sts_surgery.r_op_type,
        registry_sts_procedure.r_stat_cat,
        registry_sts_procedure.r_proc_m_cat,
        registry_sts_procedure.r_proc_s_cat
),
/*procedure names and indicators concatenated/grouped to the surgery level*/

sts_surgery_level as (
    select
        sts_procedure_level.casenum,
        max(sts_procedure_level.index_ind) as index_ind,
        group_concat(sts_procedure_level.proc_name, ';') as proc_name,
        group_concat(sts_procedure_level.proc_short_term_34, ';')
        as proc_short_term_34,
        max(sts_procedure_level.primary_proc_name) as primary_proc_name,
        max(sts_procedure_level.primary_proc_id_32) as primary_proc_id_32,
        group_concat(sts_procedure_level.proc_id_32, ';') as proc_id_32,
        max(sts_procedure_level.primary_proc_id_34) as primary_proc_id_34,
        group_concat(sts_procedure_level.proc_id_34, ';') as proc_id_34,
        max(sts_procedure_level.asd_ind) as asd_ind,
        max(sts_procedure_level.vsd_ind) as vsd_ind,
        max(sts_procedure_level.tof_ind) as tof_ind,
        max(sts_procedure_level.avc_ind) as avc_ind,
        max(sts_procedure_level.aso_ind) as aso_ind,
        max(sts_procedure_level.aso_vsd_ind) as aso_vsd_ind,
        max(sts_procedure_level.glenn_hemi_ind) as glenn_hemi_ind,
        max(sts_procedure_level.fontan_ind) as fontan_ind,
        max(sts_procedure_level.truncus_ind) as truncus_ind,
        max(sts_procedure_level.norwood_ind) as norwood_ind,
        max(sts_procedure_level.coarc_ind) as coarc_ind,
        max(sts_procedure_level.heart_tx_ind) as heart_tx_ind,
        max(sts_procedure_level.heart_lung_tx_ind) as heart_lung_tx_ind,
        max(sts_procedure_level.lung_tx_ind) as lung_tx_ind,
        max(sts_procedure_level.vad_implant_ind) as vad_implant_ind,
        max(sts_procedure_level.ecmo_cannulation_ind) as ecmo_cannulation_ind,
        max(sts_procedure_level.delayed_sternal_closure_ind)
        as delayed_sternal_closure_ind,
        max(sts_procedure_level.benchmark_name) as benchmark_name,
        max(sts_procedure_level.stat_level) as stat_level,
        max(sts_procedure_level.proc_s_category) as proc_s_category,
        max(sts_procedure_level.proc_m_category) as proc_m_category
    from
        sts_procedure_level
    group by
        sts_procedure_level.casenum
),

/*OR timestamp helper CTE */
or_timestamps as (
    select distinct
        registry_sts_surgery.r_surg_id as casenum,
        cast(
            date(registry_sts_surgery.r_surg_dt) || ' '
            || cast(registry_sts_surgery.r_or_entry_tm as time) as datetime
        ) as or_entry_date,
        cast(registry_sts_surgery.r_or_entry_tm as time) as or_entry_time,
        cast(registry_sts_surgery.r_skin_incs_open_tm as time)
        as skin_incs_open_time,
        cast(registry_sts_surgery.r_skin_incs_closed_tm as time)
        as skin_incs_closed_time,
        cast(registry_sts_surgery.r_or_exit_tm as time) as or_exit_time
    from
        {{ source('cdw', 'registry_sts_surgery') }}
        as registry_sts_surgery --noqa: L031
    inner join {{ source('cdw', 'registry_sts_surgery_procedure') }}
        as registry_sts_surgery_procedure --noqa: L031
        on registry_sts_surgery.r_surg_key
            = registry_sts_surgery_procedure.r_surg_key
    inner join {{ source('cdw', 'registry_sts_procedure') }}
        as registry_sts_procedure --noqa: L031
        on registry_sts_surgery_procedure.r_proc_key
            = registry_sts_procedure.r_proc_key
),

/*STS complications are only tracked at the index surgery level*/
complications as (
    select
        registry_sts_surgery.r_surg_id as casenum,
        -- unplanned interventional cardiovascular catheterization
        -- procedure during the postoperative or postprocedural time period
        max(case when coalesce(
                registry_sts_surgery_complication.r_cmpl_id, -1) = 1064
            and sts_surgery_level.index_ind = 1 then 1
            when sts_surgery_level.index_ind = 0 then -2
            else 0
            end) as unplanned_cath_ind,
        -- unplanned cardiac reoperation during the postoperative or
        -- postprocedural time period, exclusive of reoperation for bleeding.
        max(case when coalesce(
                registry_sts_surgery_complication.r_cmpl_id, -1) = 1063
            and sts_surgery_level.index_ind = 1 then 1
            when sts_surgery_level.index_ind = 0 then -2
            else 0
            end) as unplanned_reop_nonbleeding_ind,
        -- bleeding, requiring reoperation
        max(case when coalesce(
                registry_sts_surgery_complication.r_cmpl_id, -1) = 1060
            and sts_surgery_level.index_ind = 1 then 1
            when sts_surgery_level.index_ind = 0 then -2
            else 0
            end) as unplanned_reop_bleeding_ind,
        max(case when coalesce(
            registry_sts_surgery_complication.r_cmpl_id, -1) in (
            1061, -- sternum left open, planned
            1062, -- sternum left open, unplanned
            3097) -- sternum left open
            and sts_surgery_level.index_ind = 1 then 1
            when sts_surgery_level.index_ind = 0 then -2
            else 0
            end) as sternum_left_open_ind
    from
        {{ source('cdw', 'registry_sts_surgery') }}
        as registry_sts_surgery --noqa: L031
    inner join sts_surgery_level
        on registry_sts_surgery.r_surg_id = sts_surgery_level.casenum
    left join {{ source('cdw', 'registry_sts_surgery_complication') }}
        as registry_sts_surgery_complication --noqa: L031
        on registry_sts_surgery.r_surg_key
            = registry_sts_surgery_complication.r_surg_key
            and registry_sts_surgery_complication.cur_rec_ind = 1
    group by
        registry_sts_surgery.r_surg_id
),
/*
The following CTE links STS surgeries to their respective PC4
CICU encounters by assigning a PC4 enc_key. There is a link
from casenum to PC4 encounter via the PC4 operative table,
but that link only gets generated for PC4 surgical risk
encounters (data specialists fill out the surgical risk model
for the fist cpb or no cpb case during the encounter.
if the patient does not have a cpb or no cpb case during the
encounter, then they have a non-surgical (or medical) risk model).

If there's no link via PC4 operative, we can assign the
CICU encounter based on dates -- if the surgery happened
on the same day as CICU admission, or if surgery was between
CICU admission and discharge.

A single CICU encounter can be assigned to multiple surgeries.
*/

pc4_enc as (
    select
        registry_sts_surgery.r_surg_key,
        registry_sts_surgery.r_surg_id as casenum,
        max(coalesce(registry_pc4_encounter_op_link.r_enc_key,
            registry_pc4_encounter_date_link.r_enc_key))
        as pc4_enc_key

    from
        {{ source('cdw', 'registry_sts_surgery') }}
        as registry_sts_surgery --noqa: L031
    left join {{ source('cdw', 'registry_pc4_operative') }}
        as registry_pc4_operative --noqa: L031
        on registry_sts_surgery.r_surg_id
            = registry_pc4_operative.r_case_num
    left join {{ source('cdw', 'registry_pc4_risk_surgical') }}
        as registry_pc4_risk_surgical --noqa: L031
        on registry_pc4_operative.r_oprtv_key
            = registry_pc4_risk_surgical.r_oprtv_key
            and registry_pc4_risk_surgical.cur_rec_ind = 1
    left join {{ source('cdw', 'registry_pc4_encounter') }}
        as registry_pc4_encounter_op_link --noqa: L031
        on registry_pc4_risk_surgical.r_enc_key
            = registry_pc4_encounter_op_link.r_enc_key
            and registry_pc4_encounter_op_link.cur_rec_ind = 1
    left join {{ source('cdw', 'registry_pc4_encounter') }}
        as registry_pc4_encounter_date_link --noqa: L031
        on registry_sts_surgery.r_hsp_vst_key
            = registry_pc4_encounter_date_link.r_hsp_vst_key
            and ((date(registry_sts_surgery.r_surg_dt)
                >= date(registry_pc4_encounter_date_link.r_cicu_strt_dt)
                and registry_sts_surgery.r_surg_dt
                < registry_pc4_encounter_date_link.r_cicu_phys_end_dt)
                or (date(registry_sts_surgery.r_surg_dt)
                    >= date(registry_pc4_encounter_date_link.r_cicu_strt_dt)
                    and registry_pc4_encounter_date_link.r_cicu_phys_end_dt
                    is null))
            and registry_pc4_encounter_date_link.cur_rec_ind = 1
            and registry_pc4_encounter_date_link.card_unit_adm_ind != 1
    group by
        registry_sts_surgery.r_surg_key,
        registry_sts_surgery.r_surg_id
),

/*pc4_complications_setup flags PC4 postop complications
(at the hospitalization level) that occurred after index procedure*/
pc4_complications_setup as (
    select
        registry_hospital_visit.r_hsp_vst_key as hsp_vst_key,
        max(
            case when registry_pc4_complications.r_ards_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_ards_dt end
        ) as ards_date,
        max(
            case when registry_pc4_arrhythmia.r_arryth_strt_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_arrhythmia.r_arryth_strt_dt end
        ) as arrhythmia_date,
        max(
            case when registry_pc4_arrhythmia.arrhyth_thrpy_perm_pm_ind = 1
                and registry_pc4_arrhythmia.r_arryth_strt_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_arrhythmia.r_arryth_strt_dt end
        ) as arrhythmia_perm_pm_date,
        max(
            case when registry_pc4_complications.r_reop_bleed_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_reop_bleed_dt end
        ) as reop_bleed_date,
        max(
            case when lower(registry_pc4_infection.inf_type_desc) = 'bsi'
                and registry_pc4_infection.r_inf_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_infection.r_inf_dt end
        ) as cabsi_date,
        max(
            case when registry_pc4_cardiac_arrest.r_card_arrest_strt_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_cardiac_arrest.r_card_arrest_strt_dt end
        ) as card_arrest_date,
        max(
            case when registry_pc4_complications.r_chylo_interv_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_chylo_interv_dt end
        ) as chylo_interv_date,
        max(
            case when registry_pc4_therapy.r_acute_renal_fail_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_therapy.r_acute_renal_fail_dt end
        ) as crrt_arf_date,
        max(
            case when lower(registry_pc4_infection.inf_type_desc) = 'wound'
                and registry_pc4_infection.r_inf_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_infection.r_inf_dt end
        ) as ssi_date,
        max(
            case when registry_pc4_complications.endocarditis_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.endocarditis_dt end
        ) as endocard_date,
        max(
            case when registry_pc4_complications.r_hemo_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_hemo_dt end
        ) as hemo_interv_date,
        max(
            case when registry_pc4_complications.r_hepatic_fail_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_hepatic_fail_dt end
        ) as hepatic_fail_date,
        max(
            case when registry_pc4_complications.hepatic_injury_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.hepatic_injury_dt end
        ) as hepatic_injury_date,
        -- V2 intracranial hemorrhage
        max(
            case when registry_pc4_complications.r_hemrg_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_hemrg_dt end
        ) as intracranial_date,
        -- V3 intracranial hemorrhage non-stroke
        max(
            case when registry_pc4_complications.intracranial_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.intracranial_dt end
        ) as intracranial_non_stroke_date,
        max(
            case when registry_pc4_complications.r_ivh_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_ivh_dt end
        ) as ivh_date,
        max(
            coalesce(
                -- V2 listed during CICU encounter
                (case when registry_pc4_complications.r_hrt_trnsplnt_lstd_dt
                    > registry_sts_surgery.r_surg_dt
                    then registry_pc4_complications.r_hrt_trnsplnt_lstd_dt end),
                -- V3 listed during hospitalization
                (case when centripetus_hospitalizationfactors.hosplistednewdt
                    > registry_sts_surgery.r_surg_dt
                    then centripetus_hospitalizationfactors.hosplistednewdt end)
            )) as heart_tx_list_date,
        max(
            case when registry_pc4_complications.r_lcos_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_lcos_dt end
        ) as lcos_date,
        max(
            case when registry_pc4_complications.r_lcos2_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_lcos2_dt end
        ) as lcos2_date,
        max(
            case when registry_pc4_mechanical_support.r_mech_supp_init_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_mechanical_support.r_mech_supp_init_dt end
        ) as mech_support_date,
        max(
            case when lower(registry_pc4_infection.inf_type_desc) = 'nec'
                and registry_pc4_infection.r_inf_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_infection.r_inf_dt end
        ) as nec_date,
        max(
            case when centripetus_pc4compnecbell.necbelldt
                > registry_sts_surgery.r_surg_dt
                then centripetus_pc4compnecbell.necbelldt end
        ) as nec_bell_date,
        max(
            coalesce(
                -- V2 CICU level complication
                (case when registry_pc4_complications.r_paralyzed_diaph_dt
                    > registry_sts_surgery.r_surg_dt
                    then registry_pc4_complications.r_paralyzed_diaph_dt end),
                -- V3 hosp level complication
                (case when centripetus_hospitalizationfactors.hospdiaphragmdt
                    > registry_sts_surgery.r_surg_dt
                    then centripetus_hospitalizationfactors.hospdiaphragmdt end)
            )) as paralyzed_diaph_date,
        max(
            case when registry_pc4_complications.r_peri_effus_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_peri_effus_dt end
        ) as peri_effus_date,
        max(
            case when registry_pc4_complications.r_phtn_strt_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_phtn_strt_dt end
        ) as phtn_therapy_date,
        max(
            coalesce(
                -- V2 pleural effusion requiring chest tube
                (case when registry_pc4_complications.r_plrl_eff_chsttb_dt
                    > registry_sts_surgery.r_surg_dt
                    then registry_pc4_complications.r_plrl_eff_chsttb_dt end),
                -- V3 pleutal effusion requiring chest tube
                (case when registry_pc4_complications.effusion_tube_dt
                    > registry_sts_surgery.r_surg_dt
                    then registry_pc4_complications.effusion_tube_dt end)
            )) as plrl_effus_date,
        -- V2 pneumonia
        max(
            case when registry_pc4_complications.r_pneumonia_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_pneumonia_dt end
        ) as pneumonia_date,
        -- V3 non-vap pneumonia
        max(
            case when registry_pc4_complications.non_vap_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.non_vap_dt end
        ) as non_vap_pneumonia_date,
        max(
            coalesce(
                (case when registry_pc4_complications.r_pneumo_chsttb_dt
                    > registry_sts_surgery.r_surg_dt
                    then registry_pc4_complications.r_pneumo_chsttb_dt end),
                (case when registry_pc4_complications.pneumothorax_tube_dt
                    > registry_sts_surgery.r_surg_dt
                    then registry_pc4_complications.pneumothorax_tube_dt end)
            )) as pneumothorax_tube_date,
        max(
            case when registry_pc4_complications.pressure_ulcer_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.pressure_ulcer_dt end
        ) as pressure_ulcer_date,
        max(
            case when registry_pc4_complications.r_pulm_embol_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_pulm_embol_dt end
        ) as pulm_embol_date,
        max(
            case when registry_pc4_complications.r_seizure_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_seizure_dt end
        ) as seizure_date,
        max(
            case when registry_pc4_complications.r_sepsis_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_sepsis_dt end
        ) as sepsis_date,
        max(
            coalesce(
                (case when registry_pc4_complications.r_stroke_dt
                    > registry_sts_surgery.r_surg_dt
                    then registry_pc4_complications.r_stroke_dt end),
                (case when centripetus_pc4compstroke.strokedttm
                    > registry_sts_surgery.r_surg_dt
                    then centripetus_pc4compstroke.strokedttm end)
            )) as stroke_date,
        max(
            case when registry_pc4_complications.r_sup_wound_inf_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_sup_wound_inf_dt end
        ) as sup_ssi_date,
        max(
            case when registry_pc4_complications.r_reop_unplan_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_complications.r_reop_unplan_dt end
        ) as reop_unplan_date,
        max(
            case when lower(registry_pc4_infection.inf_type_desc) = 'uti'
                and registry_pc4_infection.r_inf_dt
                > registry_sts_surgery.r_surg_dt
                then registry_pc4_infection.r_inf_dt end
        ) as uti_date,
        max(
            coalesce(
                (case when registry_pc4_complications.r_vocal_cord_dys_dt
                    > registry_sts_surgery.r_surg_dt
                    then registry_pc4_complications.r_vocal_cord_dys_dt end),
                (case when centripetus_hospitalizationfactors.hospvocalcorddt
                    > registry_sts_surgery.r_surg_dt
                    then centripetus_hospitalizationfactors.hospvocalcorddt end)
            )) as vocal_cord_dys_date

    from
        {{ source('cdw', 'registry_pc4_complications') }}
        as registry_pc4_complications --noqa: L031
    inner join {{ source('cdw', 'registry_pc4_encounter') }}
        as registry_pc4_encounter --noqa: L031
        on registry_pc4_complications.r_enc_key
            = registry_pc4_encounter.r_enc_key
    inner join {{ source('cdw', 'registry_hospital_visit') }}
        as registry_hospital_visit --noqa: L031
        on registry_pc4_encounter.r_hsp_vst_key
            = registry_hospital_visit.r_hsp_vst_key
    left join {{ source('cdw', 'registry_pc4_arrhythmia') }}
        as registry_pc4_arrhythmia --noqa: L031
        on registry_pc4_complications.r_enc_key
            = registry_pc4_arrhythmia.r_enc_key
    left join {{ source('cdw', 'registry_pc4_cardiac_arrest') }}
        as registry_pc4_cardiac_arrest --noqa: L031
        on registry_pc4_complications.r_enc_key
            = registry_pc4_cardiac_arrest.r_enc_key
    left join {{ source('cdw', 'registry_pc4_infection') }}
        as registry_pc4_infection --noqa: L031
        on registry_pc4_complications.r_enc_key
            = registry_pc4_infection.r_enc_key
            and registry_pc4_infection.cur_rec_ind = 1
    left join {{ source('cdw', 'registry_pc4_mechanical_support') }}
        as registry_pc4_mechanical_support --noqa: L031
        on registry_pc4_complications.r_enc_key
            = registry_pc4_mechanical_support.r_enc_key
    left join {{ source('cdw', 'registry_pc4_therapy') }}
        as registry_pc4_therapy --noqa: L031
        on registry_pc4_complications.r_enc_key
            = registry_pc4_therapy.r_enc_key
    left join {{ source('cdw', 'registry_sts_surgery') }}
        as registry_sts_surgery --noqa: L031
        on registry_hospital_visit.r_hsp_vst_key
            = registry_sts_surgery.r_hsp_vst_key
            and lower(
                registry_sts_surgery.r_reop_rsn) = 'no' -- index procedure only
    left join {{ source('ccis_ods', 'centripetus_pc4compstroke') }}
        as centripetus_pc4compstroke --noqa: L031
        on registry_pc4_encounter.r_enc_id
            = centripetus_pc4compstroke.encounterid
    left join {{ source('ccis_ods', 'centripetus_pc4compnecbell') }}
        as centripetus_pc4compnecbell --noqa: L031
        on registry_pc4_encounter.r_enc_id
            = centripetus_pc4compnecbell.encounterid
    left join {{ source('ccis_ods', 'centripetus_hospitalizationfactors') }}
        as centripetus_hospitalizationfactors --noqa: L031
        on registry_hospital_visit.r_hsp_vst_id
            = centripetus_hospitalizationfactors.hospitalizationid

    group by
        registry_hospital_visit.r_hsp_vst_key
),

pc4_complications_concat as (
    select
        hsp_vst_key,
        max(case when ards_date is not null
            then 'ARDS (available through PC4 V2)'
            else '' end)
        || max(case when arrhythmia_date is not null then '; '
            || 'Arrhythmia requiring therapy' else '' end)
        || max(case when reop_bleed_date is not null then '; '
            || 'Bleeding requiring reoperation' else '' end)
        || max(case when cabsi_date is not null then '; '
            || 'CA-BSI' else '' end)
        || max(case when card_arrest_date is not null then '; '
            || 'Cardiac Arrest' else '' end)
        || max(case when chylo_interv_date is not null then '; '
            || 'Chylothorax requiring intervention' else '' end)
        || max(case when crrt_arf_date is not null then '; '
            || 'CRRT for acute renal failure' else '' end)
        || max(case when ssi_date is not null then '; '
            || 'Deep SSI' else '' end)
        || max(case when endocard_date is not null then '; '
            || 'Endocarditis' else '' end)
        || max(case when hemo_interv_date is not null then '; '
            || 'Hemothorax requiring intervention (available through PC4 V2)'
            else '' end)
        || max(case when hepatic_fail_date is not null then '; '
            || 'Hepatic failure (available through PC4 V2)' else '' end)
        || max(case when hepatic_injury_date is not null then '; '
            || 'Hepatic injury (available PC4 V3)' else '' end)
        || max(case when intracranial_date is not null then '; '
            || 'Intracranial hemorrhage (available through PC4 V2)' else '' end)
        || max(case when intracranial_non_stroke_date is not null then '; '
            || 'Intracranial hemorrhage non-stroke (available PC4 V3)'
            else '' end)
        || max(case when ivh_date is not null then '; '
            || 'IVH > grade II' else '' end)
        || max(case when heart_tx_list_date is not null then '; '
            || 'Listed for heart transplant' else '' end)
        || max(case when lcos_date is not null then '; '
            || 'LCOS (available through PC4 V2)' else '' end)
        || max(case when lcos2_date is not null then '; '
            || 'LCOS II (available PC4 V3)' else '' end)
        || max(case when mech_support_date is not null then '; '
            || 'Mechanical circulatory support' else '' end)
        || max(case when nec_date is not null then '; '
            || 'NEC (available through PC4 V2)' else '' end)
        || max(case when nec_bell_date is not null then '; '
            || 'NEC Bells Criteria (available PC4 V3)' else '' end)
        || max(case when non_vap_pneumonia_date is not null then '; '
            || 'Non-VAP Pneumonia' else '' end)
        || max(case when paralyzed_diaph_date is not null then '; '
            || 'Paralyzed diaphragm (availabe through PC4 V2)' else '' end)
        || max(case when peri_effus_date is not null then '; '
            || 'Pericardial effusion' else '' end)
        || max(case when phtn_therapy_date is not null then '; '
            || 'PHTN' else '' end)
        || max(case when plrl_effus_date is not null then '; '
            || 'Pleural effusion/hemothorax requiring chest tube' else '' end)
        || max(case when pneumonia_date is not null then '; '
            || 'Pneumonia' else '' end)
        || max(case when pneumothorax_tube_date is not null then '; '
            || 'Pneumothorax requiring chest tube' else '' end)
        || max(case when pressure_ulcer_date is not null then '; '
            || 'Pressure ulcer' else '' end)
        || max(case when pulm_embol_date is not null then '; '
            || 'Pulmonary embolism' else '' end)
        || max(case when seizure_date is not null then '; '
            || 'Seizure' else '' end)
        || max(case when sepsis_date is not null then '; '
            || 'Sepsis' else '' end)
        || max(case when stroke_date is not null then '; '
            || 'Stroke' else '' end)
        || max(case when sup_ssi_date is not null then '; '
            || 'Superficial SSI' else '' end)
        || max(case when reop_unplan_date is not null then '; '
            || 'Unplanned reoperation (available through PC4 V2)' else '' end)
        || max(case when uti_date is not null then '; '
            || 'UTI' else '' end)
        || max(case when vocal_cord_dys_date is not null then '; '
            || 'Vocal cord dysfunction (available through PC4 V2)' else '' end)
        as pc4_complication_names

    from pc4_complications_setup
    group by hsp_vst_key
),

/*CTE to flag index procedures according to the STS definition,
where only CPB type operations can count as the index. this is
different from existing "index_ind" which is based off of the
reoperations field. we can have hospitalizations without an
"sts_index_ind" */

sts_index_procedures as (
    select
        centripetus_cases.hospitalizationid,
        centripetus_cases.casenumber,
        centripetus_cases.surgdt,
        centripetus_cases.orentryt,
        row_number() over (partition by centripetus_cases.hospitalizationid
            order by centripetus_cases.surgdt,
                centripetus_cases.orentryt) as index_seq

    from {{ source('ccis_ods', 'centripetus_cases') }}
        as centripetus_cases --noqa: L031

    where centripetus_cases.optype in (
        311, -- cpb
        312, -- no cpb cardiovascular
        320, -- cpb standby
        3678 -- cpb cardiovascular
        )

),

surg_diagnosis as (
    select
        centripetus_diagnosis.casenumber,
        centripetus_diagnosis.diagnosisname as primary_surg_diagnosis

    from {{ source('ccis_ods', 'centripetus_diagnosis') }}
        as centripetus_diagnosis --noqa: L031
    where centripetus_diagnosis.sort = 1
)


select distinct -- noqa: L034
    stg_patient.pat_key,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    stg_patient.sex,
    cast(registry_sts_surgery.r_surg_key as varchar(10)) as cardiac_study_id,
    registry_hospital_visit.r_hsp_vst_key as hsp_vst_key,
    registry_hospital_visit.visit_key,
    registry_sts_surgery.caselinknum as log_id,
    or_log.log_key,
    stg_anesthesia_orlog_crosswalk.anesthesia_log_link as anesthesia_log_id,
    anesthesia_encounter_link.anes_key,
    centripetus_cases.emreventid as surgery_csn,
    registry_hospital_visit.r_admit_dt as hospital_admit_date,
    registry_hospital_visit.r_disch_dt as hospital_discharge_date,
    round((
        extract(epoch from registry_hospital_visit.r_disch_dt -- noqa: L027
            - registry_hospital_visit.r_admit_dt) / 60.0 / 60 / 24
    ), 2) as hospital_los_days,
    registry_sts_surgery.r_surg_id as casenum,
    registry_sts_surgery.r_data_spec_version as data_spec_version,
    year(registry_sts_surgery.r_surg_dt) as surg_year,
    registry_sts_surgery.r_surg_dt as surg_date,
    /*
    OR timestamps are stored as datetime fields, but only the time
    components are accurate (e.g. one might come through as
    '1900-01-01 12:45:00). the following case statements check
    for overnight procedures by comparing OR entry time with the
    subsequent OR times, and then concatenate the time with the
    surg date (or surg date + 1 day if overnight)
    */
    or_timestamps.or_entry_date,
    case
        when or_timestamps.or_entry_time > or_timestamps.skin_incs_open_time
            then cast(
                date(registry_sts_surgery.r_surg_dt) || ' '
                || or_timestamps.skin_incs_open_time as datetime)
            + interval '1 day'
        else cast(
            date(registry_sts_surgery.r_surg_dt) || ' '
            || or_timestamps.skin_incs_open_time as datetime)
    end as skin_incs_open_date,
    case
        when or_timestamps.or_entry_time > or_timestamps.skin_incs_closed_time
            then cast(date(registry_sts_surgery.r_surg_dt) || ' '
                || or_timestamps.skin_incs_closed_time as datetime)
            + interval '1 day'
        else cast(date(registry_sts_surgery.r_surg_dt) || ' '
            || or_timestamps.skin_incs_closed_time as datetime)
    end as skin_incs_closed_date,
    case
        when or_timestamps.or_entry_time > or_timestamps.or_exit_time
            then cast(date(registry_sts_surgery.r_surg_dt)
                || ' ' || or_timestamps.or_exit_time as datetime)
            + interval '1 day'
        else cast(date(registry_sts_surgery.r_surg_dt)
            || ' ' || or_timestamps.or_exit_time as datetime)
    end as or_exit_date,
    registry_sts_provider.r_prov_last_nm as surgeon,
    sts_surgery_level.proc_name,
    sts_surgery_level.proc_short_term_34,
    sts_surgery_level.primary_proc_name,
    sts_surgery_level.primary_proc_id_32,
    sts_surgery_level.proc_id_32,
    sts_surgery_level.primary_proc_id_34,
    sts_surgery_level.proc_id_34,
    registry_sts_surgery.r_op_type as op_type,
    case when lower(registry_sts_surgery.r_op_type) in (
        'cpb cardiovascular',
        'cpb',
        'vad operation done with cpb',
        'cpb non-cardiovascular') then 'OPEN' else 'CLOSED'
    end as open_closed,
    sts_surgery_level.index_ind,
    case when sts_index_procedures.index_seq = 1
        then 1 else 0 end as sts_index_ind,
    registry_sts_surgery.r_pat_wt_kg as surg_weight_kg,
    registry_sts_surgery.r_pat_ht_cm as surg_height_cm,
    sqrt((surg_weight_kg * surg_height_cm) / 3600) as surg_bsa,
    round((
        extract(epoch from registry_sts_surgery.r_surg_dt -- noqa: L027
            - stg_patient.dob) / 60.0 / 60 / 24
    ), 2) as surg_age_days,
    case
        when surg_age_days <= 30 then 'NEONATE'
        when surg_age_days >= 30.1 and surg_age_days <= 365 then 'INFANT'
        when surg_age_days >= 365.1 and surg_age_days <= 6575 then 'CHILD'
        when surg_age_days >= 3675.1 then 'ADULT'
    end as surg_age_category,
    sts_surgery_level.asd_ind,
    sts_surgery_level.vsd_ind,
    sts_surgery_level.tof_ind,
    sts_surgery_level.avc_ind,
    sts_surgery_level.aso_ind,
    sts_surgery_level.aso_vsd_ind,
    sts_surgery_level.glenn_hemi_ind,
    sts_surgery_level.fontan_ind,
    sts_surgery_level.truncus_ind,
    sts_surgery_level.norwood_ind,
    sts_surgery_level.coarc_ind,
    sts_surgery_level.heart_tx_ind,
    sts_surgery_level.heart_lung_tx_ind,
    sts_surgery_level.lung_tx_ind,
    sts_surgery_level.vad_implant_ind,
    sts_surgery_level.ecmo_cannulation_ind,
    sts_surgery_level.delayed_sternal_closure_ind,
    sts_surgery_level.stat_level,
    sts_surgery_level.benchmark_name,
    surg_diagnosis.primary_surg_diagnosis,
    sts_surgery_level.proc_m_category,
    sts_surgery_level.proc_s_category,
    registry_sts_surgery_perfusion.cpb_mn as cpb_minutes,
    registry_sts_surgery_perfusion.xclamp_mn as crossclamp_minutes,
    registry_sts_surgery_perfusion.dhca_mn as circ_arrest_minutes,
    registry_sts_surgery.r_reop_rsn as reop,
    complications.unplanned_cath_ind,
    complications.unplanned_reop_nonbleeding_ind,
    complications.unplanned_reop_bleeding_ind,
    complications.sternum_left_open_ind,
    registry_sts_surgery.r_mort_case_ind as mort_case_ind,
    case when lower(registry_sts_surgery.r_mort_30_stat) = 'alive' then 0
        when lower(registry_sts_surgery.r_mort_30_stat) = 'dead' then 1
        else -2
    end as mort_30_ind,
    case when lower(registry_hospital_visit.r_disch_mort_stat) = 'alive' then 0
        when lower(registry_hospital_visit.r_disch_mort_stat) = 'dead' then 1
        else -2
    end as mort_dc_stat_ind,
    pc4_enc.pc4_enc_key,
    case when sts_surgery_level.index_ind = 1
        -- no PDA ligations for patients <2.5kg
        and not (surg_weight_kg < 2.5
            and sts_surgery_level.primary_proc_id_32 = '1330')
        -- no neonate pacemakers
        and not (surg_age_days <= 30.0
            and sts_surgery_level.primary_proc_id_32 in (
                '1450', '1460', '1470', '1480', '2350'))
        and sts_surgery_level.stat_level is not null
        and registry_hospital_visit.r_disch_dt is not null
        and pc4_enc.pc4_enc_key is not null
        then 1 else 0 end as pc4_postop_complication_denom_ind,
    case
        when (pc4_postop_complication_denom_ind = 0
            or pc4_complications_concat.pc4_complication_names is null
            or pc4_complications_concat.pc4_complication_names = '') then null
        when pc4_complications_concat.pc4_complication_names like ';%'
            then trim(
                leading from substr( -- noqa: L027
                    pc4_complications_concat.pc4_complication_names,
                    instr(
                        pc4_complications_concat.pc4_complication_names, '; ') + 1)) -- noqa: L016
        else pc4_complications_concat.pc4_complication_names
    end as pc4_postop_complication_names
from
    {{ source('cdw', 'registry_sts_surgery') }}
    as registry_sts_surgery --noqa: L031
inner join sts_surgery_level
    on registry_sts_surgery.r_surg_id = sts_surgery_level.casenum
inner join {{ source('cdw', 'registry_hospital_visit') }}
    as registry_hospital_visit --noqa: L031
    on registry_sts_surgery.r_hsp_vst_key
        = registry_hospital_visit.r_hsp_vst_key
left join {{ source('cdw', 'registry_sts_provider') }}
    as registry_sts_provider --noqa: L031
    on registry_sts_surgery.surgn_r_prov_key = registry_sts_provider.r_prov_key
        and registry_sts_provider.cur_rec_ind = 1 -- no soft deletes
left join {{ source('cdw', 'registry_sts_surgery_perfusion') }}
    as registry_sts_surgery_perfusion --noqa: L031
    on registry_sts_surgery.r_surg_key
        = registry_sts_surgery_perfusion.r_surg_key
        and registry_sts_surgery_perfusion.cur_rec_ind = 1
inner join {{ ref('stg_patient') }} as stg_patient --noqa: L031
    on registry_hospital_visit.pat_key = stg_patient.pat_key
inner join or_timestamps
    on registry_sts_surgery.r_surg_id = or_timestamps.casenum
inner join complications
    on registry_sts_surgery.r_surg_id = complications.casenum
inner join pc4_enc
    on registry_sts_surgery.r_surg_key = pc4_enc.r_surg_key
left join pc4_complications_concat
    on registry_sts_surgery.r_hsp_vst_key = pc4_complications_concat.hsp_vst_key
left join {{ source('ccis_ods', 'centripetus_cases') }}
    as centripetus_cases --noqa: L031
    on centripetus_cases.casenumber = registry_sts_surgery.r_surg_id
left join {{ source('cdw', 'or_log') }} as or_log --noqa: L031
    on registry_sts_surgery.caselinknum = or_log.log_id
left join {{ source('cdw', 'anesthesia_encounter_link') }}
    as anesthesia_encounter_link --noqa: L031
    on or_log.log_key = anesthesia_encounter_link.or_log_key
left join sts_index_procedures
    on registry_sts_surgery.r_surg_id = sts_index_procedures.casenumber
        and sts_index_procedures.index_seq = 1
left join surg_diagnosis
    on registry_sts_surgery.r_surg_id = surg_diagnosis.casenumber
left join {{ ref('stg_anesthesia_orlog_crosswalk') }}
    as stg_anesthesia_orlog_crosswalk -- noqa: L031
    on registry_sts_surgery.caselinknum = stg_anesthesia_orlog_crosswalk.log_id

where
    registry_sts_surgery.cur_rec_ind = 1 -- no soft deletes
    and registry_hospital_visit.cur_rec_ind = 1 -- no soft deletes
    and registry_hospital_visit.r_hsp_key = 2010 -- chop only
