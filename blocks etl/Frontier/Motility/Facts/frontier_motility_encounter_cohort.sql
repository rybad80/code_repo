with
cohort_build as (--region:
    select
        visit_hx.visit_key,
        visit_hx.csn,
        visit_hx.patient_name,
        dx_life.mrn,
        visit_hx.encounter_date,
        visit_hx.provider_name,
        visit_hx.provider_id,
        visit_hx.department_name,
        visit_hx.department_id,
        visit_hx.visit_type,
        visit_hx.visit_type_id,
        visit_hx.encounter_type,
        visit_hx.encounter_type_id,
        visit_hx.specialty_name,
        visit_hx.inpatient_ind,
        ip_hx.motility_inpatient_ind,
        ip_hx.admit_start_date,
        ip_hx.inpatient_admit_date,
        ip_hx.discharge_date,
        ip_hx.inpatient_los_days,
        ip_hx.admit_num,
        ip_hx.admission_service,
        ip_hx.discharge_service,
        visit_hx.aadp_multi_d_ind,
        visit_hx.neuromodulation_visit_ind,
        visit_hx.general_motility_visit_ind,
        visit_hx.defecation_disorder_visit_ind,
        visit_hx.multi_disciplinary_visit_ind,
        visit_hx.life_bowel_dysmotility_visit_ind,
        dx_life.general_motility_blu_ind,
        dx_life.general_motility_non_blu_ind,
        dx_life.general_motility_yel_ind,
        dx_life.general_motility_blk_ind,
        dx_life.general_motility_grn_ind,
        dx_life.defecation_disorder_blu_ind,
        dx_life.defecation_disorder_non_blu_ind,
        dx_life.defecation_disorder_yel_ind,
        dx_life.defecation_disorder_blk_ind,
        dx_life.defecation_disorder_grn_ind,
        dx_life.multi_disciplinary_blu_ind,
        dx_life.multi_disciplinary_non_blu_ind,
        dx_life.multi_disciplinary_grn_ind,
        dx_life.life_bowel_dysmotility_blu_ind,
        dx_life.life_bowel_dysmotility_non_blu_ind,
        dx_life.life_bowel_dysmotility_yel_ind,
        dx_life.neuromodulation_blu_ind,
        procedure_life.gen_mot_cpt_ind,
        procedure_life.def_dis_cpt_ind,
        procedure_life.multi_d_cpt_ind,
        procedure_life.neuromod_cpt_ind,
        dx_life.pat_key,
        case
            when ip_hx.motility_inpatient_ind = 1
            then ip_hx.hsp_acct_key
            else visit_hx.hsp_acct_key
        end as hsp_acct_key,
        visit_hx.fiscal_year,
        visit_hx.visual_month
    from
        {{ ref('stg_frontier_motility_dx_life') }} as dx_life
        left join {{ ref('stg_frontier_motility_proc_life') }} as procedure_life
            on dx_life.pat_key = procedure_life.pat_key
        left join {{ ref('stg_frontier_motility_visit_hx') }} as visit_hx
            on dx_life.pat_key = visit_hx.pat_key
        left join {{ ref('stg_frontier_motility_ip_hx') }} as ip_hx
            on visit_hx.visit_key = ip_hx.visit_key
    where
        aadp_multi_d_ind = '1'
        or (neuromodulation_visit_ind + general_motility_visit_ind
            + defecation_disorder_visit_ind + multi_disciplinary_visit_ind
            + life_bowel_dysmotility_visit_ind + motility_inpatient_ind > 0
                or gen_mot_cpt_ind + def_dis_cpt_ind + multi_d_cpt_ind + neuromod_cpt_ind > 0
            )
            and (motility_inpatient_ind = 1
                or (dx_life.general_motility_blu_ind
                    + dx_life.defecation_disorder_blu_ind
                    + dx_life.multi_disciplinary_blu_ind
                    + dx_life.life_bowel_dysmotility_blu_ind > 0
                    and gi_department_ind = 1
                    )
                or (dx_life.neuromodulation_blu_ind + neuromodulation_visit_ind = 2)
                or (
                        (
                                (dx_life.general_motility_yel_ind
                                    + dx_life.general_motility_blk_ind
                                    + dx_life.general_motility_grn_ind > 0
                                    and (gen_mot_cpt_ind = 1 or general_motility_visit_ind = 1)
                                )
                            or (dx_life.defecation_disorder_yel_ind
                                + dx_life.defecation_disorder_blk_ind
                                + dx_life.defecation_disorder_grn_ind > 0
                                and (def_dis_cpt_ind = 1 or defecation_disorder_visit_ind = 1)
                                )
                            or (dx_life.multi_disciplinary_grn_ind = 1
                                    and (multi_d_cpt_ind = 1
                                        or multi_disciplinary_visit_ind = 1)
                                )
                            or (dx_life.life_bowel_dysmotility_yel_ind = 1
                                    and (life_bowel_dysmotility_visit_ind = 1)
                                )
                        )
                    and (physician_ind + other_provider_ind > 0
                            and specialty_deptartment_ind = 1
                        )
                    )
                )
    --end region
),
sub_sup as (--region: this is to establish the hierarchical order of the groups
    select
        cohort_build.pat_key,
        cohort_build.fiscal_year,
        max(case when cohort_build.neuromodulation_blu_ind = 1
            then 1 else 0 end) as neuromodulation_blu_ind,
        max(case when cohort_build.life_bowel_dysmotility_non_blu_ind = 1
            then 1 else 0 end) as life_bowel_dysmotility_non_blu_ind,
        max(case when cohort_build.multi_disciplinary_non_blu_ind = 1
            then 1 else 0 end) as multi_disciplinary_non_blu_ind,
        max(case when cohort_build.multi_d_cpt_ind = 1
            then 1 else 0 end) as multi_d_cpt_ind,
        max(case when cohort_build.defecation_disorder_non_blu_ind = 1
            then 1 else 0 end) as defecation_disorder_non_blu_ind,
        max(case when cohort_build.def_dis_cpt_ind = 1
            then 1 else 0 end) as def_dis_cpt_ind,
        max(case when cohort_build.general_motility_non_blu_ind = 1
            then 1 else 0 end) as general_motility_non_blu_ind,
        max(case when cohort_build.gen_mot_cpt_ind = 1
            then 1 else 0 end) as gen_mot_cpt_ind,
        max(case when cohort_build.life_bowel_dysmotility_blu_ind = 1
            then 1 else 0 end) as life_bowel_dysmotility_blu_ind,
        max(case when cohort_build.multi_disciplinary_blu_ind = 1
            then 1 else 0 end) as multi_disciplinary_blu_ind,
        max(case when cohort_build.defecation_disorder_blu_ind = 1
            then 1 else 0 end) as defecation_disorder_blu_ind,
        max(case when cohort_build.general_motility_blu_ind = 1
            then 1 else 0 end) as general_motility_blu_ind,
        max(case when cohort_build.neuromodulation_visit_ind = 1
            then 1 else 0 end) as neuromodulation_visit_ind,
        max(case when cohort_build.life_bowel_dysmotility_visit_ind = 1
            then 1 else 0 end) as life_bowel_dysmotility_visit_ind,
        max(case when cohort_build.multi_disciplinary_visit_ind = 1
            then 1 else 0 end) as multi_disciplinary_visit_ind,
        max(case when cohort_build.defecation_disorder_visit_ind = 1
            then 1 else 0 end) as defecation_disorder_visit_ind,
        max(case when cohort_build.general_motility_visit_ind = 1
            then 1 else 0 end) as general_motility_visit_ind
    from
        cohort_build
    group by
        cohort_build.pat_key,
        cohort_build.fiscal_year
    --end region
),
sub_cohort as (--region: this also establishes the hierarchical order of the groups w/ non-blue first
    select
        sub_sup.pat_key,
        sub_sup.fiscal_year,
        case
            when sub_sup.neuromodulation_blu_ind = 1
            then 'neuromodulation'
            when  sub_sup.life_bowel_dysmotility_non_blu_ind = 1
            then 'll-bowel-dysmotilityn non-blue'
            when sub_sup.multi_disciplinary_non_blu_ind = 1
                and sub_sup.multi_d_cpt_ind = 1
            then 'multi-d non-blue w/cpt'
            when sub_sup.general_motility_non_blu_ind = 1
                and sub_sup.gen_mot_cpt_ind = 1
            then 'general-motility non-blue w/ cpt'
            when sub_sup.defecation_disorder_non_blu_ind = 1
                and sub_sup.def_dis_cpt_ind = 1
            then 'defecation-disorder non-blue w/cpt'
            when sub_sup.life_bowel_dysmotility_blu_ind = 1
            then 'll-bowel-dysmotilityn blue'
            when sub_sup.multi_disciplinary_blu_ind = 1
            then 'multi-d blue'
            when sub_sup.general_motility_blu_ind = 1
            then 'general-motility blue'
            when sub_sup.defecation_disorder_blu_ind = 1
            then 'defecation-disorder blue'
        else null end as motility_cpt_groups,
        case
            when sub_sup.neuromodulation_blu_ind = 1
                and sub_sup.neuromodulation_visit_ind = 1
            then 'neuromodulation blue/all w/ visit-type'
            when sub_sup.life_bowel_dysmotility_non_blu_ind = 1
                and sub_sup.life_bowel_dysmotility_visit_ind = 1
            then 'll-bowel-dysmotilityn non-blue w/ visit-type'
            when sub_sup.multi_disciplinary_non_blu_ind = 1
                and sub_sup.multi_disciplinary_visit_ind = 1
            then 'multi-d non-blue w/ visit-type'
            when sub_sup.defecation_disorder_non_blu_ind = 1
                and sub_sup.defecation_disorder_visit_ind = 1
            then 'defecation-disorder non-blue w/ visit-type'
            when sub_sup.general_motility_non_blu_ind = 1
                and sub_sup.general_motility_visit_ind = 1
            then 'general-motility non-blue w/ visit-type'
            when sub_sup.life_bowel_dysmotility_blu_ind = 1
            then 'll-bowel-dysmotilityn blue'
            when sub_sup.multi_disciplinary_blu_ind = 1
            then 'multi-d blue'
            when sub_sup.defecation_disorder_blu_ind = 1
            then 'defecation-disorder blue'
            when sub_sup.general_motility_blu_ind = 1
            then 'general-motility blue'
        else null end as motility_visit_groups
    from
        sub_sup
    group by
        sub_sup.pat_key,
        sub_sup.fiscal_year,
        sub_sup.neuromodulation_blu_ind,
        sub_sup.life_bowel_dysmotility_non_blu_ind,
        sub_sup.multi_disciplinary_non_blu_ind,
        sub_sup.multi_d_cpt_ind,
        sub_sup.defecation_disorder_non_blu_ind,
        sub_sup.def_dis_cpt_ind,
        sub_sup.general_motility_non_blu_ind,
        sub_sup.gen_mot_cpt_ind,
        sub_sup.life_bowel_dysmotility_blu_ind,
        sub_sup.multi_disciplinary_blu_ind,
        sub_sup.defecation_disorder_blu_ind,
        sub_sup.general_motility_blu_ind,
        sub_sup.neuromodulation_visit_ind,
        sub_sup.life_bowel_dysmotility_visit_ind,
        sub_sup.multi_disciplinary_visit_ind,
        sub_sup.defecation_disorder_visit_ind,
        sub_sup.general_motility_visit_ind
    --end region
),
cohort_output as (--region:
    select
        cohort_build.visit_key,
        cohort_build.csn,
        cohort_build.patient_name,
        cohort_build.mrn,
        sub_cohort.motility_cpt_groups,
        sub_cohort.motility_visit_groups,
        cohort_build.encounter_date,
        cohort_build.provider_name,
        cohort_build.provider_id,
        cohort_build.department_name,
        cohort_build.department_id,
        cohort_build.visit_type,
        cohort_build.visit_type_id,
        cohort_build.encounter_type,
        cohort_build.encounter_type_id,
        cohort_build.specialty_name,
        cohort_build.inpatient_ind,
        coalesce(cohort_build.motility_inpatient_ind, 0)
        as motility_inpatient_ind,
        cohort_build.admit_start_date,
        cohort_build.inpatient_admit_date,
        cohort_build.discharge_date,
        cohort_build.inpatient_los_days,
        cohort_build.admit_num,
        cohort_build.admission_service,
        cohort_build.discharge_service,
        cohort_build.neuromodulation_visit_ind,
        cohort_build.general_motility_visit_ind,
        cohort_build.defecation_disorder_visit_ind,
        cohort_build.multi_disciplinary_visit_ind,
        cohort_build.life_bowel_dysmotility_visit_ind,
        cohort_build.general_motility_blu_ind,
        cohort_build.general_motility_non_blu_ind,
        cohort_build.general_motility_yel_ind,
        cohort_build.general_motility_blk_ind,
        cohort_build.general_motility_grn_ind,
        cohort_build.defecation_disorder_blu_ind,
        cohort_build.defecation_disorder_non_blu_ind,
        cohort_build.defecation_disorder_yel_ind,
        cohort_build.defecation_disorder_blk_ind,
        cohort_build.defecation_disorder_grn_ind,
        cohort_build.multi_disciplinary_blu_ind,
        cohort_build.multi_disciplinary_non_blu_ind,
        cohort_build.multi_disciplinary_grn_ind,
        cohort_build.life_bowel_dysmotility_blu_ind,
        cohort_build.life_bowel_dysmotility_non_blu_ind,
        cohort_build.life_bowel_dysmotility_yel_ind,
        cohort_build.neuromodulation_blu_ind,
        cohort_build.gen_mot_cpt_ind,
        cohort_build.def_dis_cpt_ind,
        cohort_build.multi_d_cpt_ind,
        cohort_build.neuromod_cpt_ind,
        sub_cohort.pat_key,
        cohort_build.hsp_acct_key,
        cohort_build.fiscal_year,
        cohort_build.visual_month
    from
        sub_cohort
        left join cohort_build on sub_cohort.pat_key = cohort_build.pat_key
            and sub_cohort.fiscal_year = cohort_build.fiscal_year
    --end region
)
select * from cohort_output
