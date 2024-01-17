with allergies as (
    select
        cohort.visit_key,
        max(
            case when lower(allergen.algn_nm) like '%penicillin%'
                then 1 else 0 end
        ) as pcn_allergy_ind,
        max(
            case when lower(allergen.algn_nm) like '%cephalosporin%'
                then 1 else 0 end
        ) as ceph_allergy_ind,
        max(
            case when (lower(allergen.algn_nm) like '%cephalexin%' or lower(allergen.algn_nm) like '%cefazolin%')
                then 1 else 0 end
        ) as cephalexin_allergy_ind

    from
        {{ref('stg_ed_encounter_cohort_all')}} as cohort
        inner join {{ source('cdw', 'patient_allergy') }} as patient_allergy
            on cohort.pat_key = patient_allergy.pat_key
            and patient_allergy.noted_dt < cohort.arrive_ed_dt --allergy known before encounter
        inner join  {{ source('cdw', 'allergen') }} as allergen
            on patient_allergy.algn_key = allergen.algn_key
        inner join {{ source('cdw', 'cdw_dictionary') }} as stat_dict
            on patient_allergy.dict_stat_key = stat_dict.dict_key
            and (
                stat_dict.src_id = 1 --currently active
                or (
                    stat_dict.src_id = 2 --deleted after encounter
                    and patient_allergy.entered_dt > cohort.arrive_ed_dt
                )
            )
    group by
        cohort.visit_key
),
infections as (
    select
        cohort.visit_key,
        max(
            case
                when hsp_inf_id = 10 --MRSA
            then 1 end
        ) as mrsa_hx_ind
    from
        {{ref('stg_ed_encounter_cohort_all')}} as cohort
        inner join {{ source('cdw', 'hospital_infection_patient_info') }} as hospital_infection_patient_info
            on cohort.pat_key = hospital_infection_patient_info.pat_key
            and lower(hospital_infection_patient_info.inf_cmt) not like '%error%'
            and (
                --still present
                hospital_infection_patient_info.inf_rec_resolved_dt is null
                --cleared after encounter
                or hospital_infection_patient_info.inf_rec_resolved_dt > cohort.arrive_ed_dt
            )
        inner join {{ source('cdw', 'dim_hospital_infection') }} as dim_hospital_infection
            on hospital_infection_patient_info.dim_hsp_inf_key = dim_hospital_infection.dim_hsp_inf_key
    group by
        cohort.visit_key
)
select
    cohort.visit_key,
    pcn_allergy_ind,
    ceph_allergy_ind,
    mrsa_hx_ind
from
    {{ref('stg_ed_encounter_cohort_all')}} as cohort
    left join allergies
        on cohort.visit_key = allergies.visit_key
    left join infections
        on cohort.visit_key = infections.visit_key
