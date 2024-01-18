-- RUNTIME_S   QH_ESTCOST  LOG_COST    QH_SNIPPETS QH_ESTMEM   N_CHAR
-- 00:00:03    15702       4.195954    12          0           3755

with cohort as (
select
    mrn,
    patient_name,
    pat_key
from {{ref('stg_usnews_pulm_asthma_base')}}
where primary_dx_ind = 1
group by
    mrn,
    patient_name,
    pat_key
),
/*
 * streamlined codes for CF, BPD/CLD, and ILD
 * cystic fibrosis patients: based on s_cdw_usnwr_pulm_enc logic using patient list
 * BPD/CLD patients: based on patient list and historical diagnosis of ('P27.1', 'P27.9', 'P28.89', 'J98.4')
 * ILD patients: based on patient list
 */
/* validated as of 2/20 - ILD has 2 less patients than original codes because of cur_rec_ind criterion */
/* needs to double check what cur_rec_ind is */
cf_cld_ild_pat as (
select
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    historic_dx.encounter_date,
    case
        when patient_list_info.pat_lst_info_id = 84278
            and (lower(historic_dx.diagnosis_name) like '%cystic fibrosis%'
                or lower(historic_dx.diagnosis_name) like '%cf%')
        then 'cystic fibrosis'
        when patient_list_info.pat_lst_info_id = 48445
            and lower(historic_dx.icd10_code) in ('p27.1', 'p27.9', 'p28.89', 'j98.4')
        then 'bpd/cld'
        when patient_list_info.pat_lst_info_id = 517142
        then 'ild'
        end as exc_cohort,
    case
        when exc_cohort = 'ild'
        then 0
        else 1
        end as date_cutoff_ind
from cohort
inner join {{source('cdw', 'patient_list')}} as patient_list
    on cohort.pat_key = patient_list.pat_key
inner join {{source('cdw', 'patient_list_info')}} as patient_list_info
    on patient_list.pat_lst_info_key = patient_list_info.pat_lst_info_key
inner join {{ref('diagnosis_encounter_all')}} as historic_dx -- get all historic dx this patient has
    on patient_list.pat_key = historic_dx.pat_key
where
    patient_list_info.pat_lst_info_id in (84278, -- cystic fibrosis
                                          48445, -- NeoCLD pt-All
                                          517142) -- ILD History: 2019-2021
    and patient_list.cur_rec_ind = 1
group by
    cohort.mrn,
    cohort.patient_name,
    cohort.pat_key,
    historic_dx.encounter_date,
    exc_cohort,
    date_cutoff_ind
)
select
    mrn,
    patient_name,
    pat_key,
    encounter_date as index_date,
    exc_cohort,
    date_cutoff_ind
from cf_cld_ild_pat
where exc_cohort is not null
