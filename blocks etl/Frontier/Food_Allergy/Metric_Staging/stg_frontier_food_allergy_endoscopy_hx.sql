with
cohort_mrn_list as (--region:
    select
        frontier_food_allergy_encounter_cohort.mrn,
        max(case when frontier_food_allergy_encounter_cohort.eoe_ind = '1' then 1 else 0 end) as eoe_ind,
        max(case when frontier_food_allergy_encounter_cohort.ec_ind = '1' then 1 else 0 end) as ec_ind,
        max(case when frontier_food_allergy_encounter_cohort.eg_ind = '1' then 1 else 0 end) as eg_ind
    from {{ ref('frontier_food_allergy_encounter_cohort')}} as frontier_food_allergy_encounter_cohort
    group by frontier_food_allergy_encounter_cohort.mrn
    --end region
),
endoscopy_hx as (--region:
    select
        cohort_mrn_list.mrn,
        procedure_billing.visit_key,
        procedure_billing.cpt_code,
        procedure_billing.procedure_name,
        procedure_billing.source_summary,
        procedure_billing.tx_id,
        procedure_billing.service_date as encounter_date,
        max(--UPPER GI NDSC DX W/WO COLLECTION SPECIMEN
            case when procedure_billing.cpt_code = '43235'
            then 1 else 0 end) as upper_gi_edoscopy_dx_ind,
        max(--UPPER NDSC BIOPSY SINGLE/MULTIPLE --this is the main one FA uses
            case when lower(procedure_billing.cpt_code) = '43239'
            then 1 else 0 end) as upper_edoscopy_biopsy_ind,
        max(--COLONOSCOPY
            case when lower(procedure_name) like '%colonoscopy%'
            and lower(procedure_billing.cpt_code) not like '%a%'
            then 1 else 0 end) as colonoscopy_ind,
        max(--imaging
            case when
            procedure_billing.cpt_code in ('74240'  --FL UPPER GI (TO LIGAMENT OF TREITZ
                                                    --RADEX GI TRACT UPPER W/WO DELAYED FILMS W/O KUB
                                                    --FL X-RAY UGI+SM INTES,MULT
                                                    --FL X-RAY EXAM UGI, W KUB
                                            )
            then 1 else 0 end) as dx_imaging_ind,
        max(--ESOPHGL BALO DISTENSION PROVOCATIO
            case when procedure_billing.cpt_code = '91040'
            then 1 else 0 end) as esophgl_balloon_study_ind,
        max(--ESOPHAGOGASTRODUODENSCOPY (EGD
            case when
            lower(procedure_name) like '%esophagogastroduodenscopy%'
            or lower(procedure_name) like '%egd%'
            then 1 else 0 end) as egd_ind,
        year(encounter_date) as enc_year,
        month(encounter_date) as enc_month,
        year(add_months(encounter_date, 6)) as fiscal_year

    from cohort_mrn_list
        inner join {{ ref('procedure_billing')}} as procedure_billing
            on cohort_mrn_list.mrn = procedure_billing.mrn
    where
        cohort_mrn_list.eoe_ind
        + cohort_mrn_list.ec_ind
        + cohort_mrn_list.eg_ind
        > 0
    group by
        cohort_mrn_list.mrn,
        procedure_billing.visit_key,
        procedure_billing.cpt_code,
        procedure_billing.procedure_name,
        procedure_billing.source_summary,
        procedure_billing.tx_id,
        procedure_billing.service_date
    having
        upper_gi_edoscopy_dx_ind
        + upper_edoscopy_biopsy_ind
        + colonoscopy_ind
        + dx_imaging_ind
        + esophgl_balloon_study_ind
        + egd_ind > 0
        and encounter_date is not null
    --end region
)
select * from endoscopy_hx
