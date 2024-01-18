{{ config(materialized='table', dist='pat_key') }}

with chemo_list as (
    select
        med_chemo.med_key
    from
        {{source('cdw', 'medication')}} as med_chemo
    where
        med_chemo.med_nm like 'AFINITOR%'
        or med_chemo.med_nm like 'ALDESLEUKIN%'
        or med_chemo.med_nm like 'ALECTINIB%'
        or med_chemo.med_nm like 'ALEMTUZUMAB%'
        or med_chemo.med_nm like 'ARSENIC%'
        or med_chemo.med_nm like 'ASPARAGINASE%'
        or med_chemo.med_nm like 'AVASTIN%'
        or med_chemo.med_nm like 'BEVACIZUMAB%'
        or med_chemo.med_nm like 'BLEOMYCIN%'
        or med_chemo.med_nm like 'BLINATUMOMAB%'
        or med_chemo.med_nm like 'BORTEZOMIB%'
        or med_chemo.med_nm like 'BRENTUXIMAB%'
        or med_chemo.med_nm like 'CAMPTOSAR%'
        or med_chemo.med_nm like 'CAPECITABINE%'
        or med_chemo.med_nm like 'CARBOPLATIN%'
        or med_chemo.med_nm like 'CISPLATIN%'
        or med_chemo.med_nm like 'CLOFARABINE%'
        or med_chemo.med_nm like 'CRIZOTINIB%'
        or med_chemo.med_nm like 'CYCLOPHOSPHAMIDE%'
        or med_chemo.med_nm like 'CYTARABINE%'
        or med_chemo.med_nm like 'DACTINOMYCIN%'
        or med_chemo.med_nm like 'DARATUMUMAB%'
        or med_chemo.med_nm like 'DASATINIB%'
        or med_chemo.med_nm like 'DAUNORUBICIN%'
        or med_chemo.med_nm like 'DINUTUXIMAB%'
        or med_chemo.med_nm like 'DOXORUBICIN%'
        or med_chemo.med_nm like 'ETOPOSIDE%'
        or med_chemo.med_nm like 'EVEROLIMUS%'
        or med_chemo.med_nm like 'FLOXURIDINE%'
        or med_chemo.med_nm like 'FLUDARABINE%'
        or med_chemo.med_nm like 'GLEEVEC%'
        or med_chemo.med_nm like 'HYCAMTIN%'
        or med_chemo.med_nm like 'HYDROXYUREA%'
        or med_chemo.med_nm like 'IFOSFAMIDE%'
        or med_chemo.med_nm like 'IMATINIB%'
        or med_chemo.med_nm like 'INTERFERON%'
        or med_chemo.med_nm like 'INTRON%'
        or med_chemo.med_nm like 'INV-BORTEZOMIB%'
        or med_chemo.med_nm like 'INV-BLINATUMOMAB%'
        or med_chemo.med_nm like 'INV-CRIZOTINIB%'
        or med_chemo.med_nm like 'INV-ERIBULIN%'
        or med_chemo.med_nm like 'INV-NELARABINE%'
        or med_chemo.med_nm like 'INV-SORAFENIB%'
        or med_chemo.med_nm like 'INV-TEMSIROLIMUS%'
        or med_chemo.med_nm like 'INV-%'
        or med_chemo.med_nm like 'IRINOTECAN%'
        or med_chemo.med_nm like 'JAKAFI%'
        or med_chemo.med_nm like 'LAPATINIB%'
        or med_chemo.med_nm like 'LETROZOLE%'
        or med_chemo.med_nm like 'LEUPROLIDE%'
        or med_chemo.med_nm like 'LOMUSTINE%'
        or med_chemo.med_nm like 'LUPRON%'
        or med_chemo.med_nm like 'MATULANE%'
        or med_chemo.med_nm like 'MEDROXYPROGESTERONE%'
        or med_chemo.med_nm like 'MEGACE%'
        or med_chemo.med_nm like 'MEGESTROL%'
        or med_chemo.med_nm like 'MERCAPTOPURINE%'
        or med_chemo.med_nm like 'METHOTREXATE%'
        or med_chemo.med_nm like 'METHOXSALEN%'
        or med_chemo.med_nm like 'MITOXANTRONE%'
        or med_chemo.med_nm like 'NELARABINE%'
        or med_chemo.med_nm like 'NEXAVAR%'
        or med_chemo.med_nm like 'NILOTINIB%'
        or med_chemo.med_nm like 'NIVOLUMAB%'
        or med_chemo.med_nm like 'OXALIPLATIN%'
        or med_chemo.med_nm like 'PAZOPANIB%'
        or med_chemo.med_nm like 'PACLITAXEL%'
        or med_chemo.med_nm like 'PEGASPARGASE%'
        or med_chemo.med_nm like 'PEMBROLIZUMAB%'
        or med_chemo.med_nm like 'PONATINIB%'
        or med_chemo.med_nm like 'PROCARBAZINE%'
        or med_chemo.med_nm like 'PURIXAN%'
        or med_chemo.med_nm like 'RASBURICASE%'
        or med_chemo.med_nm like 'RITUXIMAB%'
        or med_chemo.med_nm like 'RUXOLITINIB%'
        or med_chemo.med_nm like 'SORAFENIB%'
        or med_chemo.med_nm like 'SPRYCEL%'
        or med_chemo.med_nm like 'TABLOID%'
        or med_chemo.med_nm like 'TASIGNA%'
        or med_chemo.med_nm like 'TEMODAR%'
        or med_chemo.med_nm like 'TEMOZOLOMIDE%'
        or med_chemo.med_nm like 'THIOGUANINE%'
        or med_chemo.med_nm like 'TOPOTECAN%'
        or med_chemo.med_nm like 'TRABECTEDIN%'
        or med_chemo.med_nm like 'TRETINOIN%'
        or med_chemo.med_nm like 'TREXALL%'
        or med_chemo.med_nm like 'TYKERB%'
        or med_chemo.med_nm like 'VINBLASTINE%'
        or med_chemo.med_nm like 'VINCRISTINE%'
        or med_chemo.med_nm like 'VISMODEGIB%'
        or med_chemo.med_nm like 'VORINOSTAT%'
        or med_chemo.med_nm like 'VOTRIENT%'
),
stg_med_admin as (
    select
        ma.med_ord_key,
        date_trunc('month', ma.action_dt) as start_date,
        date_trunc('month', ma.action_dt) + cast('2 months' as interval) as end_date
    from
        {{source('cdw','medication_administration')}} as ma
    inner join {{source('cdw','cdw_dictionary')}} as drug_admin
        on ma.dict_rslt_key = drug_admin.dict_key
    where
        drug_admin.src_id in (105, 102, 122.0020, 6, 103, 1, 106, 112, 117)
        and ma.action_dt >= '2020-01-01'
        and ma.action_dt < current_date
)

select
    medication_order.pat_key,
    cohort.outbreak_type,
    'Chemo' as reason,
    medication_administration.start_date,
    medication_administration.end_date,
    'Chemo' as reason_detail
from
    {{source('cdw', 'treatment_plan_order')}} as treatment_plan_order
    inner join {{source('cdw', 'medication_order')}} as medication_order
        on treatment_plan_order.ord_key = medication_order.med_ord_key
    inner join {{source('cdw', 'medication')}} as med_chemo
        on medication_order.med_key = med_chemo.med_key
    inner join {{source('cdw', 'cdw_dictionary')}} as thera
        on med_chemo.dict_thera_class_key = thera.dict_key
    inner join {{source('cdw', 'cdw_dictionary')}} as antidote
        on med_chemo.dict_pharm_subclass_key = antidote.dict_key
    inner join {{source('cdw', 'cdw_dictionary')}} as med_hist
        on medication_order.dict_ord_class_key = med_hist.dict_key
    inner join stg_med_admin as medication_administration
        on medication_order.med_ord_key = medication_administration.med_ord_key
    inner join {{ ref('stg_outbreak_pui_immunocompromised_cohort') }} as cohort
        on medication_order.pat_key = cohort.pat_key
    left join chemo_list
        on chemo_list.med_key = med_chemo.med_key
where
    (
        (
            thera.dict_nm = 'Alntineoplastic Agents'
            and antidote.dict_nm != 'Chemotherapy Rescue/Antidote Agents'
        )
        or chemo_list.med_key is not null
    )
    and med_hist.src_id != 3
    and medication_order.pat_key != 0
group by
    medication_order.pat_key,
    cohort.outbreak_type,
    medication_administration.start_date,
    medication_administration.end_date
