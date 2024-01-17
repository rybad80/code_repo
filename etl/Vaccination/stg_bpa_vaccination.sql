select
    stg_encounter.mrn,
    stg_encounter.patient_key,
    stg_encounter.pat_id,
    stg_encounter.visit_key,
    stg_encounter.patient_name,
    smart_data_element_all.concept_id,
    smart_data_element_all.encounter_date,
    date(year(smart_data_element_all.encounter_date) || '-'
        || month(smart_data_element_all.encounter_date) || '-1') as care_asst_month_year_documented,
    row_number() over (
        partition by
            stg_encounter.patient_key
        order by
            (year(smart_data_element_all.encounter_date) || '-'
                || month(smart_data_element_all.encounter_date) || '-1') desc) as historical_seq,
    max(case
        when month(smart_data_element_all.encounter_date) in ('08', '09', '10', '11', '12')
            then year(smart_data_element_all.encounter_date + cast('1 year' as interval))
        when month(smart_data_element_all.encounter_date) in ('01', '02', '03')
            then year(smart_data_element_all.encounter_date)
        else null
    end) as care_asst_doc_flu_yr,
    case
        when smart_data_element_all.element_value = 1
            then 'NOINVENTORY'
        when smart_data_element_all.element_value = 2
            then 'REFUSED'
        when smart_data_element_all.element_value = 3
            then 'PCP'
        when smart_data_element_all.element_value = 4
            then 'ALREADYGIVEN'
    end as element_value,
    1 as influenza_vaccine_ind
from
	{{ref('smart_data_element_all') }} as smart_data_element_all
	inner join {{ref('stg_encounter') }} as stg_encounter
        on stg_encounter.visit_key = smart_data_element_all.visit_key
where
    --new SDE linked to bpa record SPECIALTY FLU ACK [CHOP#9091]
    smart_data_element_all.concept_id = 'CHOP#9091'
    --only looking at PCP and ALREADYGIVEN values
    and smart_data_element_all.element_value in ('3', '4')
group by
	stg_encounter.mrn,
	stg_encounter.patient_key,
    stg_encounter.pat_id,
	stg_encounter.visit_key,
	stg_encounter.patient_name,
	smart_data_element_all.concept_id,
	smart_data_element_all.element_value,
	smart_data_element_all.encounter_date
