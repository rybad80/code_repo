with
    orders as (
        select
            procedure_order_clinical.visit_key,
            procedure_order_clinical.pat_key,
            'orders' as source,
            date(placed_date) as pft_date,
            procedure_order_clinical.procedure_name as procedure_name,
            cast(procedure_order_clinical.procedure_order_id as int) as id,
            'procedure_order_id' as id_source
        from
            {{ ref('ctis_registry') }} as ctis_registry
            inner join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
                on procedure_order_clinical.pat_key = ctis_registry.pat_key
        where
            lower(procedure_order_clinical.order_status) = 'completed'
            and lower(procedure_order_clinical.procedure_order_type) != 'parent order'
            and procedure_order_clinical.procedure_id in (
                    86565,	-- ip pft infant resp resistance single
                    86563,	-- ip pft infant resp compl single
                    86560,	-- ip pft infant lung volumes
                    86561,	-- ip pft infant lung pre/post
                    93717,	-- infant spirometry, single through 2 years
                    93718,	-- infant spirometry, pre/post bronchodilator
                    93719	-- infant lung volumes through 2 years of age
            )
    ),
    results as (
        select
            procedure_order_result_clinical.visit_key,
            procedure_order_result_clinical.pat_key,
            'order result' as source,
            procedure_order_result_clinical.result_date as pft_date,
            procedure_order_result_clinical.procedure_name,
            cast(procedure_order_result_clinical.procedure_order_id as int) as id,
            'procedure_order_id' as id_source
        from
            {{ ref('ctis_registry') }} as ctis_registry
            inner join {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical
                on procedure_order_result_clinical.pat_key = ctis_registry.pat_key
        where
            procedure_order_result_clinical.result_value_numeric is not null
            and (
                regexp_like(procedure_order_result_clinical.procedure_name, 'lung vol|pft|spiro', 'i')
                or procedure_order_result_clinical.procedure_id in (
                    86578, --'single spirometry'
                    7030, --'spirometry'
                    86577--'pre/post spirometry'
                )
            )
        group by
            procedure_order_result_clinical.visit_key,
            procedure_order_result_clinical.pat_key,
            procedure_order_result_clinical.result_date,
            procedure_order_result_clinical.procedure_name,
            procedure_order_result_clinical.procedure_order_id
    ),
    media as (
        select distinct
            document_info.visit_key,
            document_info.pat_key,
            'media' as source,
            to_date(
                regexp_extract(document_info.doc_desc, '\d{1,2}[/\.]\d{1,2}[/\.]\d{4}'), 'MM/DD/YYYY'
            ) as pft_date,
            regexp_replace(document_info.doc_desc, '(^[^,]+, )|(, ([\d/]+|CHOP[ ]*$))', '') as procedure_name,
            document_info.doc_info_id as id,
            'doc_info_id' as id_source
        from
            {{ ref('ctis_registry') }} as ctis_registry
            inner join {{ source('cdw', 'document_info') }} as document_info
                on document_info.pat_key = ctis_registry.pat_key
            inner join {{ source('cdw', 'cdw_dictionary') }} as document_info_type
                on document_info_type.dict_key = document_info.dict_doc_info_type_key
            left join results
                on results.pat_key = document_info.pat_key
                and (results.pft_date = to_date(regexp_extract(document_info.doc_desc,
                '\d{1,2}[/\.]\d{1,2}[/\.]\d{4}'), 'MM/DD/YYYY'))
            left join orders
                on orders.pat_key = document_info.pat_key
                and (orders.pft_date = to_date(regexp_extract(document_info.doc_desc,
                '\d{1,2}[/\.]\d{1,2}[/\.]\d{4}'), 'MM/DD/YYYY'))
        where
            --'consent to operation, diagnostic procedure, medical treatment & blood transfusion'
            document_info_type.src_id != 100017
            -- want to drop this file if it exists in the other 2 queries
            and (results.pat_key is null and orders.pat_key is null)
            and lower(document_info.doc_desc) like '%infant%'
            and lower(document_info.doc_desc) not like '%consent%'
            and lower(document_info.doc_desc) not like '%follow_up%'
            and (
                lower(document_info.doc_desc) like '%lung vol%'
                 or lower(document_info.doc_desc) like '%pft%'
                 or lower(document_info.doc_desc) like '%spiro'
            )
    ),
    all_pft as (
        select * from results
        union
        select * from orders
        union
        select * from media
    )
select
    {{
        dbt_utils.surrogate_key([
            'all_pft.source',
            'all_pft.id'
        ])
    }} as pft_key,
    stg_patient.mrn,
    stg_patient.patient_name,
    stg_patient.sex,
    coalesce(
            stg_encounter.age_years,
            (date(all_pft.pft_date) - date(stg_patient.dob)) / 365.25
        ) as age_years,
    stg_patient.dob,
    all_pft.pft_date,
    all_pft.procedure_name,
    coalesce(stg_department_all.specialty_name, 'INPATIENT') as specialty,
    stg_encounter.encounter_type,
    stg_encounter.encounter_type_id,
    all_pft.id_source,
    all_pft.id as test_id,
    stg_encounter.department_name,
    initcap(provider.full_nm) as provider_name,
    provider.prov_id as provider_id,
    year(all_pft.pft_date) as calendar_year,
    year(add_months(all_pft.pft_date, 6)) as fiscal_year,
    stg_encounter.visit_key,
    stg_patient.pat_key,
    stg_encounter.dept_key,
    stg_encounter.prov_key
from
    all_pft
    inner join {{ ref('stg_patient') }} as stg_patient
        on stg_patient.pat_key = all_pft.pat_key
    left join {{ ref('stg_encounter') }} as stg_encounter
        on stg_encounter.visit_key = all_pft.visit_key
    left join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    left join {{ ref('stg_department_all') }} as stg_department_all
        on stg_department_all.dept_key = stg_encounter.dept_key
