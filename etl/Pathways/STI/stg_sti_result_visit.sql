select
    procedure_order_result_clinical.visit_key,
    procedure_order_result_clinical.mrn,
    procedure_order_result_clinical.department_name,
    procedure_order_result_clinical.encounter_date,
    --multiple tests can be done in a single day, encounter
    date(procedure_order_result_clinical.result_date) as result_day,
    (case
        when
            regexp_like(
                lower(procedure_order_result_clinical.result_component_name), 'chlamydia|trachomatis'
            )
            then 'CHLAMYDIA'
        when regexp_like(lower(procedure_order_result_clinical.result_component_name), 'trichomonas| trich ')
            then 'TRICHOMONAS'
        when regexp_like(lower(procedure_order_result_clinical.result_component_name), 'hepatitis b|hep b')
            then 'HEP_B'
        when regexp_like(lower(procedure_order_result_clinical.result_component_name), 'hepatitis c|hep c')
            then 'HEP_C'
        when regexp_like(lower(procedure_order_result_clinical.result_component_name), 'gonor')
            then 'GONORRHEA'
        when
            regexp_like(
                lower(procedure_order_result_clinical.result_component_name),
                'syphilis|syphillis'
            )
            then 'SYPHILIS'
        when regexp_like(lower(procedure_order_result_clinical.result_component_name), 'hiv')
            then 'HIV'
        when regexp_like(lower(procedure_order_result_clinical.result_component_name), 'hsv|herpes')
            then 'HSV'
        else null
    end) as sti_test_type,
    max(case
        when
            regexp_like(lower(procedure_order_result_clinical.result_value), 'positive|reactive|detected|isolated')
            and not regexp_like(
                lower(procedure_order_result_clinical.result_value), 'non-reactive|not detected|nonreactive'
            )
            then 1
        else 0
    end) as sti_test_positive_ind,
    max(case
        when regexp_like(lower(procedure_order_result_clinical.order_specimen_source), 'throat|pharynx|tonsil')
            then 1
        else 0
    end) as throat_sample_ind,
    max(case
        when regexp_like(lower(procedure_order_result_clinical.order_specimen_source), 'rectal')
            then 1
        else 0
    end) as rectal_sample_ind,
    max(case
        when regexp_like(lower(procedure_order_result_clinical.order_specimen_source), 'urine')
            then 1
        else 0
    end) as urine_sample_ind,
    max(case
        when regexp_like(lower(procedure_order_result_clinical.order_specimen_source), 'vagina|cervix')
            --vulva, labia are also options
            then 1
        else 0
    end) as vaginal_sample_ind,
    max(case
        when regexp_like(lower(procedure_order_result_clinical.order_specimen_source), 'blood|plasma')
            then 1
        else 0
    end) as blood_sample_ind

from {{ ref('procedure_order_result_clinical') }} as procedure_order_result_clinical

where
    procedure_order_result_clinical.encounter_date >= '2021-06-01'
    and lower(procedure_order_result_clinical.procedure_group_name) = 'lab'
    and regexp_like(lower(procedure_order_result_clinical.result_component_name),
        'chlamydia|trachomatis|trichomonas| trich |hepatitis|^hep |gonor|syphilis|syphillis|hiv|hsv|herpes'
    )
    and procedure_order_result_clinical.result_component_name != 'ARCHIVAL CASE ADDENDUM'
    and procedure_order_result_clinical.result_status not in ('Incomplete', 'NOT APPLICABLE')
    and sti_test_type is not null --noqa: L028

group by
    procedure_order_result_clinical.visit_key,
    procedure_order_result_clinical.mrn,
    procedure_order_result_clinical.department_name,
    procedure_order_result_clinical.encounter_date,
    result_day, --noqa: L028
    sti_test_type --noqa: L028
