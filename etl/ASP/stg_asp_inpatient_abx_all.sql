-- purpose: get all abx medication usage for relevant meds for DOT / LOT
-- granularity: one row per medication administration

-- get component medication info of each medication mixture order
with mixtures as (
    select
        rx_med_mix_compon.medication_id,
        'Anti-Infective Agents' as therapeutic_class

    from
        {{ source('clarity_ods', 'rx_med_mix_compon') }}      as rx_med_mix_compon
        inner join {{ source('clarity_ods', 'clarity_medication') }}     as component_medication
            on rx_med_mix_compon.drug_id = component_medication.medication_id

    where
        component_medication.thera_class_c = 1001 --'Anti-Infective Agents'
    group by
        rx_med_mix_compon.medication_id
),

abx_all as (
    select
        stg_asp_abx_all.visit_key,
        stg_asp_abx_all.pat_key,
        stg_asp_abx_all.med_ord_key,
        stg_asp_abx_all.medication_order_dept_key          as order_department,
        stg_asp_abx_all.ordering_department                as order_department_name,
        date(stg_asp_abx_all.administration_date)          as abx_admin_date,
        stg_asp_abx_all.administration_date                as abx_admin_datetime,
        stg_asp_abx_all.medication_administration_dept_key as admin_department,
        stg_asp_abx_all.administration_department          as admin_department_name,
        -- get route group from lookup: use order route, fall back to medication route
        -- if no match on order route or medication route, check for "inj" in medication order name
        coalesce(
            lookup_asp_inpatient_order_route.abx_route,
            lookup_asp_inpatient_medication_route.abx_route,
            case
                when regexp_like(stg_asp_abx_all.medication_order_name, '\binj', 'i')
                then 'IV'
            end
        ) as abx_route,
        stg_asp_abx_all.abx_name,
        stg_asp_abx_all.generic_medication_name,
        stg_asp_abx_all.medication_order_name,
        max(case
                when mixtures.medication_id is not null
                then 1 else 0
        end) as mixture_ind,
        stg_asp_abx_all.drug_category,
        stg_asp_abx_all.drug_class,
        stg_asp_abx_all.drug_subclass,
        stg_asp_abx_all.cdc_drug_ind,
        stg_asp_abx_all.last_line_ind,
        stg_asp_abx_all.targeted_ind,
        stg_asp_abx_all.rule_out_48_hour_ind

    from
        {{ref('stg_asp_abx_all') }}             as stg_asp_abx_all
        inner join {{ source('clarity_ods', 'clarity_medication') }}   as clarity_medication
            on stg_asp_abx_all.medication_id = clarity_medication.medication_id
        left join mixtures
            on stg_asp_abx_all.medication_id = mixtures.medication_id
        left join {{ref('lookup_asp_inpatient_abx_route') }}   as lookup_asp_inpatient_order_route
            on stg_asp_abx_all.order_route = lookup_asp_inpatient_order_route.order_route
        left join {{ref('lookup_asp_inpatient_abx_route') }}   as lookup_asp_inpatient_medication_route
            on clarity_medication.route = lookup_asp_inpatient_medication_route.route
        left join {{ref('lookup_asp_inpatient_admin_route') }} as lookup_asp_inpatient_admin_route
            on stg_asp_abx_all.admin_route = lookup_asp_inpatient_admin_route.admin_route

    where
        -- exclude medications requested by ASP on case-by-case basis
        stg_asp_abx_all.medication_id not in (
            200200531, -- methotrexate or mercaptopurine (anti-metabolites)
            200200678, -- methotrexate sodium 10 mg or tabs onco custom
            200200677, -- methotrexate 2.5 mg or tabs onco custom
            34322,     -- peginterferon alfa-2b For inj kit 50 mcg/0.5ml
            200202295, -- inv-cyclodextrin 200 mg/ml it injection custom
            200100672, -- cefazolin injection ophthalmic mixture intra-op only
            200100658, -- ceftazidime desensitization injection 0.1 mg/ml custom
            200100659, -- ceftazidime desensitization injection 1 mg/ml custom
            200100660, -- ceftazidime desensitization injection 100 mg/ml custom
            200100929, -- inv-isavuconazonium sulfate (17ll016) infusion custom
            200202823  -- inv-isavuconazonium sulfate (17ll016) 74.5 mg or caps custom
        )
        -- force exclude abx with topical order route
        -- med route fallback could include topical order route meds otherwise
        and stg_asp_abx_all.order_route != 'Topical'
        and (
            -- initial partial clean-up to decrease lookup
            clarity_medication.route in (
                'Enteral',
                'Inhalation',
                'Injection',
                'Intramuscular',
                'Intrathecal',
                'Intravenous',
                'Nebulizer',
                'Oral',
                'Subcutaneous',
                '-'
            )
            or clarity_medication.route is null
        )
        -- exclude admin routes that aren't tracked by NHSN
        and lookup_asp_inpatient_admin_route.admin_route is null
        -- include anti-infectives and mixtures with an anti-infective component
        and (
            stg_asp_abx_all.therapeutic_class = 'Anti-Infective Agents'
            or mixtures.therapeutic_class = 'Anti-Infective Agents'
        )
        -- include medication administrations that should count for metrics
        and stg_asp_abx_all.administration_type_id in (
            1,        -- given
            6,        -- new bag
            7,        -- restarted
            9,        -- rate change
            12,       -- bolus
            13,       -- push
            102,      -- pt/caregiver admin - non high alert
            103,      -- pt/caregiver admin - high alert
            105,      -- given by other
            106,      -- new syringe
            112,      -- iv started
            115,      -- iv restarted
            116,      -- divided dose
            117,      -- started by other
            119,      -- neb restarted
            122.0020, -- performed
            127       -- bolus from bag/bottle/syringe
        )
        and stg_asp_abx_all.administration_date > '2013-07-01'
        -- exclude "lock" to exclude antibiotic locks
        and not regexp_like(stg_asp_abx_all.generic_medication_name, '\block', 'i')
        and not regexp_like(stg_asp_abx_all.medication_order_name, '\block', 'i')

    group by
        stg_asp_abx_all.visit_key,
        stg_asp_abx_all.pat_key,
        stg_asp_abx_all.med_ord_key,
        stg_asp_abx_all.medication_order_dept_key,
        stg_asp_abx_all.ordering_department,
        stg_asp_abx_all.administration_date,
        stg_asp_abx_all.medication_administration_dept_key,
        stg_asp_abx_all.administration_department,
        stg_asp_abx_all.order_route,
        clarity_medication.route,
        lookup_asp_inpatient_order_route.abx_route,
        lookup_asp_inpatient_medication_route.abx_route,
        stg_asp_abx_all.abx_name,
        stg_asp_abx_all.generic_medication_name,
        stg_asp_abx_all.medication_order_name,
        stg_asp_abx_all.drug_category,
        stg_asp_abx_all.drug_class,
        stg_asp_abx_all.drug_subclass,
        stg_asp_abx_all.cdc_drug_ind,
        stg_asp_abx_all.last_line_ind,
        stg_asp_abx_all.targeted_ind,
        stg_asp_abx_all.rule_out_48_hour_ind
)

select
    visit_key,
    pat_key,
    med_ord_key,
    order_department,
    order_department_name,
    abx_admin_date,
    abx_admin_datetime,
    admin_department,
    admin_department_name,
    abx_route,
    abx_name,
    generic_medication_name,
    medication_order_name,
    mixture_ind,
    drug_category,
    drug_class,
    drug_subclass,
    cdc_drug_ind,
    last_line_ind,
    targeted_ind,
    rule_out_48_hour_ind

from abx_all
