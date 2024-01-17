--5. patients that see these providers in multi-d clinic during the past 3 years
-- if a patient ever saw andy(bauer, andrew j) for any reason and then saw ted/kelly/stephanie,
--then we would consider them 'developmental therapeutics patient'
with
enc_other_prov_2_tmp1 as (--region:
    select distinct
        stg_encounter.pat_key,
        stg_encounter.mrn,
        stg_encounter.encounter_date,
        stg_encounter.visit_key,
        stg_encounter.csn,
        provider.prov_id as provider_id
    from {{ ref('stg_frontier_thyroid_cohort_base_tmp') }} as cohort_base_tmp
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on cohort_base_tmp.pat_key = stg_encounter.pat_key
    inner join {{ ref('stg_frontier_thyroid_dx_hx') }} as dx_hx
        on cohort_base_tmp.pat_key = dx_hx.pat_key
        and stg_encounter.encounter_date >= dx_hx.thyroid_center_dx_date
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    inner join {{ ref('stg_encounter') }} as stg_encounter_andy
        on stg_encounter.pat_key = stg_encounter_andy.pat_key
    inner join {{source('cdw','provider')}} as provider_andy
        on provider_andy.prov_key = stg_encounter_andy.prov_key
        and provider_andy.prov_id = '10352' -- "bauer, andrew j."
    where
        provider.prov_id in ('9810', --laetsch, theodore
                                          '25535', --meyers, kelly
                                          '2006349', --"o'reilly, stephanie  h"
                                          '1000155' -- onc off day md, provider
                                          )
        and year(add_months(stg_encounter.encounter_date, 6)) >= 2020
        and stg_encounter.encounter_date >= stg_encounter_andy.encounter_date
        and stg_encounter.encounter_date < '2022-06-01' -- starting 2022-6-1 new visits will be used
        and stg_encounter.visit_type_id != '0'
),
enc_other_prov_2_tmp2 as (
    select
        enc.pat_key,
        enc.mrn,
        enc.encounter_date,
        enc.visit_key,
        pat_enc.attnd_prov_id
    from enc_other_prov_2_tmp1 as enc
    inner join {{source('clarity_ods', 'pat_enc')}} as pat_enc
        on pat_enc.pat_enc_csn_id = enc.csn
    where enc.provider_id = '1000155'  -- onc off day md, provider
        and pat_enc.attnd_prov_id in ('9810', --laetsch, theodore
                                      '25535', --meyers, kelly
                                      '2006349' --"o'reilly, stephanie  h"
                                      )
),
enc_other_prov_2_tmp3 as (
    select distinct
        enc.pat_key,
        enc.mrn,
        enc.encounter_date,
        enc.visit_key,
        final_provider.prov_id
    from enc_other_prov_2_tmp1 as enc
    inner join {{source('cdw','note_info')}} as note_info
        on enc.visit_key = note_info.visit_key
    inner join {{source('cdw','dim_ip_note_type')}} as dim_ip_note_type
        on dim_ip_note_type.dim_ip_note_type_key = note_info.dim_ip_note_type_key
    inner join {{source('cdw','employee')}} as final_employee
        on final_employee.emp_key = note_info.curr_auth_emp_key
    inner join {{source('cdw','provider')}} as final_provider
        on final_provider.prov_key = final_employee.prov_key
    where
        enc.provider_id = '1000155' -- onc off day md, provider
        and final_provider.prov_id in ('9810', --laetsch, theodore
                                        '25535', --meyers, kelly
                                        '2006349' --"o'reilly, stephanie  h"
                                        )
        and dim_ip_note_type.ip_note_type_id in ('1', '4') -- progress note, h&p
),
enc_other_prov_2_tmp as (
    select t1.pat_key,
        t1.mrn,
        t1.encounter_date,
        t1.visit_key,
        case when t1.provider_id = '1000155' -- onc off day md, provider
            then coalesce(t2.attnd_prov_id, t3.prov_id)
            else t1.provider_id end as provider_id,
        case when t1.provider_id = '1000155' -- onc off day md, provider
            then provider2.full_nm
            else provider.full_nm end as provider_name
    from enc_other_prov_2_tmp1 as t1
    left join enc_other_prov_2_tmp2 as t2
        on t1.visit_key = t2.visit_key
    left join enc_other_prov_2_tmp3 as t3
        on t1.visit_key = t3.visit_key
    left join {{source('cdw','provider')}} as provider
        on t1.provider_id = provider.prov_id
    left join {{source('cdw','provider')}} as provider2
        on coalesce(t2.attnd_prov_id, t3.prov_id) = provider2.prov_id
    where t1.provider_id != '1000155'
        or coalesce(t2.attnd_prov_id, t3.prov_id) is not null
)
--include these patients visits with andy after they seen ted/kelly/stephanie
    select
        stg_encounter.pat_key,
        stg_encounter.mrn,
        stg_encounter.encounter_date,
        stg_encounter.visit_key,
        provider.prov_id as provider_id,
        initcap(provider.full_nm) as provider_name
    from enc_other_prov_2_tmp
    inner join {{ ref('stg_encounter') }} as stg_encounter
        on enc_other_prov_2_tmp.pat_key = stg_encounter.pat_key
        and stg_encounter.encounter_date >= enc_other_prov_2_tmp.encounter_date
        and stg_encounter.visit_type_id != '0'
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
        and provider.prov_id = '10352' -- "bauer, andrew j."
    where stg_encounter.encounter_date < '2022-06-01' -- starting 2022-6-1 new visits will be used
        or stg_encounter.appointment_made_date < '2022-06-01 00:00:00' -- starting 2022-6-1 new visits will be used
    union
    select
        pat_key,
        mrn,
        encounter_date,
        visit_key,
        provider_id,
        provider_name
    from enc_other_prov_2_tmp
    union
    select
        pat_key,
        mrn,
        encounter_date,
        visit_key,
        provider.prov_id as provider_id,
        initcap(provider.full_nm) as provider_name
    from {{ ref('stg_encounter') }} as stg_encounter
    inner join {{source('cdw','provider')}} as provider
        on provider.prov_key = stg_encounter.prov_key
    where visit_type_id in ('3487', -- new thyroid therapeutics
                            '3485') -- follow up thyroid therapeutics
