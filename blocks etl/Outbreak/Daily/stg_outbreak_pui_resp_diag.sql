{{ config(materialized='table', dist='pat_key') }}

with cohort as (
    select distinct
        stg_outbreak_covid_pui_cohort.pat_key,
        stg_outbreak_covid_pui_cohort.min_specimen_taken_date,
        'covid' as outbreak_type
    from
        {{ref('stg_outbreak_covid_pui_cohort')}} as stg_outbreak_covid_pui_cohort
        union
    select distinct
        stg_outbreak_flu_pui_cohort.pat_key,
        stg_outbreak_flu_pui_cohort.min_specimen_taken_date,
        stg_outbreak_flu_pui_cohort.test_type as outbreak_type
    from
    {{ref('stg_outbreak_flu_pui_cohort')}} as stg_outbreak_flu_pui_cohort
)

select
    procedure_order_clinical.pat_key,
    cohort.outbreak_type,
    min(case when lookup_outbreak_pui_labs.test_description = 'flu a rapid ag'
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a h1%'
            or lower(procedure_order_result_clinical.result_value) = 'detected'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a detected%'
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'flu a rapid ag'
                and (lower(procedure_order_result_clinical.result_value) like '%neg%'
                or lower(procedure_order_result_clinical.result_value)like ('%not detected%')
                ) then 2
  when lookup_outbreak_pui_labs.test_description = 'flu a rapid ag'
                and lower(procedure_order_result_clinical.result_value) like '%pending%'
                then 3
    when lookup_outbreak_pui_labs.test_description = 'flu a rapid ag'
                and (lower(procedure_order_result_clinical.result_value) like '%not done%'
                or lower(procedure_order_result_clinical.result_value) like '%not reported%')
                then 4
    else 4 end
        ) as resp_flua_ag,
    min(case when lookup_outbreak_pui_labs.test_description = 'flu b rapid ag'
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a h1%'
            or lower(procedure_order_result_clinical.result_value) = 'detected'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a detected%'
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'flu b rapid ag'
                and (lower(procedure_order_result_clinical.result_value) like '%neg%'
                or lower(procedure_order_result_clinical.result_value)like ('%not detected%')
                )
                then 2
  when lookup_outbreak_pui_labs.test_description = 'flu b rapid ag'
                and lower(procedure_order_result_clinical.result_value) like '%pending%'
                then 3
    when lookup_outbreak_pui_labs.test_description = 'flu b rapid ag'
                and (lower(procedure_order_result_clinical.result_value) like '%not done%'
                or lower(procedure_order_result_clinical.result_value) like '%not reported%')
                then 4
    else 4 end
    ) as resp_flub_ag,
    min(case when lookup_outbreak_pui_labs.test_description = 'flu a pcr'
            and (
            lower(procedure_order_result_clinical.result_value) like '%positive%'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a h1%'
            or lower(procedure_order_result_clinical.result_value) = 'detected'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a detected%'
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'flu a pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value)like ('%not detected%')
            ) then 2
  when lookup_outbreak_pui_labs.test_description = 'flu a pcr'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'flu a pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_flua_pcr,
    min(case when lookup_outbreak_pui_labs.test_description = 'flu b pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a h1%'
            or lower(procedure_order_result_clinical.result_value) = 'detected'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a detected%'
            )
            then 1
 when lookup_outbreak_pui_labs.test_description = 'flu b pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value)like ('%not detected%')
            )
            then 2
  when lookup_outbreak_pui_labs.test_description = 'flu b pcr'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'flu b pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
                or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_flub_pcr,
    min(case when lookup_outbreak_pui_labs.test_description = 'flu a rapid pcr'
            and (
            lower(procedure_order_result_clinical.result_value) like '%positive%'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a h1%'
            or lower(procedure_order_result_clinical.result_value) = 'detected'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a detected%'
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'flu a rapid pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value)like ('%not detected%')
            ) then 2
  when lookup_outbreak_pui_labs.test_description = 'flu a rapid pcr'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'flu a rapid pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_flua_rapid_pcr,
    min(case when lookup_outbreak_pui_labs.test_description = 'flu b rapid pcr'
            and (
            lower(procedure_order_result_clinical.result_value) like '%positive%'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a h1%'
            or lower(procedure_order_result_clinical.result_value) = 'detected'
            or lower(procedure_order_result_clinical.result_value) like '%influenza a detected%'
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'flu b rapid pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value)like ('%not detected%')
            ) then 2
  when lookup_outbreak_pui_labs.test_description = 'flu b rapid pcr'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'flu b rapid pcr'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_flub_rapid_pcr,
    min(case when lookup_outbreak_pui_labs.test_description = 'rsv'
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or (lower(procedure_order_result_clinical.result_value) like '%respiratory syncytial virus%'
                and lower(procedure_order_result_clinical.result_value) not like '%negative%')
            or (lower(procedure_order_result_clinical.result_value) like '%respiratory syncytial virus%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            )
            then 1
 when lookup_outbreak_pui_labs.test_description = 'rsv'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            )
            then 2
  when lookup_outbreak_pui_labs.test_description = 'rsv'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'rsv'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_rsv,
    min(case when lookup_outbreak_pui_labs.test_description = 'human metapneumovirus'
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
                or (lower(procedure_order_result_clinical.result_value) like '%metapneumovirus%'
                and lower(procedure_order_result_clinical.result_value) not like '%negative%')
                or (lower(procedure_order_result_clinical.result_value) like '%metapneumovirus%'
                    and lower(procedure_order_result_clinical.result_value) not like '%not%')
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'human metapneumovirus'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            ) then 2
  when lookup_outbreak_pui_labs.test_description = 'human metapneumovirus'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'human metapneumovirus'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_hm,
    min(case when lower(result_component_external_name) like '%parainfluenza%'  --region Parainfluenza 1-4
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or (lower(procedure_order_result_clinical.result_value) like '%parainfluenza%'
                and lower(procedure_order_result_clinical.result_value) not like '%negative%')
            or (lower(procedure_order_result_clinical.result_value) like '%parainfluenza%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
            and lower(procedure_order_result_clinical.result_value) not like '%not%')) then 1
 when lower(result_component_external_name) like '%parainfluenza%'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            ) then 2
  when lower(result_component_external_name) like '%parainfluenza%'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lower(result_component_external_name) like '%parainfluenza%'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end) as resp_pi,
    min(case when lower(result_component_external_name) like '%adenovirus%'  --region Adenovirus 
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
                or (lower(procedure_order_result_clinical.result_value) like '%adenovirus%'
                    and lower(procedure_order_result_clinical.result_value) not like '%negative%')
            or (lower(procedure_order_result_clinical.result_value) like '%adenovirus%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            ) then 1
 when lower(result_component_external_name) like '%adenovirus%'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            ) then 2
  when lower(result_component_external_name) like '%adenovirus%'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lower(result_component_external_name) like '%adenovirus%'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_adv,
    min(case
        when lower(result_component_external_name) like '%rhinovirus%'  --region Rhinovirus/Enterovirus
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or (lower(procedure_order_result_clinical.result_value) like '%rhinovirus%'
                and lower(procedure_order_result_clinical.result_value) not like '%negative%')
            or (lower(procedure_order_result_clinical.result_value) like '%rhinovirus%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            ) then 1
        when lower(result_component_external_name) like '%rhinovirus%'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            ) then 2
                when lower(result_component_external_name) like '%rhinovirus%'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
                                when lower(result_component_external_name) like '%rhinovirus%'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
                                else 4 end
    ) as resp_rhino,
    min(case when lookup_outbreak_pui_labs.test_description = 'coronavirus (oc43, 229e, hku1, nl63)'
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or (lower(procedure_order_result_clinical.result_value) like '%coronavirus (oc43, 229e, hku1, nl63)%'
                and lower(procedure_order_result_clinical.result_value) not like '%negative%')
            or (lower(procedure_order_result_clinical.result_value) like '%coronavirus (oc43, 229e, hku1, nl63)%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'coronavirus (oc43, 229e, hku1, nl63)'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            ) then 2
  when lookup_outbreak_pui_labs.test_description = 'coronavirus (oc43, 229e, hku1, nl63)'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'coronavirus (oc43, 229e, hku1, nl63)'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_cov,
    min(case when lookup_outbreak_pui_labs.test_description = 'm. pneumoniae'
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'm. pneumoniae'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            ) then 2
  when lookup_outbreak_pui_labs.test_description = 'm. pneumoniae'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'm. pneumoniae'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_mp,
    min(case when lookup_outbreak_pui_labs.test_description = 'c. pneumoniae'
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            ) then 1
 when lookup_outbreak_pui_labs.test_description = 'c. pneumoniae'
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            )
            then 2
  when lookup_outbreak_pui_labs.test_description = 'c. pneumoniae'
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when lookup_outbreak_pui_labs.test_description = 'c. pneumoniae'
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as resp_rcp,
    min(case when outbreak_master_covid_tests.result_component_id is not null
            and (lower(procedure_order_result_clinical.result_value) like '%positive%'
            or (lower(procedure_order_result_clinical.result_value) like '%detected%'
                and lower(procedure_order_result_clinical.result_value) not like '%not%')
            ) then 1
 when outbreak_master_covid_tests.result_component_id is not null
            and (lower(procedure_order_result_clinical.result_value) like '%neg%'
            or lower(procedure_order_result_clinical.result_value) like ('%not detected%')
            ) then 2
  when outbreak_master_covid_tests.result_component_id is not null
            and lower(procedure_order_result_clinical.result_value) like '%pending%'
            then 3
    when outbreak_master_covid_tests.result_component_id is not null
            and (lower(procedure_order_result_clinical.result_value) like '%not done%'
            or lower(procedure_order_result_clinical.result_value) like '%not reported%')
            then 4
    else 4 end
    ) as covid_19,
    max(case when (lower(procedure_order_clinical.procedure_name) like '%xr%'
        and lower(procedure_order_clinical.procedure_name) like '%chest%') -- region Chest X-Ray
        then procedure_order_clinical.abnormal_result_ind end
    ) as abxchest_yn --end region
from
    cohort
    inner join {{ref('procedure_order_clinical')}} as procedure_order_clinical
        on cohort.pat_key = procedure_order_clinical.pat_key
    left join {{ref('procedure_order_result_clinical')}} as procedure_order_result_clinical
        on procedure_order_clinical.proc_ord_key = procedure_order_result_clinical.proc_ord_key
        and lower(procedure_order_result_clinical.result_status) not in ('incomplete', 'not applicable')
    left join {{ref('lookup_outbreak_pui_labs')}} as lookup_outbreak_pui_labs
        on lookup_outbreak_pui_labs.result_component_id = procedure_order_result_clinical.result_component_id
    left join {{ref('outbreak_master_covid_tests')}} as outbreak_master_covid_tests
        on outbreak_master_covid_tests.result_component_id = procedure_order_result_clinical.result_component_id
where
    procedure_order_clinical.placed_date
        between (cohort.min_specimen_taken_date - interval '90 days')
            and (cohort.min_specimen_taken_date + interval '30 days')
    and lower(procedure_order_clinical.order_status) not in ('canceled', 'not applicable')
group by
    procedure_order_clinical.pat_key,
    cohort.outbreak_type
