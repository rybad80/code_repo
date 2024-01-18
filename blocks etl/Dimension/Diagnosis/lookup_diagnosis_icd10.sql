{% set db =  env_var('ENVIRONMENT', 'UAT') if target.type == 'snowflake' else  env_var("DATA_LAKE_DB","CDW_ODS_UAT") %}
{{ config(meta = {
    'critical': true
}) }}

{% set fetch_tables = dbt_utils.get_relations_by_pattern(
    schema_pattern='admin',
    table_pattern='icd10_diagnosis_hierarchy_%',
    database=db
)%}

with
all_years as (
    {{ dbt_utils.union_relations(
        relations=fetch_tables,
        exclude=["UPD_DT", "UPDATE_DATE"]
    ) }}
)
,

most_recent as (
    select
        all_years.*,
        all_years.year as latest_year,
        case 
            when all_years.year = max(all_years.year) over(partition by all_years.icd10_code) then 1
            when all_years.derived_code_ind = 1 then 1 
            else 0  
            end as current_listing_ind,
        row_number() over(
            partition by all_years.icd10_code 
            order by -- use un-derived code first, then most recent year
                all_years.derived_code_ind,
                all_years.year desc 
        ) as code_listing_seq_num
    from 
        all_years
)

select    
    {{ dbt_utils.star(from = source('ods', 'icd10_diagnosis_hierarchy_2022'),
     except=["UPD_DT", "UPDATE_DATE", "YEAR"],
     relation_alias = "most_recent") }},
    current_listing_ind,
    latest_year,
    case
        when
            most_recent.category in ('B34', 'J06', 'J21', 'A08', 'R50', 'J02',
                'R11', 'J05', 'J11', 'H10', 'B08', 'B09',
                'B30', 'K12', 'J12', 'A87', 'A98')
            or lower(lookup_ed_icd10_groupers.major_group_desc) in ('respiratory diseases', 'systemic states')
            or (lower(lookup_ed_icd10_groupers.major_group_desc) = 'gastrointestinal diseases' 
                and lower(lookup_ed_icd10_groupers.subgroup_desc) = 'vomiting')
        then 1
        else 0
    end as viral_infection_ind
    
from 
    most_recent
    left join {{ref('lookup_ed_icd10_groupers')}} as lookup_ed_icd10_groupers
        on replace(most_recent.icd10_code, '.', '') = lookup_ed_icd10_groupers.icd10_code
where
    code_listing_seq_num = 1
