{%- set src_table = 'stg_visit_clarity__maierd' %}
{%- set tgt_table = 'visit' %}
{%- set base_raw_table = 'STG_PAT_ENC__MAIERD' %}
{%- set base_raw_col = 'pat_enc_csn_id' %}
{%- set natural_id_col = 'enc_id' %}
{%- set result_cols = get_visit_result_columns() %}
{# col_name, lkup_table, lkup_table_alias, lkup_join_col, base_raw_join_col, raw_compare_col #}

with aggregated as (
    select 'dummy' as field_name, 'dummy' as match_result, -1 as counts
    {%- for col_name, lkup_table, lkup_table_alias, lkup_join_col, base_raw_join_col, raw_compare_col in result_cols %}
    union all
    select '{{col_name}}' as field_name, {{col_name}}_result as match_result, count(*) as counts from {{ref('validate_visit_detail')}}  group by 2
    {%- endfor %}
),
totals as (
    select count(*) as num_rows from chop_analytics_dev.ADMIN.{{src_table}} 
)
select 
    field_name,
    match_result,
    counts,
    counts*100.0/totals.num_rows as pct_total
from 
    aggregated
    inner join totals on 1 = 1
where
    field_name <> 'dummy'
