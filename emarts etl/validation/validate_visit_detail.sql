{%- set src_table = 'stg_visit_clarity__maierd' %}
{%- set tgt_table = 'visit' %}
{%- set base_raw_table = 'STG_PAT_ENC__MAIERD' %}
{%- set base_raw_col = 'pat_enc_csn_id' %}
{%- set natural_id_col = 'enc_id' %}
{%- set result_cols = get_visit_result_columns() %}
{# col_name, lkup_table, lkup_table_alias, lkup_join_col, base_raw_join_col, raw_compare_col #}

select 
    src.{{natural_id_col}}
    {%- for col_name, lkup_table, lkup_table_alias, lkup_join_col, base_raw_join_col, raw_compare_col in result_cols %}
    , case
        when src.{{col_name}} = tgt.{{col_name}} 
            or (src.{{col_name}} is null and tgt.{{col_name}} is null) 
            or src.{{col_name}}::varchar(1200) = tgt.{{col_name}}::varchar(1200) then 'MATCH TARGET'
        {%- if lkup_table is not none %}
        when src.{{col_name}} = {{lkup_table_alias}}.{{raw_compare_col}} 
            or (src.{{col_name}} is null and {{lkup_table_alias}}.{{raw_compare_col}} is null) 
            or src.{{col_name}}::varchar(1200) = {{lkup_table_alias}}.{{raw_compare_col}}::varchar(1200) then 'MATCH RAW'
        {%- elif raw_compare_col is not none %}
        when src.{{col_name}} = raw_tbl.{{raw_compare_col}} 
            or (src.{{col_name}} is null and raw_tbl.{{raw_compare_col}} is null) 
            or src.{{col_name}}::varchar(1200) = raw_tbl.{{raw_compare_col}}::varchar(1200) then 'MATCH RAW'        
        {%- endif %}
        else 'NO MATCH'
    end as {{col_name}}_result
    {%- endfor %}        
from 
    chop_analytics_dev..{{src_table}} as src
    left join cdwuat..{{tgt_table}} tgt
        on tgt.{{natural_id_col}} = src.{{natural_id_col}}
    left join chop_analytics_dev..{{base_raw_table}} as raw_tbl
        on raw_tbl.{{base_raw_col}} = tgt.{{natural_id_col}}
    {%- for col_name, lkup_table, lkup_table_alias, lkup_join_col, base_raw_join_col, raw_compare_col in result_cols %}
        {%- if lkup_table is not none %}
    left join cdwuat..{{lkup_table}} as {{lkup_table_alias}}
        on {{lkup_table_alias}}.{{lkup_join_col}} = raw_tbl.{{base_raw_join_col}}
        {%- endif %}
    {%- endfor %}