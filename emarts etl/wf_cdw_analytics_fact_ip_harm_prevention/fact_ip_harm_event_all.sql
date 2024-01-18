select
    cast(visit_key as bigint) as visit_key,
    cast(event_dt as timestamp) as harm_event_dt,
    cast(pat_key as bigint) as pat_key,
    cast(dept_key as bigint) as dept_key,
    cast(mstr_dept_grp_key as bigint) as mstr_dept_grp_key,
    cast(dept_grp_nm as varchar(100)) as dept_grp_nm,
    cast(dept_grp_abbr as varchar(100)) as dept_grp_abbr,
    cast(csn as numeric(14, 3)) as enc_id,
    cast(harm_id as bigint) as harm_id,
    cast(harm_type as varchar(30)) as harm_type,
    cast(numerator_source as varchar(50)) as data_source,
    cast(division as varchar(50)) as division,
    cast(pathogen_code_1 as varchar(50)) as pathogen_code_1,
    cast(pathogen_code_2 as varchar(50)) as pathogen_code_2,
    cast(pathogen_code_3 as varchar(50)) as pathogen_code_3,
    cast(numerator_value as integer) as numerator_value,
    cast(denominator_value as integer) as denominator_value,
    cast(conf_dt as timestamp) as harm_conf_dt,
    cast(hosp_admit_dt as timestamp) as hosp_admit_dt,
    cast(hosp_dischrg_dt as timestamp) as hosp_dischrg_dt,
    cast(compare_to_hai_pop_days_ind as byteint) as compare_to_hai_pop_days_ind,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from {{ ref('stg_harm_event_all_numerator') }}
union all
select
    cast(visit_key as bigint) as visit_key,
    cast(harm_event_dt as timestamp) as harm_event_dt,
    cast(pat_key as bigint) as pat_key,
    cast(dept_key as bigint) as dept_key,
    cast(mstr_dept_grp_key as bigint) as mstr_dept_grp_key,
    cast(dept_grp_nm as varchar(100)) as dept_grp_nm,
    cast(dept_grp_abbr as varchar(100)) as dept_grp_abbr,
    cast(enc_id as numeric(14, 3)) as enc_id, --csn net name change from fact_ip_tables
    cast(harm_id as bigint) as harm_id,
    cast(harm_type as varchar(30)) as harm_type,
    cast(data_source as varchar(50)) as data_source,
    cast(division as varchar(50)) as division,
    cast(pathogen_code_1 as varchar(50)) as pathogen_code_1,
    cast(pathogen_code_2 as varchar(50)) as pathogen_code_2,
    cast(pathogen_code_3 as varchar(50)) as pathogen_code_3,
    cast(numerator_value as integer) as numerator_value,
    cast(denominator_value as integer) as denominator_value,
    cast(harm_conf_dt as timestamp) as harm_conf_dt,
    cast(hosp_admit_dt as timestamp) as hosp_admit_dt,
    cast(hosp_dischrg_dt as timestamp) as hosp_dischrg_dt,
    cast(compare_to_hai_pop_days_ind as byteint) as compare_to_hai_pop_days_ind,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from {{ ref('stg_harm_event_all_denominator') }} 
where harm_type <> 'VAP'
or (harm_type = 'VAP' and harm_event_dt < '2022-07-01') --stg_ instead of s_
union all
select
    cast(visit_key as bigint) as visit_key,
    cast(harm_event_dt as timestamp) as harm_event_dt,
    cast(pat_key as bigint) as pat_key,
    cast(dept_key as bigint) as dept_key,
    cast(mstr_dept_grp_key as bigint) as mstr_dept_grp_key,
    cast(dept_grp_nm as varchar(100)) as dept_grp_nm,
    cast(dept_grp_abbr as varchar(100)) as dept_grp_abbr,
    cast(csn as numeric(14, 3)) as enc_id,
    cast(harm_id as bigint) as harm_id,
    cast(harm_type as varchar(30)) as harm_type,
    cast(numerator_source as varchar(50)) as data_source,
    cast(division as varchar(50)) as division,
    cast(pathogen_code_1 as varchar(50)) as pathogen_code_1,
    cast(pathogen_code_2 as varchar(50)) as pathogen_code_2,
    cast(pathogen_code_3 as varchar(50)) as pathogen_code_3,
    cast(numerator_value as integer) as numerator_value,
    cast(denominator_value as integer) as denominator_value,
    cast(conf_dt as timestamp) as harm_conf_dt,
    cast(hosp_admit_dt as timestamp) as hosp_admit_dt,
    cast(hosp_dischrg_dt as timestamp) as hosp_dischrg_dt,
    cast(compare_to_hai_pop_days_ind as byteint) as compare_to_hai_pop_days_ind,
    current_timestamp as create_dt,
    'DBT' as create_by,
    current_timestamp as upd_dt
from {{ ref('stg_harm_event_all_post') }}
where harm_type <> 'VAP'
or (harm_type = 'VAP' and harm_event_dt < '2022-07-01')
