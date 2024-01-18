
with pk_info as (
select table_constraints.table_schema,
	   lower(constraint_column_usage.table_name) table_name,
	   lower(constraint_column_usage.column_name) column_name
from 
    information_schema.table_constraints
    inner join information_schema.constraint_column_usage on constraint_column_usage.constraint_name = table_constraints.constraint_name
	    and constraint_column_usage.table_name = table_constraints.table_name
where
    lower(table_constraints.constraint_type) = 'primary key'
),

source_metadata_raw as (
select 'YYYYYYYY' as entity_owner,
	lower(columns.table_name) as table_name,
	lower(columns.column_name) as column_name,
	lower(columns.data_type) as data_type,
	case when columns.character_maximum_length=-1 then null
		when columns.character_maximum_length is not null then columns.character_maximum_length
		else columns.numeric_precision end as column_precision,
	columns.numeric_scale as column_scale,
	case when pk_info.table_name is not null then 1 else 0 end as is_primary_key,
	case when lower(columns.is_nullable)='no' then 1 else 0 end as is_required,
	columns.ordinal_position as column_order
from 
    INFORMATION_SCHEMA.COLUMNS
    inner join INFORMATION_SCHEMA.TABLES on columns.table_name = tables.TABLE_NAME
	  and columns.table_schema = tables.TABLE_SCHEMA
    left join pk_info on pk_info.table_schema = columns.table_schema
	 and pk_info.table_name = columns.table_name
	 and pk_info.column_name = columns.column_name
where 
    tables.TABLE_NAME = 'XXXXXXXX'
		and not(columns.DATA_TYPE in ('image','varbinary'))
	--or tables.TABLE_NAME in ('apoe1_apoe')
) ,

source_metadata as (

select
      entity_owner,
	  table_name,
	  case when CHARINDEX('_', column_name) = 1 then RIGHT(column_name, LEN(column_name) - 1) else column_name end as target_column_name,	  
	  column_name,
	  data_type as source_data_type,
	  case  when data_type = 'text' then 'string'
	            when data_type = 'varchar' then 'string'
		        when data_type = 'nvarchar' then 'string'
				when data_type = 'char' then 'string'
				when data_type = 'nchar' then 'string'
				when data_type = 'bit' then 'boolean'
				when data_type = 'int' then 'integer'
				when data_type = 'smallint' then 'integer'				
                when data_type = 'datetime' then 'datetime'
				when data_type = 'smalldatetime' then 'datetime'
                when data_type = 'numeric' then 'number'
				when data_type = 'decimal' then 'number'
				when data_type = 'float' then 'number'
				when data_type = 'xml' then 'string'
        else DATA_TYPE end as target_type_category,
	  case when data_type = 'datetime' then 'datetime'
				when data_type = 'smalldatetime' then 'timestamp'
				when data_type = 'decimal' then 'numeric'
				when data_type = 'text' then 'varchar'
				when data_type = 'bit' then 'boolean'
				when data_type = 'xml' then 'varchar'
        else DATA_TYPE end as target_data_type,
	  case when data_type in ('text','xml') then 4000
	       when data_type like '%char%' and column_precision is null then 4000 
	  else column_precision end as column_precision_new,
	  column_scale,
	  is_primary_key,
	  is_required,
	  column_order
from source_metadata_raw

) , 

pk as (
select table_name,
max(coalesce(case when is_primary_key = 1 then column_name else null end ,case when column_order = 1 then column_name else null end)) as column_name
from source_metadata
group by table_name
),

column_list as (
	select       
      '{' + CHAR(13) + '
	  "name": "'+source_metadata.target_column_name+'",' + CHAR(13) + '
        "description": "NA",' + CHAR(13) + '
        "type": "'+target_type_category +'",' + CHAR(13) + '
        "dbType": "'+target_data_type+'",'+
		'"constraints": { '+
		case when pk.column_name = source_metadata.column_name then '"required": true' else '' end +
        case when pk.column_name = source_metadata.column_name and source_data_type like '%char%' then ',
                 "maxLength":'+ cast(column_precision_new as varchar(max)) 
		     when source_data_type like '%char%' or source_data_type = 'text' then '
                 "maxLength":'+ cast(column_precision_new as varchar(max))
             when pk.column_name = source_metadata.column_name and source_data_type in ('numeric', 'int', 'decimal') then ',
                 "precision":'+ cast(column_precision_new as varchar(max)) +',
                 "scale":'+ cast(column_scale as varchar(max))  
             when source_data_type in ('numeric', 'int', 'decimal') then '
                 "precision":'+ cast(column_precision_new as varchar(max)) +',
                 "scale":'+ cast(column_scale as varchar(max))
        else '' end +'' + CHAR(13) + 
		+'
        }' + CHAR(13) + '
        '+','+
		'
        "source": [' + CHAR(13) + '
          {' + CHAR(13) + '
            "field": "'+source_metadata.column_name+'",' + CHAR(13) + '
            "resource": "'+source_metadata.table_name+'",' + CHAR(13) + '
            "package": "YYYYYYYY"' + CHAR(13) + '
          }' + CHAR(13) + '
        ]   ' + CHAR(13) + '
    },'
	as jsontext


from 
source_metadata left join pk on source_metadata.table_name = pk.table_name
    

),

header as (
select distinct
'{
  "profile": "chop-data-resource",
  "name": "'+'YYYYYYYY_'+table_name+'",
  "path": "data_lake",
  "description": "Not Available",
  "securityGroups": [
    "ccis_data"
  ],
  "loadStrategy": "truncate_reload",
  "schema": {
    "fields": [
	' as jsontext
from 
source_metadata
),

footer as (
select
'      {
        "name": "upd_dt",
        "description": "This column shows information about when the record was updated in the table. It is used by Data Engineers for Data Loading purposes",
        "type": "datetime",
        "dbType": "timestamp",
        "constraints": {
          "required": true
        }
      }
    ]
  '  as jsontext
),

primary_key as (
select 
',
    "primaryKey": "'+column_name+'"
' as jsontext
from pk
),

ending as (
select
' 
 }
}' bracket
)

select * from header
union all
select * from column_list
union all
select * from footer
union all 
select jsontext from primary_key
union all
select bracket from ending