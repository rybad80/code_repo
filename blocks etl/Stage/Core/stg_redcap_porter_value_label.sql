{{ config(meta = {
    'critical': true
}) }}

/*Parse redcap_metadata.element_enum into long format with ids and value labels
Note: Current method doesn't work for cases in which the element_id is a string rather than numeric
*/

with num_spine as (
/*
this is to generate a list of numbers 1 to 1000
used in the regex below
*/
    select
        cast(row_number() over (order by date_key asc) as integer) as idx
    from
        {{ ref('dim_date') }}
	order by idx -- note, need the order by for the join later
	limit 1000
)
select
    redcap_metadata.project_id,
    redcap_metadata.field_name,
    regexp_extract(
        regexp_replace(
        redcap_metadata.element_enum, '(?<=\\n)( )+', ''),  -- remove spaces after /n
        '(^|(?<=\\n))-?[\d]+',  -- starting digits (possibly negative) or first digits after '\n '
        1,                    -- start at the beginning
        num_spine.idx             -- which instance to grab
    ) as element_id,
    regexp_extract(
        redcap_metadata.element_enum,
        '(?<=, )[^\\]+',     -- text between 1st comma and '\'
        1,
        num_spine.idx
    ) as element_text
from
    {{source('ods_redcap_porter','redcap_metadata')}} as redcap_metadata
    inner join num_spine as num_spine
        on num_spine.idx between 1 and regexp_match_count(redcap_metadata.element_enum, '\\n') + 1
where
    /*Parsing currently only works for these types*/
    redcap_metadata.element_type in (
        'radio',
        'select',
        'checkbox'
    )
