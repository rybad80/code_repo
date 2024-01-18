select distinct
    award_award_reference_wid as award_wid,
    award_award_reference_award_reference_id as award_id,
    coalesce(cast(award_data_award_line_data_line_number as int), 0) as line_number,
    coalesce(award_line_data_grant_reference_wid, 'N/A') as grant_wid,
    award_line_data_grant_reference_grant_id as grant_id,
    cast({{
        dbt_utils.surrogate_key([
            'award_wid',
            'award_id',
            'line_number',
            'grant_wid',
            'grant_id'
            ])
    }} as varchar(100)) as md5,
    current_timestamp as create_dt,
    'WORKDAY' as create_by,
    current_timestamp as upd_dt,
    'WORKDAY' as upd_by
from
    {{source('workday_ods', 'get_awards_data')}} as get_awards_data
where
    1 = 1
    and award_award_reference_award_reference_id is not null

