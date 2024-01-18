with stage as (
    select
        immune.immune_id
    from
        {{source('clarity_ods', 'immune')}} as immune
    where
        immune.immnztn_status_c = 1
        and immune.immune_date is not null
)
select
    immune.pat_id,
    immune.immune_date as received_date,
    immune.immunzatn_id as grouper_records_numeric_id,
    immune.order_id,
    stage.immune_id,
    row_number() over(
        partition by immune.pat_id, immune.immune_date, immune.immunzatn_id order by immune.immune_id
    ) as row_num,
    case
        when lower(clarity_immunzatn.name) like '%flu%'
            then 1
            else 0
        end as influenza_vaccine_ind
from
    stage
inner join
    {{source('clarity_ods', 'immune')}} as immune
    on stage.immune_id = immune.immune_id
left join
    {{source('clarity_ods', 'clarity_immunzatn')}} as clarity_immunzatn
    on immune.immunzatn_id = clarity_immunzatn.immunzatn_id
