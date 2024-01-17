select
    docs_rcvd.pat_id,
    imm_admin.imm_date as received_date,
    coalesce(imm_admin.imm_type_id, 0) as grouper_records_numeric_id,
    case
        when lower(imm_admin.imm_type_free_text) like '%flu%'
            then 1
            else 0
        end as influenza_vaccine_ind
from
    {{source('clarity_ods', 'imm_admin')}} as imm_admin
    inner join {{source('clarity_ods', 'docs_rcvd')}} as docs_rcvd
        on docs_rcvd.document_id = imm_admin.document_id
	where
        imm_admin.imm_status_c = 1 --Given
        and imm_admin.imm_date is not null
        and (docs_rcvd.record_state_c != 4 or docs_rcvd.record_state_c is null) --removing invalid records
