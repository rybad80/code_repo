select
    stg_rpm_patient.pat_id,
    pat_myc_prxy_acss.proxy_wpr_id,
    pat_relationship_list.pat_relationship_id,
    coalesce(
        pat_myc_prxy_acss.proxy_pat_id,
        pat_relationship_list.pat_contact_pat_id
    ) as parent_pat_id,
    /* order by display sequence first, then by proxy id
    to find the "best" proxy. Need to order by proxy id
    because some patients have `1` as the display sequence
    for all relations. */
    row_number() over (
        partition by
            stg_rpm_patient.pat_id
        order by
            pat_relationship_list.display_sequence nulls last,
            pat_myc_prxy_acss.proxy_wpr_id
    ) as rn,
    zc_myc_prxy_relatn.name as prxy_relatn,
    pat_myc_prxy_acss.access_ecl_id
from
    {{ref('stg_rpm_patient') }} as stg_rpm_patient
    inner join {{ source('clarity_ods', 'pat_myc_prxy_acss') }} as pat_myc_prxy_acss
        on pat_myc_prxy_acss.pat_id = stg_rpm_patient.pat_id
            /* make sure the proxy id is active */
            and pat_myc_prxy_acss.from_date <= current_date
            and (pat_myc_prxy_acss.proxy_status_c = 1 /* activated */
                or pat_myc_prxy_acss.proxy_status_c is null)
            and (pat_myc_prxy_acss.to_date >= current_date
                or pat_myc_prxy_acss.to_date is null)
    inner join {{ source('clarity_ods', 'zc_myc_prxy_relatn') }} as zc_myc_prxy_relatn
		on pat_myc_prxy_acss.myc_prxy_relatn_c = zc_myc_prxy_relatn.myc_prxy_relatn_c
    left join {{ source('clarity_ods', 'pat_relationship_list') }} as pat_relationship_list
        on pat_relationship_list.pat_id = stg_rpm_patient.pat_id
            and pat_relationship_list.mypt_id = pat_myc_prxy_acss.proxy_wpr_id
