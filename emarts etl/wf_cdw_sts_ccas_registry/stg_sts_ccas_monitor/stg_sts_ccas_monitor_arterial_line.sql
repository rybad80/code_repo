select
    arterial_line_tmp2_cte.vsi_key,
    place_dt as art_line_place_dt,
    remove_dt as art_line_remove_dt,
    '1' as arterial_line,
    min(case when upper(lda_site) = 'RADIAL' then '1' else '2' end) as art_radial,
    min(case when upper(lda_site) = 'BRACHIAL' then '1' else '2' end) as art_brachial,
    min(case when upper(lda_site) = 'AXILLARY' then '1' else '2' end) as art_axillary,
    min(case when upper(lda_site) = 'FEMORAL' then '1' else '2' end) as art_femoral,
    min(case when upper(lda_site) = 'ULNAR' then '1' else '2' end) as art_ulnar,
    min(case when upper(lda_site) = 'PEDAL' then '1' else '2' end) as art_dorsalis_pedis,
    min(case when upper(lda_site) = 'POSTERIOR TIBIAL' then '1' else '2' end) as art_posterior_tibial,
    min(case when upper(lda_site) = 'UMBILICAL' then '1' else '2' end) as art_umbilical,
    min(case when upper(lda_disp) like '%CUTDOWN%' then '1' else '2' end) as cutdown,
    min(
        case when upper(lda_disp) like '%CUTDOWN%' and upper(lda_site) = 'RADIAL' then '1' else '2' end
    ) as cutdown_radial,
    min(
        case
            when
                upper(lda_disp) like '%CUTDOWN%' and (upper(lda_site) = 'ULNAR' or upper(meas_cmt) = 'MEDIAL') then '1'
            else '2'
        end
    ) as cutdown_ulnar,
    min(
        case when upper(lda_disp) like '%CUTDOWN%' and upper(lda_site) = 'FEMORAL' then '1' else '2' end
    ) as cutdown_femoral,
    min(
        case
            when
                upper(
                    lda_disp
                ) like '%CUTDOWN%' and upper(
                    lda_site
                ) != 'RADIAL' and upper(
                    lda_site
                ) != 'ULNAR' and upper(lda_site) != 'FEMORAL' and upper(meas_cmt) != 'MEDIAL' then '1'
            else '2'
        end
    ) as cutdown_other,
    min(case when upper(meas_val) = 'ULTRASOUND' then '1' else '2' end) as art_ultra,
    min(case when upper(lda_desc) like '%INTRACARDIAC%' then '1' else '2' end) as art_surg_place
from
    {{source('cdw','patient_lda')}} as lda
    inner join {{source('cdw','visit_stay_info_rows')}} as vsr on lda.pat_lda_key = vsr.pat_lda_key
    inner join {{source('cdw','flowsheet')}} as lda_fs on lda.fs_key = lda_fs.fs_key
    inner join {{ ref('stg_sts_ccas_monitor_arterial_line_tmp2') }} as arterial_line_tmp2_cte on arterial_line_tmp2_cte.vsi_key = vsr.vsi_key
where
    --1=1 AND
    vsr.seq_num = arterial_line_tmp2_cte.occurance
    and lda_fs.fs_id in (40008000, 40010017, 40007080, 40007073)
group by
    1, 2, 3, 4
