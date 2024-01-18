select
    vsi_key,
    perc_place_dt,
    perc_remove_dt,
    ins_by,
    pcp,
    min(
        case when side = 'RIGHT' and ins_vessel = 'INTERNAL JUGULAR' then '1' else '2' end
    ) as right_internal_jugular,
    min(case when side = 'RIGHT' and ins_vessel = 'SUBCLAVIAN' then '1' else '2' end) as right_subclavian,
    min(case when side = 'RIGHT' and ins_vessel = 'FEMORAL' then '1' else '2' end) as right_femoral_vein,
    min(
        case when side = 'LEFT' and ins_vessel = 'INTERNAL JUGULAR' then '1' else '2' end
    ) as left_internal_jugular,
    min(case when side = 'LEFT' and ins_vessel = 'SUBCLAVIAN' then '1' else '2' end) as left_subclavian,
    min(case when side = 'LEFT' and ins_vessel = 'FEMORAL' then '1' else '2' end) as left_femoral_vein,
    min(
        case
            when
                side in (
                    'RIGHT', 'LEFT'
                ) and not(
                    ins_vessel in (
                        'INTERNAL JUGULAR',
                        'SUBCLAVIAN',
                        'FEMORAL',
                        'INTERNAL JUGULAR',
                        'SUBCLAVIAN',
                        'FEMORAL'
                    )
                ) then '1'
            else '2'
        end
    ) as pcp_other
from
    (
        select
            fr.vsi_key,
            place_dt as perc_place_dt,
            remove_dt as perc_remove_dt,
            '1' as pcp,
            max(case when f.fs_id = 40000347 then upper(meas_val) else '' end) as side,
            max(case when f.fs_id = 40000341 then upper(meas_val) else '' end) as ins_vessel,
            max(case when f.fs_id = 40000369 then upper(meas_val) else '' end) as ins_tech,
            max(case when f.fs_id = 40000360 then upper(meas_val) else '' end) as ins_by
        from
            {{source('cdw','patient_lda')}} as lda
            inner join {{source('cdw','visit_stay_info')}} as vsi on vsi.visit_key = lda.visit_key
            inner join {{source('cdw','visit_stay_info_rows')}} as vsr on vsi.vsi_key = vsr.vsi_key
            inner join {{source('cdw','flowsheet_record')}} as fr on vsr.vsi_key = fr.vsi_key
            inner join {{source('cdw','flowsheet_measure')}} as fm on fr.fs_rec_key = fm.fs_rec_key
            inner join {{source('cdw','flowsheet')}} as f on fm.fs_key = f.fs_key
            inner join {{source('cdw','flowsheet_lda_group')}} as lda_group on vsr.fs_key = lda_group.fs_key
            inner join {{source('cdw','cdw_dictionary')}} as lda_group_dict on lda_group_dict.dict_key = lda_group.dict_lda_type_key
            inner join {{ ref('stg_sts_ccas_monitor_encs') }} as encs on encs.anes_vsi_key = fr.vsi_key
        where
            1 = 1
            and vsr.seq_num = fm.occurance
            and lda_group_dict.src_id in (36, 2, 46, 1)
            and f.fs_id in (40000341, 40000347, 40000360, 40000369)
        group by
            1, 2, 3, 4
    ) as a
group by
    1, 2, 3, 4, 5
