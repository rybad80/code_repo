with flowsheet_limited as (
    select
        flowsheet.fs_key,
        flowsheet.fs_id,
        flowsheet.fs_nm
    from
        {{ source('cdw','flowsheet') }} as flowsheet

    group by
        flowsheet.fs_key,
        flowsheet.fs_id,
        flowsheet.fs_nm
),

flowsheet_lda_group_limited as (
    select flowsheet_lda_group.dict_lda_type_key
    from
        {{ source('cdw','flowsheet_lda_group') }} as flowsheet_lda_group
    group by
        flowsheet_lda_group.dict_lda_type_key
)

select
    flowsheet_limited.fs_id,
    flowsheet_limited.fs_key,
    flowsheet_limited.fs_nm,
    flowsheet_lda_group_limited.dict_lda_type_key
from
    flowsheet_limited
cross join flowsheet_lda_group_limited
where (
        flowsheet_limited.fs_id in (
            40068151,
            40068153,
            40068156
        )
        and flowsheet_lda_group_limited.dict_lda_type_key in (
            20870,
            20861,
            20862
        )
    )
    or flowsheet_limited.fs_id in (40068156021, 40068153011, 4006815102)
