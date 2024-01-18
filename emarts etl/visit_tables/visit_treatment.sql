with sq_hsp_trtmt_team as (
    select
    hsp_trtmt_team.pat_id,
    hsp_trtmt_team.pat_enc_date_real,
    hsp_trtmt_team.line,
    hsp_trtmt_team.trtmnt_team_rel_c,
    hsp_trtmt_team.trtmnt_tm_begin_dt,
    hsp_trtmt_team.trtmnt_tm_end_dt,
    hsp_trtmt_team.trtmnt_tm_ed_yn,
    hsp_trtmt_team.cm_ct_owner_id,
    hsp_trtmt_team.pat_enc_csn_id
from
    {{ source('clarity_ods', 'hsp_trtmt_team') }} as hsp_trtmt_team
),
exp_all as (
    select
    visit.visit_key,
    hsp_trtmt_team.line,
    prov.prov_key,
    hsp_trtmt_team.trtmnt_team_rel_c,
    case
        when dict_treat_rel.dict_key is null and hsp_trtmt_team.trtmnt_team_rel_c is null then -2
        when dict_treat_rel.dict_key is null and hsp_trtmt_team.trtmnt_team_rel_c is not null then -1
        else dict_treat_rel.dict_key
    end as dict_treat_rel_key,
    hsp_trtmt_team.pat_enc_csn_id,
    hsp_trtmt_team.trtmnt_tm_begin_dt,
    hsp_trtmt_team.trtmnt_tm_end_dt,
    hsp_trtmt_team.trtmnt_tm_ed_yn,
    {{yn_to_ind('hsp_trtmt_team.trtmnt_tm_ed_yn')}} as ed_ind,
    current_timestamp as create_dt,
    'CLARITY' as create_by,
    current_timestamp as upd_dt,
    'CLARITY' as upd_by
    from {{ source('clarity_ods', 'hsp_trtmt_team') }} as hsp_trtmt_team
    left join {{ ref('visit') }} as visit
        on hsp_trtmt_team.pat_enc_csn_id = visit.enc_id
    left join {{ source('cdw','provider') }} as prov
        on COALESCE(hsp_trtmt_team.prov_id,'0') = prov.prov_id
    left join {{ source('cdw','cdw_dictionary') }} as dict_treat_rel
        on dict_treat_rel.dict_cat_key = '10056'
        and dict_treat_rel.src_id = hsp_trtmt_team.trtmnt_team_rel_c
)
select
    cast(exp_all.visit_key as bigint) as visit_key,
    cast(exp_all.line as bigint) as seq_num,
    cast(exp_all.prov_key as bigint) as prov_key,
    cast(exp_all.dict_treat_rel_key as bigint) as dict_treat_rel_key,
    cast(exp_all.pat_enc_csn_id as bigint) as enc_id,
    cast(exp_all.trtmnt_tm_begin_dt as timestamp) as prov_start_dt,
    cast(exp_all.trtmnt_tm_end_dt as timestamp) as prov_end_dt,
    cast(exp_all.ed_ind as bigint) as ed_ind,
    cast(exp_all.create_dt as timestamp) as create_dt,
    cast(exp_all.create_by as varchar(20)) as create_by,
    cast(exp_all.upd_dt as timestamp) as upd_dt,
    cast(exp_all.upd_by as varchar(20)) as upd_by
from exp_all
