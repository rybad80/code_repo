with visit_maps as (
    select
        enc_init.pat_id as init_patid,
        enc_init.pat_enc_csn_id as init_csn,
        enc_init.hosp_admsn_time as init_hosp_admit,
        enc_72.pat_id as less72_patid,
        enc_72.pat_enc_csn_id as less72_csn,
        enc_72.hosp_admsn_time as less72_hosp_admit,
        row_number() over (partition by enc_init.pat_enc_csn_id order by enc_72.hosp_admsn_time desc) as admit_order
    from
        {{ref('stg_pat_enc')}} as enc_init
        inner join {{ref('stg_pat_enc')}} as enc_72
            on enc_init.pat_id = enc_72.pat_id
            and enc_init.pat_enc_csn_id != enc_72.pat_enc_csn_id
    where
        enc_init.hosp_admsn_time is not null
        and enc_72.hosp_admsn_time is not null
        and enc_init.hosp_admsn_time < enc_72.hosp_admsn_time
        and extract(epoch from enc_72.hosp_admsn_time - enc_init.hosp_admsn_time) / 60 / 60 between 0 and 72
)
select
    init_lookup.visit_key,
    less72_lookup.visit_key as less_72hr_visit_key,
    case when less72_lookup.visit_key is null then 0 else 1 end as less_72hr_hosp_admit_ind
from
    visit_maps
    left join {{ref('stg_visit_key_lookup')}} as init_lookup
        on init_lookup.encounter_id = visit_maps.init_csn
        and init_lookup.source_name = 'clarity'
    left join {{ref('stg_visit_key_lookup')}} as less72_lookup
        on less72_lookup.encounter_id = visit_maps.less72_csn
        and less72_lookup.source_name = 'clarity'
where
    visit_maps.admit_order = 1
