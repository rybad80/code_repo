select
    r_enc_key,
    card_arrest_venue,
    r_card_arrest_strt_dt
from
    {{source('cdw', 'registry_pc4_cardiac_arrest')}}
where
    cur_rec_ind = 1
group by
    r_enc_key,
    card_arrest_venue,
    r_card_arrest_strt_dt
