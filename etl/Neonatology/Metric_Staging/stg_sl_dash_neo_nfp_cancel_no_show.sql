select
    visit_key,
    encounter_date,
    no_show_or_cancel_48h_ind,
    past_appointment_ind,
    visit_location

from
    {{ ref('stg_sl_dash_neo_nfp_visits') }}

where
    past_appointment_ind = 1
