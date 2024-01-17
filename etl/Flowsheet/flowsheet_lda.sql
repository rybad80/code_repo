/* logic for the lda join taken from epic user COG240 class material:
https://galaxy.epic.com/Redirect.aspx?DocumentID=4354005&PrefDocID=104773 */

with lda_types as (
   --  One LDA can have more than one reportable `type`.
    -- We can aggregate types to one combined row so that we don't duplicate LDA data 
    select
        ip_lda_noaddsingle.ip_lda_id,
        group_concat(zc_lines_group.name, ';') as lda_types
    from
        {{ source('clarity_ods', 'ip_lda_noaddsingle') }} as ip_lda_noaddsingle
        inner join {{ source('clarity_ods', 'ip_flo_lda_types') }} as ip_flo_lda_types
            on ip_flo_lda_types.id = ip_lda_noaddsingle.flo_meas_id
            and ip_flo_lda_types.contact_date_real = ip_lda_noaddsingle.lda_group_cdr
        inner join {{ source('clarity_ods', 'zc_lines_group') }} as zc_lines_group
            on zc_lines_group.lines_group_c = ip_flo_lda_types.lda_type_ot_c
    group by
        ip_lda_noaddsingle.ip_lda_id
),

users as (
	select
		clarity_emp.name,
        clarity_ser.prov_type,
        clarity_emp.user_id
	from
        {{ source('clarity_ods', 'clarity_emp') }} as clarity_emp
        left join {{ source('clarity_ods', 'clarity_ser') }} as clarity_ser
            on clarity_emp.prov_id = clarity_ser.prov_id
)

select
    ip_lda_noaddsingle.ip_lda_id,
    ip_flwsht_meas.flo_meas_id,
    stg_patient.patient_name,
    stg_patient.mrn,
    stg_patient.dob,
    ip_lda_noaddsingle.removal_instant,
    ip_lda_noaddsingle.placement_instant,
    coalesce(lda_types.lda_types, 'none recorded') as lda_types,
    ip_lda_noaddsingle.description as lda_description,
    ip_lda_noaddsingle.properties_display,
    ip_flwsht_meas.recorded_time as recorded_date,
    ip_flwsht_meas.occurance,
    ip_flwsht_meas.meas_value,
    ip_flwsht_meas.meas_comment,
    ip_flo_gp_data.disp_name,
    taken_user.name as taken_user_name,
    taken_user.prov_type as taken_user_prov_type,
    entry_user.name as entry_user_name,
    entry_user.prov_type as entry_user_prov_type,
    /* other pk */
    ip_flwsht_meas.fsd_id,
    ip_lda_noaddsingle.pat_id,
    stg_patient.pat_key,
    /* csn when flowsheet was recorded */
    pat_enc_hsp.pat_enc_csn_id as flowsheet_rec_csn_id,
    stg_encounter_flowsheet_rec.visit_key as flowsheet_rec_visit_key,
     /* csn when lda was placed */
    ip_lda_noaddsingle.pat_enc_csn_id as lda_place_csn,
    stg_encounter_lda_place.visit_key as lda_place_visit_key

from
    {{ source('clarity_ods', 'ip_lda_noaddsingle') }} as ip_lda_noaddsingle
    left join lda_types
        on lda_types.ip_lda_id = ip_lda_noaddsingle.ip_lda_id
    inner join {{ source('clarity_ods', 'ip_flowsheet_rows') }} as ip_flowsheet_rows
        on ip_flowsheet_rows.ip_lda_id = ip_lda_noaddsingle.ip_lda_id
    inner join {{ source('clarity_ods', 'ip_flwsht_rec') }} as ip_flwsht_rec
        on ip_flwsht_rec.inpatient_data_id = ip_flowsheet_rows.inpatient_data_id
    inner join {{ source('clarity_ods', 'ip_flwsht_meas') }} as ip_flwsht_meas
        on ip_flwsht_meas.fsd_id = ip_flwsht_rec.fsd_id
        and ip_flwsht_meas.occurance = ip_flowsheet_rows.line
    inner join {{ source('clarity_ods', 'ip_flo_gp_data') }} as ip_flo_gp_data
        on ip_flo_gp_data.flo_meas_id = ip_flwsht_meas.flo_meas_id
    inner join {{ source('clarity_ods', 'pat_enc_hsp') }} as pat_enc_hsp
        on pat_enc_hsp.inpatient_data_id = ip_flowsheet_rows.inpatient_data_id
    inner join {{ref('stg_encounter')}} as stg_encounter_flowsheet_rec
        on stg_encounter_flowsheet_rec.csn = pat_enc_hsp.pat_enc_csn_id
    inner join {{ref('stg_encounter')}} as stg_encounter_lda_place
        on stg_encounter_lda_place.csn = ip_lda_noaddsingle.pat_enc_csn_id
    inner join {{ref('stg_patient')}} as stg_patient
        on stg_patient.pat_id = ip_lda_noaddsingle.pat_id
	left join users as taken_user
        on taken_user.user_id = ip_flwsht_meas.taken_user_id
	left join users as entry_user
        on entry_user.user_id = ip_flwsht_meas.entry_user_id
