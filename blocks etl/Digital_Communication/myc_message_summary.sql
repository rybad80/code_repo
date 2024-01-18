with cohort as (
    select
        myc_message_detail.myc_message_id,
        myc_message_detail.convo_id,
        myc_message_detail.convo_subject,
        myc_message_detail.subtopic,
        myc_message_detail.message_type,
        myc_message_detail.message_type_id,
        myc_message_detail.subject,
        myc_message_detail.convo_line,
        myc_message_detail.csn,
        myc_message_detail.myc_message_create_time,
        myc_message_detail.myc_message_direction,
        stg_patient.patient_name,
        stg_patient.mrn,
        stg_patient.pat_id,
        stg_patient.dob,
        myc_message_detail.myc_message_sender,
        myc_message_detail.myc_message_recipient,
        myc_message_detail.modified_to,
        myc_message_detail.pool_name,
        myc_message_detail.department_id,
        myc_message_detail.department_name,
        myc_message_detail.specialty_name,
        myc_message_detail.notallow_reply_yn,
        myc_message_detail.reply_direct_yn,
        myc_message_detail.inbasket_msg_id,
        myc_message_detail.parent_message_id,
        myc_message_detail.first_action,
        myc_message_detail.last_action,
        myc_message_detail.final_handled_time,
        date(myc_message_create_time) as myc_msg_create_date,
        (date(myc_message_create_time) - date(dob))/365 as pat_age_of_mesg,
        max(myc_message_detail.rec_fwd_line) as times_fwd,
        max(myc_message_detail.final_handled_time - myc_message_detail.myc_message_create_time)
            as message_turnaround_time,
        max(myc_message_detail.convo_line) over(partition by convo_id) as thread_length,
        case when myc_message_detail.convo_line = thread_length then 1 else 0 end as last_message_ind,
        max(myc_message_detail.final_handled_time)
            over(partition by myc_message_detail.convo_id)
            as convo_final_time,
        min(myc_message_detail.myc_message_create_time)
            over(partition by myc_message_detail.convo_id)
            as convo_start_time,
        extract(epoch from (convo_final_time - convo_start_time)) / 3600 as res_time,
        case when first_action is null then 1 else 0 end as unhandled_ind,
        case when date(myc_message_create_time) < current_date - 7 then 1 else 0 end as seven_day_ind,
        case when seven_day_ind = 1 and unhandled_ind = 1 then 1 else 0 end as unhandled_7_day_ind,
        case when convo_line = 1 and myc_message_direction = 'To Patient' then 'To Patient' else null end
            as original_direction_to_pat,
        extract(hour from myc_message_create_time) as message_time_hr,
        case when message_time_hr between '06' and '09' then 'Pre-Clinic'
	        when message_time_hr between '09' and '12' then 'AM'
	        when message_time_hr between '12' and '15' then 'Mid-Day'
	        when message_time_hr between '15' and '18' then 'Afternoon'
	        when message_time_hr between '18' and '21' then 'Evening'
	        when message_time_hr between '21' and '06' then 'Overnight'
		else null end as grouped_msg_time,
        max(case when convo_line = 1 and myc_message_direction = 'From Patient' then 1 else 0 end)
            over(partition by convo_id)
            as pat_init_thread_ind,
        max(
            case when myc_message_detail.convo_line = 1
                and myc_message_detail.myc_message_direction = 'From Patient' then 1 else 0 end
        ) as first_msg_from_pat_ind,
        max(case
            when myc_message_detail.myc_message_recipient like 'P %'
                or myc_message_detail.modified_to like 'P %' then 1 else 0
            end) as sent_modified_pool_ind,
        max(case when myc_message_detail.cmd_audit in ('106', '163') then 1 else 0 end) as fwd_ind,
        max(case when myc_message_detail.fwd_to_user_role in ('Anesthesiologist',
            'Nursing Inptnt Adv Pract Nurse',
            'Nursing Outpnt Adv Pract Nurse',
            'Nursing Physician Practice',
            'Physician',
            'Psychologist') then 1 else 0 end) as fwd_to_prov_ind,
        case when convo_line = thread_length and myc_message_direction = 'To Patient' then 1 else 0 end
            as last_convo_msg_frm_prov_ind,
        extract(epoch from message_turnaround_time)/3600 as message_turnaround_time_hr,
        max(case when myc_message_detail.last_action_id = '7' then 1 else 0 end) as simple_thx_ind
    from
        {{ ref ('myc_message_detail') }} as myc_message_detail
        left join {{ ref ('stg_patient') }} as stg_patient
            on myc_message_detail.pat_id = stg_patient.pat_id
    where
        myc_message_detail.myc_message_create_time >= '2022-07-14' --Epic encounter creation fix date
        --conversation message types
        and myc_message_detail.message_type_id in ('11', '12', '13', '14', '15', '16', '18', '25')
    group by
        myc_message_detail.myc_message_id,
        myc_message_detail.convo_id,
        myc_message_detail.convo_line,
        myc_message_detail.inbasket_msg_id,
        myc_message_detail.message_type,
        myc_message_detail.message_type_id,
        myc_message_detail.subtopic,
        myc_message_detail.convo_subject,
        myc_message_detail.subject,
        myc_message_detail.csn,
        myc_message_detail.myc_message_create_time,
        myc_message_detail.myc_message_direction,
        stg_patient.patient_name,
        stg_patient.mrn,
        stg_patient.pat_id,
        stg_patient.dob,
        myc_message_detail.myc_message_sender,
        myc_message_detail.myc_message_recipient,
        myc_message_detail.modified_to,
        myc_message_detail.pool_name,
        myc_message_detail.department_id,
        myc_message_detail.department_name,
        myc_message_detail.specialty_name,
        notallow_reply_yn,
        reply_direct_yn,
        first_action,
        last_action,
        parent_message_id,
        final_handled_time
),

prov_hand as (
    select
        myc_mesg_audit.message_id,
        1 as provider_handled_ind
    from
        {{source('clarity_ods', 'myc_mesg_audit')}} as myc_mesg_audit
        left join {{ ref ('worker') }} as worker
            on myc_mesg_audit.who_handld_user_id = upper(worker.ad_login)
    where
        ad_login is not null
        and job_family in ('Anesthesiologist',
        'Nursing Inptnt Adv Pract Nurse',
        'Nursing Outpnt Adv Pract Nurse',
        'Nursing Physician Practice',
        'Physician',
        'Psychologist')
    group by
        myc_mesg_audit.message_id
),

demographic_info as (
    select
        cohort.myc_message_id,
        cohort.pat_id,
        encounter_all.csn,
        encounter_all.patient_address_seq_num,
        encounter_all.patient_address_zip_code,
        encounter_all.payor_name,
        encounter_all.payor_group,
        stg_patient.race,
        stg_patient.ethnicity,
        stg_patient.race_ethnicity,
        stg_patient.preferred_language,
        social_vulnerability_index.overall_category,
        social_vulnerability_index.overall_percentile as overall_percentile_svi,
        equity_coi2.opportunity_score_coi_metro_norm,
        equity_coi2.opportunity_lvl_coi_metro_norm
    from
	cohort
	inner join {{ ref ('encounter_all') }} as encounter_all
            on cohort.csn = encounter_all.csn
        inner join {{ ref ('stg_patient') }} as stg_patient
            on encounter_all.pat_key = stg_patient.pat_key
        left join {{ ref ('patient_geospatial') }} as patient_geospatial
            on patient_geospatial.pat_key = encounter_all.pat_key
                and patient_geospatial.address_seq_num = encounter_all.patient_address_seq_num
        left join  {{source ('cdc_ods', 'social_vulnerability_index')}} as social_vulnerability_index
            on patient_geospatial.census_tract_fips_code_2010 = social_vulnerability_index.fips
        left join {{ ref ('equity_coi2') }} as equity_coi2
            on patient_geospatial.census_tract_geoid_2010 = equity_coi2.census_tract_geoid_2010
                and observation_year = '2015'
    where
        encounter_all.encounter_type = 'Mychart Encounter'
        and encounter_all.encounter_date >= '2022-08-01'
)

select
    {{
        dbt_utils.surrogate_key([
            'cohort.myc_message_id',
            'cohort.convo_id',
            'stg_myc_ib_thread.inbasket_thread'
        ])
    }} as myc_message_convo_key,
    cohort.myc_message_id,
	cohort.convo_id,
	cohort.convo_subject,
	cohort.subtopic,
	cohort.message_type,
	cohort.message_type_id,
	cohort.subject,
	cohort.convo_line,
	cohort.csn,
	cohort.myc_message_create_time,
	cohort.myc_message_direction,
	cohort.patient_name,
	cohort.mrn,
	cohort.pat_id,
	cohort.dob,
    cohort.pat_age_of_mesg,
	cohort.myc_message_sender,
	cohort.myc_message_recipient,
	cohort.modified_to,
	cohort.pool_name,
	cohort.department_id,
	cohort.department_name,
	cohort.specialty_name,
    dim_department.intended_use_name,
	cohort.notallow_reply_yn,
	cohort.reply_direct_yn,
	cohort.inbasket_msg_id,
    stg_myc_ib_thread.inbasket_thread,
    stg_myc_ib_thread.inbasket_chain_length,
	cohort.parent_message_id,
	cohort.first_action,
	cohort.last_action,
	cohort.final_handled_time,
	cohort.times_fwd,
	cohort.message_turnaround_time,
	cohort.thread_length,
	cohort.last_message_ind,
	cohort.convo_final_time,
	cohort.convo_start_time,
	cohort.res_time,
	cohort.first_msg_from_pat_ind,
	cohort.sent_modified_pool_ind,
	cohort.fwd_ind,
	cohort.fwd_to_prov_ind,
	cohort.simple_thx_ind,
	cohort.message_turnaround_time_hr,
	cohort.last_convo_msg_frm_prov_ind,
	cohort.myc_msg_create_date,
	cohort.unhandled_ind,
	cohort.unhandled_7_day_ind,
	cohort.original_direction_to_pat,
	cohort.pat_init_thread_ind,
	cohort.grouped_msg_time,
	prov_hand.provider_handled_ind,
	demographic_info.patient_address_seq_num,
	demographic_info.patient_address_zip_code,
	demographic_info.payor_name,
	demographic_info.payor_group,
	demographic_info.race,
	demographic_info.ethnicity,
	demographic_info.race_ethnicity,
    demographic_info.preferred_language,
    case when demographic_info.preferred_language = 'ENGLISH' then 'English'
    	   when demographic_info.preferred_language = 'SPANISH' then 'Spanish'
    	   else 'Other than English/Spanish' end as otes_language,
    demographic_info.overall_category,
    demographic_info.overall_percentile_svi,
    demographic_info.opportunity_score_coi_metro_norm,
    demographic_info.opportunity_lvl_coi_metro_norm
from
    cohort
left join
    prov_hand
        on cohort.myc_message_id = prov_hand.message_id
left join
    demographic_info
        on cohort.myc_message_id = demographic_info.myc_message_id
left join
    {{ ref ('dim_department') }} as dim_department
        on cohort.department_id = dim_department.department_id
left join
    {{ ref('stg_myc_ib_thread') }} as stg_myc_ib_thread
        on cohort.inbasket_msg_id = stg_myc_ib_thread.inbasket_msg_id
        