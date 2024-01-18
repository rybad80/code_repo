with transplant_info as (
    select distinct
        transplant_info.pat_key,
        transplant_info.epsd_key,
        dim_transplant_class.transplant_class_nm as organ,
        case when lower(dim_transplant_class.transplant_class_nm) = 'heart' then 1
            when lower(dim_transplant_class.transplant_class_nm) = 'lung' then 2
            when lower(dim_transplant_class.transplant_class_nm) = 'liver' then 3
            when lower(dim_transplant_class.transplant_class_nm) = 'kidney' then 4
            when lower(dim_transplant_class.transplant_class_nm) = 'hand' then 5
            else 6 end as organ_id,
        {{dbt_utils.surrogate_key(['transplant_info.epsd_key','organ_id'])}} as episode_organ_key,
        stg_patient.mrn,
        stg_patient.patient_name,
        transplant_info.summary_block_id,
        epsd_status.epsd_stat_nm as episode_status,
        episode_type.dict_nm as recipient_donor,
        current_stage.dict_nm as curr_stage,
        current_status.dict_nm as phoenix_episode_status,
        dict_hist_center.dict_nm as historical_transplant_center,
        dict_donor_rl.dict_nm as donor_relation,
        date(transplant_info.transplnt_rfl_dt) as referral_date,
        date(transplant_info.transplnt_eval_dt) as evaluation_date,
        date(transplant_info.transplnt_cntr_wtlst_dt) as center_waitlist_date,
        date(transplant_info.transplnt_surg_dt) as transplant_date,
        max(transplant_info.transplnt_surg_dt)
            over (partition by transplant_info.pat_key)
            as most_recent_transplant_date,
        min(transplant_info.transplnt_surg_dt)
            over (partition by transplant_info.pat_key)
            as first_transplant_date,
        reason_dict.dict_nm as reason_removed,
        date(transplant_info.transplnt_cur_stage_dt) as transplant_current_stage_update_date,
        max(
            case
                when
                    lower(current_status.dict_nm) = 'not followed'
                    then date(transplant_current_stage_update_date) else null
            end
        ) over (partition by transplant_info.epsd_key) as not_followed_date,
        dim_transplant_protocol.transplant_ptcl_nm as transplant_protocol_name,
        stg_patient.deceased_ind,
        date(stg_patient.death_date) as death_date,
        max(
            case
                when
                    date(stg_patient.death_date)
                    <= (date(transplant_info.transplnt_surg_dt) + cast('1 year' as interval))
                    then 1 else 0
            end
        ) over (partition by transplant_info.epsd_key) as death_less_than_1_year_post_transplant_ind,
        max(
            case
                when
                    date(stg_patient.death_date)
                    <= (date(transplant_info.transplnt_surg_dt) + cast('3 year' as interval))
                    then 1 else 0
            end
        )  over (partition by transplant_info.epsd_key) as death_less_than_3_year_post_transplant_ind
    from
        {{source('cdw', 'transplant_info')}} as transplant_info
        left join {{source('cdw', 'transplant_class')}} as transplant_class
            on transplant_class.epsd_key = transplant_info.epsd_key
        left join {{source('cdw', 'dim_transplant_class')}} as dim_transplant_class
            on transplant_class.dim_transplant_class_key = dim_transplant_class.dim_transplant_class_key
        left join {{source('cdw', 'cdw_dictionary')}} as current_status
            on current_status.dict_key = transplant_info.dict_transplnt_curr_stat_key
        left join {{source('cdw', 'cdw_dictionary')}} as current_stage
            on current_stage.dict_key = transplant_info.dict_transplnt_curr_stage_key
        left join {{source('cdw', 'cdw_dictionary')}} as episode_type
            on episode_type.dict_key = transplant_info.dict_transplnt_epsd_type_key
        left join {{source('cdw', 'cdw_dictionary')}} as reason_dict
            on reason_dict.dict_key = transplant_info.dict_transplnt_curr_rsn_key
        left join {{source('cdw', 'dim_episode_status')}} as epsd_status
            on epsd_status.dim_epsd_stat_key = transplant_info.dim_epsd_stat_key
        left join {{ref('stg_patient')}} as stg_patient
            on stg_patient.pat_key = transplant_info.pat_key
        left join {{source('cdw', 'transplant_protocol')}} as transplant_protocol
            on transplant_info.epsd_key = transplant_protocol.epsd_key
        left join {{source('cdw', 'dim_transplant_protocol')}} as dim_transplant_protocol
            on transplant_protocol.dim_transplant_ptcl_key = dim_transplant_protocol.dim_transplant_ptcl_key
        left join {{source('cdw', 'cdw_dictionary')}} as dict_hist_center
            on transplant_info.dict_transplnt_hist_cntr_key = dict_hist_center.dict_key
        left join {{source('cdw', 'transplant_organs')}} as transplant_organs
            on transplant_organs.epsd_key = transplant_info.epsd_key
        left join {{source('cdw', 'organ')}} as organ
            on organ.orgn_rec_key = transplant_organs.orgn_rec_key
        left join {{source('cdw', 'cdw_dictionary')}} as dict_donor_rl
            on organ.dict_transplnt_dnr_rel_key = dict_donor_rl.dict_key
    where
        lower(recipient_donor) = 'recipient'
        and lower(episode_status) != 'deleted'
),
surgery_info as (
    select
        surgery_procedure.pat_key,
        min(date(surgery_procedure.surgery_date)) as surgery_date,
        transplant_info.epsd_key
    from
        {{ref('surgery_procedure')}} as surgery_procedure
        inner join transplant_info
            on transplant_info.pat_key = surgery_procedure.pat_key
            and days_between(date(transplant_info.transplant_date), date(surgery_procedure.surgery_date)) <= 1
    where
        surgery_procedure.or_proc_id in (
            '628.003', --liver, orthotopic liver transplant, any type/age	
            '811.003', --transplant, heart and lung	
            '812.003', --transplant, heart	
            '929.003', --transplant, lung(s)	
            '1686.003', --kidney, transplant, cadaveric	
            '1687.003', --kidney, transplant, living related	
            '1688.003', --kidney transplant- living related with nephrectomy	
            '1689.003', --kidney, transplant, cadaveric with nephrectomy	
            '6537', --kidney, transplant, cadaveric	
            '6768', --liver, orthotopic liver transplant, any type/age	
            '6896', --kidney transplant - without recipient nephrectomy	
            '7680', --transplant, heart	
            '7681', --transplant, heart and lung	
            '7682', --transplant, lung(s)	
            '8849') --hand transplant	
    group by
        surgery_procedure.pat_key,
        transplant_info.epsd_key
)
select
    transplant_info.*,
    surgery_info.surgery_date
from
    transplant_info
    left join surgery_info
        on transplant_info.epsd_key = surgery_info.epsd_key
