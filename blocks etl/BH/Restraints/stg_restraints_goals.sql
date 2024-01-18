select
        pt_goals_info.pat_id,
		goal.goal_id,
        goal.created_time as goal_create_dttm,
        max(coalesce(goal_contact.end_date, goal_contact.contact_date)) as clty_last_edit_dt
    from {{ source('clarity_ods','pt_goals_info') }} as pt_goals_info
        inner join {{ source('clarity_ods', 'goal') }} as goal
            on pt_goals_info.goal_id = goal.goal_id
        left join {{ source('clarity_ods', 'goal_contact') }} as goal_contact --goal metadata history
            on goal.goal_id = goal_contact.goal_id
        left join {{ source('clarity_ods', 'zc_ip_cp_igo_160') }} as zc_ip_cp_igo_160
            on goal_contact.goal_outcome_c = zc_ip_cp_igo_160.ip_cp_igo_160_c
    where
        --Patient will remain free of injury from restraints (Violent or Psychiatric Emergency
        goal.goal_type_id = 304800553
    group by
        pt_goals_info.pat_id,
        goal.goal_id,
        goal.created_time
