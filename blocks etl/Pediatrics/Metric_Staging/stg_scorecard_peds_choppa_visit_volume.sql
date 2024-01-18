select
    tdl_tran.tdl_id as primary_key,
    tdl_tran.post_date,
    tdl_tran.procedure_quantity,
    case
        when lower(tdl_tran.cpt_code) in ('92002', '92004', '99201', '99202', '99203', '99204', '99205', '99241',
                '99242', '99243', '99244', '99245', '99381', '99382', '99383', '99384', '99385', '99499', 's0620')
            then 'op new visits'
        when lower(tdl_tran.cpt_code) in ('92012', '92014', '99211', '99212', '99213', '99214', '99215', '99391',
                '99392', '99393', '99394', '99395', 's0621')
            then 'op established visits' else 'ip admissions'
        end as op_ip_flag,
    case
        when (lower(clarity_pos.pos_name) like '%telehealth%'
            or lower(tdl_tran.modifier_one) in ('95', 'gt', 'gq', 'td')
            or lower(tdl_tran.modifier_two) in ('95', 'gt', 'gq', 'td')
            or lower(tdl_tran.modifier_three) in ('95', 'gt', 'gq', 'td')
            or lower(tdl_tran.modifier_four) in ('95', 'gt', 'gq', 'td'))
            then 'telehealth' else 'in person'
        end as telvsip,
    case
        when lower(zc_dep_rpt_grp_15.name) = 'chop main' then 'chop main'
        when lower(zc_dep_rpt_grp_15.name) = 'chop affiliated' then 'chop affiliated'
        when lower(zc_dep_rpt_grp_15.name) = 'other' then 'other'
        when lower(zc_dep_rpt_grp_15.name) in ('chop koph', 'chop - koph')
        then 'chop koph' else 'chop scc' end as chop_scc_flag,
    case
        when op_ip_flag in ('op new visits', 'op established visits')
        then procedure_quantity else 0 end as total_visits,
    case
        when op_ip_flag in ('op new visits', 'op established visits') and telvsip = 'telehealth'
        then procedure_quantity else 0 end as total_telehealth_visits,
    case
        when op_ip_flag in ('op new visits', 'op established visits') and telvsip = 'in person'
        then procedure_quantity else 0 end as total_inperson_visits,
    case
        when op_ip_flag = 'op new visits'
        then procedure_quantity else 0 end as new_visits,
    case
        when op_ip_flag = 'op established visits'
        then procedure_quantity else 0 end as established_visits,
    case
        when op_ip_flag = 'op new visits' and telvsip = 'telehealth'
        then procedure_quantity else 0 end as new_telehealth_visits,
    case
        when op_ip_flag = 'op new visits' and telvsip = 'in person'
        then procedure_quantity else 0 end as new_inperson_visits,
    case
        when op_ip_flag = 'op established visits' and telvsip = 'telehealth'
        then procedure_quantity else 0 end as established_telehealth_visits,
    case
        when op_ip_flag = 'op established visits' and telvsip = 'in person'
        then procedure_quantity else 0 end as established_inperson_visits,
    case
        when op_ip_flag = 'ip admissions' and (lower(zc_dep_rpt_grp_15.name) like '%chop%'
        or lower(zc_dep_rpt_grp_15.name) in ('other', 'spuh')) then procedure_quantity else 0 end as ip_admissions,
    case
        when chop_scc_flag = 'chop scc' and op_ip_flag in ('op new visits', 'op established visits')
        then procedure_quantity else 0 end as scc_growth
from
    {{ref('stg_all_transactions')}} as tdl_tran
    left join {{source('clarity_ods', 'clarity_pos')}} as clarity_pos
        on clarity_pos.pos_id = tdl_tran.pos_id
    left join {{source('clarity_ods', 'clarity_dep')}} as clarity_dep
        on clarity_dep.department_id = tdl_tran.dept_id
    left join {{source('clarity_ods', 'zc_dep_rpt_grp_10')}} as zc_dep_rpt_grp_10
        on clarity_dep.rpt_grp_ten = zc_dep_rpt_grp_10.internal_id
    left join {{source('clarity_ods', 'zc_dep_rpt_grp_15')}} as zc_dep_rpt_grp_15
        on clarity_dep.rpt_grp_fifteen_c = zc_dep_rpt_grp_15.rpt_grp_fifteen_c
where
    tdl_tran.post_date >= '01/01/2019'
    and tdl_tran.detail_type in (1, 10) -- 1 - new charge, 10 - voided charge
    and tdl_tran.loc_id in (1012, 1022) -- chca nj rl, chca pa rl locations
    and lower(zc_dep_rpt_grp_10.name) != 'emergency medicine'
    and lower(tdl_tran.cpt_code) in ('92002', '92004', '99201', '99202', '99203', '99204', '99205',
        '99241', '99242', '99243', '99244', '99245', '99381', '99382', '99383', '99384', '99385', 's0620',
        '92012', '92014', '99211', '99212', '99213', '99214', '99215', '99391', '99392', '99393', '99394',
        '99395', '99499', 's0620', 's0621', -- new and established op visits cpt codes
        '99217', '99218', '99219', '99220', '99224', '99225', '99226', '99234', '99235', '99236', '99221',
        '99222', '99223', '99231', '99232', '99233', '99238', '99239', '99251', '99252', '99253', '99254',
        '99255', '99291', '99460', '99461', '99462', '99463', '99468', '99469', '99471', '99472', '99476',
        '99477', '99478', '99479', '99480')-- ip admission cpt codes
