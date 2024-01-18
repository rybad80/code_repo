with max_note as (
    select
        note_edit_metadata_history.vsi_key,
        note_edit_metadata_history.note_id
    from {{ref('note_edit_metadata_history')}} as note_edit_metadata_history
    where
        note_edit_metadata_history.last_edit_ind = 1
        and note_edit_metadata_history.note_type_id = '5' -- discharge summary
        and note_edit_metadata_history.note_entry_date >= to_date('01/01/2019', 'mm/dd/yyyy')
        and note_edit_metadata_history.note_status_id in (
            '2', --signed
            '3', --addendum
            '4', --deleted
            '9', --cosign needed
            '11') --cosign needed addendum
),

inpatient_notes as (
    select
        note_info.pat_key,
        note_info.visit_key,
        note_info.note_key
    from max_note as max_note
        inner join {{source('cdw', 'note_info')}} as note_info
        on max_note.vsi_key = note_info.vsi_key
        and max_note.note_id = note_info.note_id
),

note_information_rank as (
    select
        b.*,
        rank()
            over(partition by b.note_key, b.note_act_nm order by b.note_key, b.note_act_nm, b.note_date)
            as note_rank
    from (
        select
            inpatient_notes.visit_key,
            dim_note_action.note_act_id,
            dim_note_action.note_act_nm,
            dim_routing_method.route_meth_nm,
            inpatient_notes.note_key,
            note_history.seq_num,
            note_history.route_rcpt_nm,
            note_history.route_rcpt_fax,
            note_history.route_rcpt_addr,
            note_history.route_rcpt_city,
            note_history.note_act_local_dt as note_date,
            case when note_history.route_rcpt_prov_key is null or note_history.route_rcpt_prov_key < 1
                then employee.prov_key else note_history.route_rcpt_prov_key end as prov_key,
            patient.prov_key as pcp_prov_key,
            case when lower(dim_note_action.note_act_nm) = 'cosign' then employee_cs.full_nm end as cosign_name
        from inpatient_notes as inpatient_notes
            inner join {{source('cdw', 'note_history')}} as note_history
                on inpatient_notes.note_key = note_history.note_key
            inner join {{source('cdw', 'dim_note_action')}} as dim_note_action
                on note_history.dim_note_act_key = dim_note_action.dim_note_act_key
            inner join {{source('cdw', 'dim_routing_method')}} as dim_routing_method
                on note_history.dim_route_meth_key = dim_routing_method.dim_route_meth_key
            left join {{source('cdw', 'employee')}} as employee
                on note_history.route_rcpt_emp_key = employee.emp_key
            left join {{source('cdw', 'patient')}} as patient
                on inpatient_notes.pat_key = patient.pat_key
            left join {{source('cdw', 'employee')}} as employee_cs
                on note_history.act_emp_key = employee_cs.emp_key
        where
            dim_note_action.note_act_id in (
                    '2',     --sign
                    '6',     --addend / edit transcription
                    '7',     --cosign
                    '12')     --route    
            and note_history.note_act_local_dt >= to_date('01/01/2019', 'mm/dd/yyyy')
        ) as b
    where b.prov_key = b.pcp_prov_key or b.note_act_id in ('2', '6', '7')
),

note_information as (
       select
        note_information_rank.visit_key,
        max(case when lower(note_information_rank.note_act_nm) = 'route'
            then note_information_rank.route_meth_nm else null end) as rout_method,
        max(case when lower(note_information_rank.note_act_nm) = 'route'
            then note_information_rank.prov_key else null end) as routing_prov_id,
        max(case when lower(note_information_rank.note_act_nm) = 'route'
            then note_information_rank.route_rcpt_nm else null end) as rout_recip_name,
        max(case when lower(note_information_rank.note_act_nm) = 'route'
            then note_information_rank.route_rcpt_fax else null end) as rout_recip_fax,
        max(case when lower(note_information_rank.note_act_nm) = 'route'
            then note_information_rank.route_rcpt_addr else null end) as rout_recip_addr,
        max(case when lower(note_information_rank.note_act_nm) = 'route'
            then note_information_rank.route_rcpt_city else null end) as rout_recip_city,
        max(case when lower(note_information_rank.note_act_nm) = 'route'
            then note_information_rank.note_date else null end) as rout_note_date,
        max(case when lower(note_information_rank.note_act_nm) = 'sign'
            then note_information_rank.note_date else null end) as sign_note_date,
        max(case when lower(note_information_rank.note_act_nm) = 'cosign'
            then note_information_rank.note_date else null end) as cosign_note_date,
        max(case when lower(note_information_rank.note_act_nm) = 'cosign'
            then note_information_rank.cosign_name else null end) as cosign_name,
        max(case when lower(note_information_rank.note_act_nm) = 'addend / edit transcription'
            then note_information_rank.note_date else null end) as addend_note_date,
        min(case when lower(note_information_rank.note_act_nm)
            in ('route', 'cosign', 'addend / edit transcription')
            then note_information_rank.note_date else null end) as master_note_date
    from
        note_information_rank as note_information_rank
    where
        note_information_rank.note_rank = 1
    group by
        note_information_rank.visit_key
),

most_recent_attending as (
    select
        visit_provider_hist.visit_key,
        max(visit_provider_hist.seq_num) as max_seq_num
    from
        {{source('cdw', 'visit_provider_hist')}} as visit_provider_hist
    group by
        visit_provider_hist.visit_key
),

attending_prov as (
    select
        visit_provider_hist.visit_key,
        visit_provider_hist.prov_key,
        provider.full_nm as attending_prov,
        provider.prov_id,
        provider.npi
    from
        most_recent_attending as most_recent_attending
        inner join {{source('cdw', 'visit_provider_hist')}} as visit_provider_hist
            on most_recent_attending.visit_key = visit_provider_hist.visit_key
            and most_recent_attending.max_seq_num = visit_provider_hist.seq_num
        inner join {{source('cdw', 'provider')}} as provider
            on visit_provider_hist.prov_key = provider.prov_key
)

select
    visit.enc_id as primary_key, --csn,
    visit.hosp_dischrg_dt as disch_dt,
    att.prov_key,
    att.attending_prov,
    att.prov_id,
    att.npi,
    round(((cast(extract(epoch from (note_information.master_note_date - visit.hosp_dischrg_dt))
        as numeric(38, 2)) / 3600) / 24), 2) as total_turnaround_days,
    case when round(((cast(extract(epoch from (note_information.master_note_date - visit.hosp_dischrg_dt))
        as numeric(38, 2)) / 3600) / 24), 2) < 11 then 1 else 0 end as numerator,
    1 as denominator,
    'kpi_peds_discharge_summaries' as metric_id
from
    {{source('cdw', 'visit')}} as visit
    inner join {{source('cdw', 'patient')}} as patient on visit.pat_key = patient.pat_key
    inner join {{source('cdw', 'department')}} as department on visit.eff_dept_key = department.dept_key
    inner join {{source('cdw', 'hospital_account_visit')}} as hav on visit.visit_key = hav.visit_key
    inner join {{source('cdw', 'hospital_account')}} as har on hav.hsp_acct_key = har.hsp_acct_key
    inner join {{source('cdw', 'cdw_dictionary')}} as svc on har.dict_pri_svc_key = svc.dict_key
    inner join {{source('cdw', 'cdw_dictionary')}} as pat on har.dict_acct_class_key = pat.dict_key
    inner join {{source('cdw', 'provider')}} as pcp on patient.prov_key = pcp.prov_key
    left join note_information as note_information on visit.visit_key = note_information.visit_key
    left join attending_prov as att on visit.visit_key = att.visit_key
    left join {{source('cdw', 'provider')}} as ref on har.ref_prov_key = ref.prov_key
where
    lower(pat.dict_cat_nm) = 'clarity_acct_class_ha'
    and pat.src_id not in ('2', '4', '6') --exclude outpatient, day surgery, and recurring outpatient
    and department.dept_id not in ('83417099', '10201512', '80111001', '89478031', '101001047', '101001046',
        '1001153', '10292012', '10011051', '10270011', '101001022', '89393046', '10421099', '89487047',
        '101001073', '101001069', '82424099', '89367044', '101001077', '10011055', '62')
    and date_trunc('day', visit.hosp_dischrg_dt) between '01/01/2019' and (date_trunc('day', current_date) - 10)
    and pcp.prov_id not in (
        '26',    --provider, family declined
        '1000',    --provider, unknown
        '532245',    --provider, family states no pcp
        '532246',    --provider, information not available
        '2000000',    --provider, not in system
        '2000001',    --provider, self referred
        '16298',    --international patient services, provider
        '0' --unknown provider
    )
    and (lower(pcp.prov_type) != 'resource' or pcp.prov_type is null)
    and lower(svc.dict_nm) not in (
        'not applicable',
        'obstetrics',
        'dentistry',
        'cardiovascular radiology',
        'other',
        'emergency',
        'oral and maxillofacial surgery',
        'trauma'
    )
    and (lower(patient.country) != 'international' or patient.country is null)
    and lower(pcp.full_nm) not like '%karabots%'
