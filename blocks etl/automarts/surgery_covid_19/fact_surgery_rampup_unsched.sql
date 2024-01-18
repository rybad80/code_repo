with timeframe as ( --noqa: PRS
    select
        or_case_order.ord_key,
        case when order_question.ansr = 900 then 'Urgent'
            when order_question.ansr = 901 then 'Add-On Case - Same Day or within 24 hrs'
            when order_question.ansr = 902 then '< 1 Week'
            when order_question.ansr = 903 then '5-7 Days'
            when order_question.ansr = 904 then '2-4 Weeks'
            when order_question.ansr = 905 then '< 3 Months'
            when order_question.ansr = 906 then '> 3 Months'
            when order_question.ansr = 907 then 'Next Available'
            when order_question.ansr = 908 then 'Parent to Call'
            when order_question.ansr = 909 then 'Add-On Case - Friday for Next Business Day'
            when order_question.ansr = 910 then '> 1 Week'
            else null
        end as order_timeframe
    from
        {{ source('cdw', 'or_case_order') }} as or_case_order
        inner join {{ source('cdw', 'order_question') }} as order_question
            on order_question.ord_key = or_case_order.ord_key
        inner join {{ source('cdw', 'master_question') }} as master_question
            on master_question.quest_key = order_question.quest_key
    where
        quest_id in ('900100128', '900100129', '900100139')
),

orc_surgeon as (
    select
        orc_surgeon.or_case_key,
        orc_surgeon_nm.full_nm as case_surgeon_primary
    from
        {{ source('cdw', 'or_case_all_surgeons') }} as orc_surgeon
        inner join {{ source('cdw', 'cdw_dictionary') }} as dict_or_panel_role
            on dict_or_panel_role.dict_key = orc_surgeon.dict_or_panel_role_key
        inner join
            {{ source('cdw', 'provider') }} as orc_surgeon_nm
                on orc_surgeon_nm.prov_key = orc_surgeon.surg_prov_key

    where
        dict_or_panel_role.src_id in (1.0000, 1.0030)
        and orc_surgeon.panel_num = 1

    group by
        orc_surgeon.or_case_key,
        orc_surgeon_nm.full_nm
),

proc_primary as ( --region primary procedure

	with procs as ( --region all proces

		select
		orc_proc.or_case_key,
		max(orc_proc.seq_num) as cnt_procs,
		min(case when orc_surgeon.panel_num = 1 then orc_proc.seq_num end) as min_proc_1st_panel

		from {{ source('cdw', 'or_case_all_procedures') }} as orc_proc
		inner join
            {{ source('cdw', 'or_case_all_surgeons') }} as orc_surgeon on
                orc_surgeon.or_case_key = orc_proc.or_case_key

		where orc_surgeon.panel_num = orc_proc.panel_num

		group by
		orc_proc.or_case_key

		--end region
    )

select
    orc_proc.or_case_key,
    or_procedure.or_proc_nm as proc_primary

from
    {{ source('cdw', 'or_case_all_procedures') }} as orc_proc
    inner join
        {{ source('cdw', 'or_procedure') }} as or_procedure
            on or_procedure.or_proc_key = orc_proc.or_proc_key
    inner join
        {{  source('cdw', 'or_case_all_surgeons') }} as orc_surgeon
            on orc_surgeon.or_case_key = orc_proc.or_case_key
    inner join procs on procs.or_case_key = orc_proc.or_case_key

where
    orc_surgeon.panel_num = 1
    and orc_proc.seq_num = procs.min_proc_1st_panel
    and orc_surgeon.panel_num = orc_proc.panel_num

group by
    orc_proc.or_case_key,
    or_procedure.or_proc_nm

--endregion
),

created as ( --region creation date
    select
        or_case_audit_hist.or_case_key,
        min(or_case_audit_hist.audit_act_dt) as created_dt

    from
        {{ source('cdw', 'or_case_audit_history') }} as or_case_audit_hist
        inner join {{ source('cdw', 'dim_or_audit_action') }} as dim_or_audit_action
            on dim_or_audit_action.dim_or_audit_act_key = or_case_audit_hist.dim_or_audit_act_key

    where
        lower(or_audit_act_nm) = 'creation'

    group by
        or_case_audit_hist.or_case_key

--end region
),

surg_pred as ( -- region expected LOS in order

select
or_case.or_case_id,
m_quest.latest_display_quest_nm,
m_quest.quest_nm,
case when ansr = 100 then '< 23 Hours'
      when ansr = 105 then '23 - 48 Hours'
      when ansr = 110 then '48 - 72 Hours'
      when ansr = 115 then '> 72 Hours'
      else null end as questionnaire_los

from {{ source('cdw', 'order_question') }} as ord_quest
inner join {{  source('cdw', 'master_question') }} as m_quest on ord_quest.quest_key = m_quest.quest_key
inner join {{ source('cdw', 'order_xref') }} as ord_x on ord_x.ord_key = ord_quest.ord_key
inner join {{ source('cdw', 'or_case_order') }} as orc_order on orc_order.ord_key = ord_quest.ord_key
inner join {{ source('cdw', 'or_case') }} as or_case on orc_order.or_case_key = or_case.or_case_key

where
m_quest.quest_id = '900100174' --CHOP OPT EXPECTED LENGTH OF STAY

--end region
)

select
procedure_order_clinical.procedure_name,
cdw_dictionary.dict_nm as status,
or_case.or_case_id,
or_case.pat_key,
stg_patient.mrn,
patient.zip,
patient.county,
stg_patient.patient_name,
stg_patient.current_age,
or_case.sched_dt,
location.loc_nm,
dict_svc.dict_nm as service,
orc_surgeon.case_surgeon_primary,
or_case.tot_tm_needed,
proc_primary.proc_primary,
dict_postop_dest.dict_nm as postop_dest,
surg_pred.questionnaire_los,
dict_priority.dict_nm as order_priority,
timeframe.order_timeframe,
dict_time.dict_nm as case_timeframe,
procedure_order_clinical.placed_date,
created.created_dt,
coalesce(procedure_order_clinical.placed_date, created.created_dt) as index_date,
days_between(current_date, index_date) as wait_time,

--values here come from 75th percentile times in `timeframe_avgs.sql`
case
    when
        lower(
            case_timeframe
        ) in (
            'urgent', 'add-on case - same day or within 24 hrs', 'Add-On Case - Friday for Next Business Day'
        )  then 2
      when lower(case_timeframe) in ('< 1 week', '5-7 days') then 9
      when lower(case_timeframe) = 'not applicable' then 46
      when lower(case_timeframe) = '> 1 week' then 22
      when lower(case_timeframe) = '2-4 weeks' then 35
      when lower(case_timeframe) = 'next available' then 60
      when lower(case_timeframe) = '< 3 months' then 81
      when lower(case_timeframe) = 'parent to call' then 97
      when lower(case_timeframe) = '> 3 months' then 164
      else null end as mtbt,

case when wait_time = 0 then (164 / mtbt) else wait_time * (164 / mtbt) end as priority


from
    {{ source('cdw', 'or_case') }} as or_case
    inner join {{ source('cdw', 'cdw_dictionary') }} as cdw_dictionary
        on cdw_dictionary.dict_key = or_case.dict_or_sched_stat_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_time
        on dict_time.dict_key = or_case.dict_or_case_type_key
    inner join {{ source('cdw', 'cdw_dictionary') }} as dict_svc
        on dict_svc.dict_key = or_case.dict_or_svc_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_postop_dest on
            dict_postop_dest.dict_key = or_case.dict_or_post_dest_key
    inner join
        {{ source('cdw', 'cdw_dictionary') }} as dict_priority
            on dict_priority.dict_key = or_case.dict_or_prty_key
    inner join {{ source('cdw', 'location') }} as location --noqa: L029
        on location.loc_key = or_case.loc_key
    left join {{ ref('stg_patient') }} as stg_patient  on stg_patient.pat_key = or_case.pat_key
    left join {{ source('cdw', 'patient') }} as patient on patient.pat_key = or_case.pat_key
    left join {{ source('cdw', 'or_case_order') }} as or_case_order
        on or_case_order.or_case_key = or_case.or_case_key
    left join {{ source('cdw', 'procedure_order') }} as proc_order
        on proc_order.proc_ord_key = or_case_order.ord_key
    left join {{ ref('procedure_order_clinical') }} as procedure_order_clinical
        on procedure_order_clinical.proc_ord_key = proc_order.proc_ord_key
    left join timeframe on timeframe.ord_key = or_case_order.ord_key
    left join orc_surgeon on orc_surgeon.or_case_key = or_case.or_case_key
    left join proc_primary on proc_primary.or_case_key = or_case.or_case_key
    left join created on created.or_case_key = or_case.or_case_key
    left join surg_pred on surg_pred.or_case_id = or_case.or_case_id

where
    lower(status) in ('not scheduled', 'missing information', 'pending unscheduled')
    --and lower(loc_nm) not in ('cardiac operative imaging complex', 'special delivery unit')
    --and (date(placed_date) >= '2019-07-01' or placed_date is null)
    and date(index_date) >= '2020-01-01'
