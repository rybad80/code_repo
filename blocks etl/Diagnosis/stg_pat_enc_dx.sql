with dx_edit as (
    select distinct
        pat_enc_csn_id,
        dx_edit_diag_id,
        max(line) over (
            partition by
                pat_enc_csn_id,
                    dx_edit_diag_id
        ) as line,
        max(dx_edit_inst) over (
            partition by
                pat_enc_csn_id,
                dx_edit_diag_id
        ) as dx_edit_inst
    from
        {{ source('clarity_ods', 'enc_dx_edit_trail') }}
),

ed_event as (
    select distinct
        pat.pat_enc_csn_id,
        min(
                case
                    when evnt.event_type = '95' then evnt.event_time
                end
            ) over (partition by pat.pat_enc_csn_id) as ed_depart
    from
        {{ source('clarity_ods', 'ed_iev_event_info') }} as evnt
        inner join {{ source('clarity_ods', 'ed_iev_pat_info') }} as pat on evnt.event_id = pat.event_id
    where
        evnt.event_type = '95'
),

lkp_vis_event1 as (
    select distinct -- noqa: L036
        ed_iev_event_info.event_source_csn_id as pat_enc_csn_id
    from
        {{source('clarity_ods','ed_iev_event_info')}} as ed_iev_event_info
        inner join {{source('clarity_ods','pat_enc_hsp')}} as pat_enc_hsp
            on ed_iev_event_info.event_source_csn_id = pat_enc_hsp.pat_enc_csn_id
            and pat_enc_hsp.ed_disposition_c is not null
    where
        ed_iev_event_info.event_type = 95 -- 'ED DEPART'
        and ed_iev_event_info.event_time is not null
),
lkp_vis_event2 as (
    select distinct -- noqa: L036
        ed_iev_event_info.event_source_csn_id as pat_enc_csn_id
    from
        {{source('clarity_ods','ed_iev_event_info')}} as ed_iev_event_info
        inner join {{source('clarity_ods','pat_enc_hsp')}} as pat_enc_hsp
            on ed_iev_event_info.event_source_csn_id = pat_enc_hsp.pat_enc_csn_id
            and pat_enc_hsp.ed_disposition_c is not null
    where
        ed_iev_event_info.event_id = 50 -- 'ED ARRIVED'
        and ed_iev_event_info.event_time is not null
),

stage as (
    select
        pat_enc_dx.pat_enc_csn_id,
        pat_enc_dx.dx_id,
        pat_enc_dx.dx_ed_yn,
        case
            when pat_enc_dx.dx_ed_yn = 'Y' then 'Y'
            when ed_event.ed_depart >= dx_edit.dx_edit_inst then 'Y'
            else 'N'
        end as derived_dx_ed_yn,
        pat_enc_dx.primary_dx_yn,
        pat_enc_dx.line,
        pat_enc_dx.annotation,
        case when pat_enc_dx.contact_date < cast('2011-01-01 00:00:00' as timestamp) then 1 else 0 end as asap_ind
    from
        {{source('clarity_ods','pat_enc_dx')}} as pat_enc_dx
        left join dx_edit
            on pat_enc_dx.pat_enc_csn_id = dx_edit.pat_enc_csn_id
            and pat_enc_dx.dx_id = dx_edit.dx_edit_diag_id
        left join ed_event
            on ed_event.pat_enc_csn_id = pat_enc_dx.pat_enc_csn_id
    where
        pat_enc_dx.contact_date >= to_date('07/01/2007', 'mm/dd/yyyy')
)

select
    visit.visit_key,
    visit.pat_key,
    stage.pat_enc_csn_id,
    stage.dx_id,
    stage.dx_ed_yn,
    derived_dx_ed_yn,
    lkp_vis_event2.pat_enc_csn_id as event1_pat_enc_csn_id,
    lkp_vis_event1.pat_enc_csn_id as event2_pat_enc_csn_id,
    stage.primary_dx_yn,
    stage.line,
    stage.annotation,
    stage.asap_ind,
     case
		when asap_ind = 1
		then
			case
				when dx_ed_yn = 'Y'
				then 1
				when lkp_vis_event1.pat_enc_csn_id  is not null and lkp_vis_event2.pat_enc_csn_id is not null
				then 1
				when visit_ed_extended.visit_key is not null
				then 1
				else 0
			end
		when asap_ind = 0 and derived_dx_ed_yn = 'Y'
		then 1
		else 0
	end as ed_dx_flag_pre_asap,
	case
		when primary_dx_yn = 'Y' and ed_dx_flag_pre_asap =  1 then 'ED Primary'
		when primary_dx_yn = 'N' and ed_dx_flag_pre_asap = 1 then 'ED Other'
		when primary_dx_yn = 'Y' and ed_dx_flag_pre_asap !=  1 then 'Visit Primary'
		else 'Visit Other'
	end as dx_status
from
    stage
    left join {{source('cdw','visit')}} as visit on
            case
                when stage.pat_enc_csn_id is not null
                then stage.pat_enc_csn_id
                when 0 is not null
                then cast('0' as int8)
                else null::int8 -- noqa: L067
            end = visit.enc_id
    left join lkp_vis_event1 on visit.enc_id = lkp_vis_event1.pat_enc_csn_id
    left join lkp_vis_event2 on visit.enc_id = lkp_vis_event2.pat_enc_csn_id
    left join {{source('cdw', 'visit_ed_extended')}} as visit_ed_extended
        on visit.visit_key = visit_ed_extended.visit_key
