with
vad_or_tran_pat as (
    select mrn,
        max(heart_tx_ind) as heart_tx_ind,
        max(vad_implant_ind) as vad_implant_ind
    from {{ ref('cardiac_surgery') }}
    where heart_tx_ind = 1 or vad_implant_ind = 1
    group by mrn
),
dx_pat as (
    select mrn,
        max(case when (lower(echo_diagnosis) like '%cardiomyopathy%'
            and regexp_like(echo_diagnosis, 'dilated|hypertrophic|restrictive', 'i')) then 1 else 0 end)
            as cardiomyopathy_ind,
        max(case when lower(echo_diagnosis) like '%heart failure%' then 1 else 0 end) as hf_ind,
        max(case when lower(echo_diagnosis) like '%alcapa%' then 1 else 0 end) as alcapa_ind,
        max(case when regexp_like(echo_diagnosis, 'ventricular dysfunction', 'i') then 1 else 0 end) as vd_ind,
        max(case when regexp_like(echo_diagnosis, 's/p|status post', 'i')
            and regexp_like(echo_diagnosis, 'vad|ventricular assist device', 'i') then 1 else 0 end) as sp_vad_ind,
        max(case when (regexp_like(echo_diagnosis, 's/p|status post', 'i')
                and (lower(echo_diagnosis) like '%heart%transplant%'
                or lower(echo_diagnosis) like '%transplant%heart%'))
            or (mapped_diagnosis = 'STATUS POST HEART TRANSPLANT'
                and (lower(echo_diagnosis) like '%heart%transplant%'
                or lower(echo_diagnosis) like '%transplant%heart%'))
            then 1 else 0 end) as sp_heart_tx_ind
    from {{ ref('cardiac_diagnosis_echo') }}
    where
        (lower(echo_diagnosis) like '%cardiomyopathy%'
            and regexp_like(echo_diagnosis, 'dilated|hypertrophic|restrictive', 'i'))
        or lower(echo_diagnosis) like '%heart failure%'
        or lower(echo_diagnosis) like '%alcapa%' --Anomalous Left Coronary Artery from the Pulmonary Artery
        or regexp_like(echo_diagnosis, 'ventricular dysfunction', 'i')
        or (regexp_like(echo_diagnosis, 's/p|status post', 'i')
            and regexp_like(echo_diagnosis, 'vad|ventricular assist device', 'i'))
        or (regexp_like(echo_diagnosis, 's/p|status post', 'i')
            and (lower(echo_diagnosis) like '%heart%transplant%'
                or lower(echo_diagnosis) like '%transplant%heart%'))
        or (mapped_diagnosis = 'STATUS POST HEART TRANSPLANT'
            and (lower(echo_diagnosis) like '%heart%transplant%'
            or lower(echo_diagnosis) like '%transplant%heart%'))
    group by
        mrn
    having
        (alcapa_ind = 1 and vd_ind = 1)
        or cardiomyopathy_ind = 1
        or hf_ind = 1
        or sp_vad_ind = 1
        or sp_heart_tx_ind = 1
),
proc_result_dx as (
    select
        poc.mrn,
        max(case when regexp_like(pon.ord_narr,
            'severe dilatation of the (left|right) ventricle|
            |severe dilatation of the (lv|rv)|
            |severe (left|right) ventricular dysfunction|
            |severe (lv|rv) dysfunction|
            |severely diminished (lv|rv) systolic shortening|
            |severely diminished (left|right) ventricular systolic shortening', 'i') then 1 else 0 end) as severe_vd_ind,
        max(case when regexp_like(pon.ord_narr, '\bsevere TR\b', 'i') then 1 else 0 end)
            as severe_tr_ind,
        max(case when regexp_like(pon.ord_narr, 'myopericarditis', 'i') then 1 else 0 end)
            as myopericarditis_ind,
        max(case when regexp_like(pon.ord_narr, 'acute myocarditis', 'i') then 1 else 0 end)
            as myocarditis_ind
	from {{ ref('procedure_order_clinical') }} as poc
    inner join {{source('cdw', 'procedure_order_narrative')}} as pon
        on pon.proc_ord_key = poc.proc_ord_key
    where
        poc.procedure_id in (
                                                131988, --IP TRANSTHORACIC ECHO NON-SEDATED
                                                131760, -- IP TRANSTHORACIC ECHO ANESTHESIA SEDATED
                                                131758, --IP TRANSESOPHAGEAL ECHO ANESTHESIA SEDATED
                                                109106 -- IP ECHOCARDIOGRAM
                                                )
		and (regexp_like(pon.ord_narr,
                'severe dilatation of the (left|right) ventricle|
                |severe dilatation of the (lv|rv)|
                |severe (left|right) ventricular dysfunction|
                |severe (lv|rv) dysfunction|
                |\bsevere TR\b|
                |myopericarditis|
                |acute myocarditis|
                |severely diminished (lv|rv) systolic shortening|
                |severely diminished (left|right) ventricular systolic shortening', 'i')
		)
	group by poc.mrn
),
tx_waitlist_pat as (
    select mrn,
        max(case when phoenix_episode_status = 'Removed' then 1 else 0 end) as tx_waitlist_removed_ind,
        max(case when phoenix_episode_status = 'Inactive' then 1 else 0 end) as tx_waitlist_inactive_ind,
        max(case when phoenix_episode_status in ('Removed', 'Inactive')
            then transplant_current_stage_update_date else null end)
            as most_recent_removed_date,
        case when most_recent_removed_date is null or most_recent_removed_date >= '11/28/2022' then 1 else 0 end
            as tx_waitlist_active_ind,
        1 as tx_waitlist_ind
    from {{ ref('transplant_recipients') }}
    where organ_id = 1 -- HEART
        and center_waitlist_date is not null and transplant_date is null
        and episode_status = 'ACTIVE' and curr_stage = 'Waitlist'
    group by mrn
)
select p.mrn,
	coalesce(max(dx_pat.cardiomyopathy_ind), 0) as cardiomyopathy_ind,
	coalesce(max(dx_pat.hf_ind), 0) as hf_ind,
	coalesce(max(tx_waitlist_pat.tx_waitlist_ind), 0) as tx_waitlist_ind,
	coalesce(max(tx_waitlist_pat.tx_waitlist_active_ind), 0) as tx_waitlist_active_ind,
	coalesce(max(dx_pat.alcapa_ind), 0) as alcapa_ind,
	coalesce(max(dx_pat.vd_ind), 0) as vd_ind,
	coalesce(max(case when dx_pat.sp_vad_ind = 1 or vad_or_tran_pat.vad_implant_ind = 1 then 1 else 0 end), 0)
        as sp_vad_ind,
	coalesce(max(case when dx_pat.sp_heart_tx_ind = 1 or vad_or_tran_pat.heart_tx_ind = 1 then 1 else 0 end), 0)
        as sp_heart_tx_ind,
	coalesce(max(proc_result_dx.myocarditis_ind), 0) as myocarditis_ind,
	coalesce(max(proc_result_dx.severe_vd_ind), 0) as severe_vd_ind,
	coalesce(max(proc_result_dx.severe_tr_ind), 0) as severe_tr_ind,
	coalesce(max(proc_result_dx.myopericarditis_ind), 0) as myopericarditis_ind
from
	(select mrn from dx_pat
	union all
	select mrn from tx_waitlist_pat
	union all
	select mrn from vad_or_tran_pat
	union all
	select mrn from proc_result_dx
	) as p
left join dx_pat
	on p.mrn = dx_pat.mrn
left join tx_waitlist_pat
	on p.mrn = tx_waitlist_pat.mrn
left join vad_or_tran_pat
	on p.mrn = vad_or_tran_pat.mrn
left join proc_result_dx
	on p.mrn = proc_result_dx.mrn
group by p.mrn
