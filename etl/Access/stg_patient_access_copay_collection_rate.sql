{{ config(materialized='table', dist='pat_enc_csn_id') }}

--The purpose of this code is to determine, at the encounter level, what the copay due and
--copay paid was. Copay data is stored at the transaction level so there could be multiple
--rows per encounter. 

with cvg_check as (
       --This query looks at the 2-4 filling orders and IDs any that are medicaide payors for 
       --eventual exclusion from Copay Collections
	select t1.pat_id,
            case when t1.secondary_payor_id in  (1074, 1079, 1176, 1082, 1105, 1180, 1078, 1190, 1160,
            1147, 1075, 1080, 1083, 1009, 1084, 1031, 1139, 1175, 1047, 1008, 1092, 1122, 1043,
            1007, 1130, 1062, 1116, 1141, 1086, 1054, 1140, 1168, 1081, 1087, 1210, 1181, 1208, 1209)
			or t1.third_payor_id in (1074, 1079, 1176, 1082, 1105, 1180, 1078, 1190, 1160, 1147, 1075, 1080,
            1083, 1009, 1084, 1031, 1139, 1175, 1047, 1008, 1092, 1122, 1043,
            1007, 1130, 1062, 1116, 1141, 1086, 1054, 1140, 1168, 1081, 1087, 1210, 1181, 1208, 1209)
			or t1.fourth_payor_id in (1074, 1079, 1176, 1082, 1105, 1180, 1078, 1190, 1160, 1147, 1075, 1080,
            1083, 1009, 1084, 1031, 1139, 1175, 1047, 1008, 1092, 1122, 1043,
            1007, 1130, 1062, 1116, 1141, 1086, 1054, 1140, 1168, 1081, 1087, 1210, 1181, 1208, 1209)
			then 'MEDICAIDE' else null end as sec_payor_group,
			case when t1.secondary_payor is not null then t1.secondary_payor
                when t1.third_payor is not null then t1.third_payor
				when t1.fourth_payor is not null then t1.fourth_payor
				else null end as add_payor_name,
			t1.cvg_eff_dt,
			t1.cvg_term_dt
            from
            (
           select
            pat_cvg_file_order.pat_id,
            max(decode(pat_cvg_file_order.filing_order, 1, coverage.payor_id)) as primary_payor_id,
            max(decode(pat_cvg_file_order.filing_order, 1, clarity_epm.payor_name)) as primary_payor,
            max(decode(pat_cvg_file_order.filing_order, 1, zc_epm_rpt_grp_10.abbr)) as primary_pay_grp,
            max(decode(pat_cvg_file_order.filing_order, 1, clarity_fc.fin_class_title)) as primary_pay_fc,
			max(decode(pat_cvg_file_order.filing_order, 2, coverage.payor_id)) as secondary_payor_id,
            max(decode(pat_cvg_file_order.filing_order, 2, clarity_epm.payor_name)) as secondary_payor,
            max(decode(pat_cvg_file_order.filing_order, 2, zc_epm_rpt_grp_10.abbr)) as secondary_pay_grp,
            max(decode(pat_cvg_file_order.filing_order, 2, clarity_fc.fin_class_title)) as secondary_pay_fc,
			max(decode(pat_cvg_file_order.filing_order, 3, coverage.payor_id)) as third_payor_id,
            max(decode(pat_cvg_file_order.filing_order, 3, clarity_epm.payor_name)) as third_payor,
            max(decode(pat_cvg_file_order.filing_order, 3, zc_epm_rpt_grp_10.abbr)) as third_pay_grp,
            max(decode(pat_cvg_file_order.filing_order, 3, clarity_fc.fin_class_title)) as third_pay_fc,
			max(decode(pat_cvg_file_order.filing_order, 4, coverage.payor_id)) as fourth_payor_id,
            max(decode(pat_cvg_file_order.filing_order, 4, clarity_epm.payor_name)) as fourth_payor,
            max(decode(pat_cvg_file_order.filing_order, 4, zc_epm_rpt_grp_10.abbr)) as fourth_pay_grp,
            max(decode(pat_cvg_file_order.filing_order, 4, clarity_fc.fin_class_title)) as fourth_pay_fc,
			cvg_eff_dt,
			cvg_term_dt
            from {{source('clarity_ods', 'pat_cvg_file_order')}} as pat_cvg_file_order
            inner join {{source('clarity_ods', 'coverage')}} as coverage
                on pat_cvg_file_order.coverage_id = coverage.coverage_id
            inner join {{source('clarity_ods', 'clarity_epm')}} as clarity_epm
                on coverage.payor_id = clarity_epm.payor_id
                and (clarity_epm.payor_name not like '.%' or clarity_epm.payor_name = '.SELF PAY')
            left join {{source('clarity_ods', 'zc_epm_rpt_grp_10')}} as zc_epm_rpt_grp_10
                on zc_epm_rpt_grp_10.rpt_grp_ten = clarity_epm.rpt_grp_ten
            left join {{source('clarity_ods', 'clarity_fc')}} as clarity_fc
                on clarity_epm.financial_class = clarity_fc.financial_class
            where pat_cvg_file_order.filing_order in (1, 2, 3, 4)
			and (cvg_eff_dt not in ( '1901-01-01 00:00:00') or cvg_term_dt not in ('1901-01-01 00:00:00'))
            group by
			pat_cvg_file_order.pat_id,
			cvg_eff_dt,
			cvg_term_dt) as t1
),

ma_csn as (
--This query takes the pat_ids found above with Medicaide payors and IDs enc CSNs that happened
--while that patient had a Medicaide coverage for exclusion. Takes into account the coverage start/end dates
select
    pat_enc_csn_id
from
    {{source('clarity_ods', 'front_end_pmt_coll_hx')}} as front_end_pmt_coll_hx
	inner join cvg_check on front_end_pmt_coll_hx.pat_id = cvg_check.pat_id
where
	cvg_check.sec_payor_group is not null
    and contact_date >= cvg_check.cvg_eff_dt
	and (contact_date < cvg_check.cvg_term_dt or cvg_check.cvg_term_dt is null)
),

--This part of the code collapses the copays to the encounter level.
copay_amts as (
    select
        front_end_pmt_coll_hx.pat_enc_csn_id,
        max(front_end_pmt_coll_hx.pb_copay_coll) as pb_copay_coll,
        max(front_end_pmt_coll_hx.pb_copay_due) as pb_copay_due,
        max(front_end_pmt_coll_hx.pb_copay_paid) as pb_copay_paid,
        max(front_end_pmt_coll_hx.hb_copay_coll) as hb_copay_coll,
        max(front_end_pmt_coll_hx.hb_copay_due) as hb_copay_due,
        max(front_end_pmt_coll_hx.hb_copay_paid) as hb_copay_paid
    from
        {{source('clarity_ods', 'front_end_pmt_coll_hx')}} as front_end_pmt_coll_hx
	left join ma_csn on ma_csn.pat_enc_csn_id = front_end_pmt_coll_hx.pat_enc_csn_id
    and front_end_pmt_coll_hx.contact_date < '08-Mar-2022' --add in secondary payor information prior to 
                                                            --3/8/22 as an Epic change was implemented 
                                                            --after this date
    where
        front_end_pmt_coll_hx.contact_date < current_date
    and date(front_end_pmt_coll_hx.contact_date) <= date(front_end_pmt_coll_hx.coll_instant_utc_dttm)
	and ma_csn.pat_enc_csn_id is null --includes only patients without a Medicadie payor as secondary insurance
	and front_end_pmt_coll_hx.pat_enc_csn_id not in (
        select pat_enc_csn_id
            from {{source('clarity_ods', 'front_end_pmt_coll_hx')}}
            where
                rsn_non_coll_amt_c = 101)
    and front_end_pmt_coll_hx.pat_enc_csn_id not in
	(--excludes POST OP encounters, no copays should be collected for these encounters based on CSA input (Dylan Horn)
	select pat_enc.pat_enc_csn_id
  from {{source('clarity_ods', 'pat_enc')}} as pat_enc
  inner join {{ref('lookup_access_post_op_visit_types')}} as lookup_access_post_op_visit_types
    on pat_enc.appt_prc_id = lookup_access_post_op_visit_types.prc_id
  where pat_enc.appt_prc_id not like '%P%'
    )
    group by
        front_end_pmt_coll_hx.pat_enc_csn_id
),

--HB and PB data are stored in separate columns so this part of the code combines them into one.
combine_hb_and_pb as (
select
    pat_enc_csn_id,
    hb_copay_due + pb_copay_due as copay_due,
    hb_copay_coll + pb_copay_coll as copay_coll,
    hb_copay_paid + pb_copay_paid as copay_paid
from
    copay_amts
),

--Copay paid tells us how much of the copay was paid at a specific point in time whereas copay
--collected tells us how much of the copay was already collected at that point. Since at the
--encounter level they are essentially telling us the same thing, they are combined here.    
copay_paid_amt as (
select
    pat_enc_csn_id,
    copay_due,
    case when
        copay_coll = 0 then copay_paid
        else copay_coll
    end as copay_paid
from
    combine_hb_and_pb
),

all_rsn_non_coll as (
select
    front_end_pmt_coll_hx.pat_enc_csn_id,
    zc_rsn_non_coll_amt.name as rsn_non_coll,
    row_number() over (partition by front_end_pmt_coll_hx.pat_enc_csn_id order by pat_enc_date_real) as rn
from {{source('clarity_ods', 'front_end_pmt_coll_hx')}} as front_end_pmt_coll_hx
    inner join {{source('clarity_ods', 'zc_rsn_non_coll_amt')}} as zc_rsn_non_coll_amt
        on zc_rsn_non_coll_amt.rsn_non_coll_amt_c = front_end_pmt_coll_hx.rsn_non_coll_amt_c
where rsn_non_coll is not null
),

first_rsn_non_coll as (
select
    pat_enc_csn_id,
    rsn_non_coll
from all_rsn_non_coll
where rn = 1
)

--detail as (
select
    front_end_pmt_coll_hx.pat_enc_csn_id,
    copay_due as copay_amount_due,
    case when copay_due > 3500 then 1 else 0 end as high_copay_amount_due_ind,
    copay_paid as copay_amount_paid,
    rsn_non_coll
from {{source('clarity_ods', 'front_end_pmt_coll_hx')}} as front_end_pmt_coll_hx
    left join copay_paid_amt
        on copay_paid_amt.pat_enc_csn_id = front_end_pmt_coll_hx.pat_enc_csn_id
    left join first_rsn_non_coll
        on first_rsn_non_coll.pat_enc_csn_id = front_end_pmt_coll_hx.pat_enc_csn_id
where
    contact_date < current_date
    and copay_due > 0
    and event_type_c = 0    --Collection event
