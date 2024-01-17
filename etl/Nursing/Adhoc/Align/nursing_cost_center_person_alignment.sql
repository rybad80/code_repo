/* nursing_cost_center_person_alignment
roll up the NCCS cost center people alignments to the system chief nursing officer and of those set the
acute unit subsets for Philadelphia and King of Prussia to serve fast filters in the Nursing Dashboard
ip_ed_nod_subset_ind = inpatient, emergency, nursing operations department set of acute nursing units
and include rows for the finance partners
*/

with
vp_and_dir as (
    select
        -- cc_owner.reporting_chain as superior_reporting_chain,
        cc_superior_id,
        cost_center_id,
        cc_superior_name,
        superior_has_rn_reports_ind,
        nurse_supervisor_or_manager_job_ind,
        cc_nurse_mid_management_nm,
        cc_nccs_leader_nm,
        cc_nurse_upper_management_nm,
        cc_superior_management_level_sort_num,
        cc_superior_management_level_abbreviation
    from
        {{ ref('stg_nursing_cost_center_leader') }}
),

cno_data as (
    select
        worker_job_reference.worker_id as cno_worker_id,
        nursing_worker.legal_reporting_name as cno,
        worker_job_reference.job_title_display as current_cno_job_title,
        worker_job_reference.data_as_of as cno_as_of_dt,
        nursing_worker.worker_wid as cno_worker_wid
    from
		{{ ref('worker_job_reference') }} as worker_job_reference
        inner join {{ ref('nursing_worker') }} as nursing_worker
            on worker_job_reference.worker_id = nursing_worker.worker_id
    where
        rn_alt_job_group_id = 'CNO'
),

cno_reports as (
    select
        cno_worker_id,
        cno,
        lookup_management_level.management_level_sort_num as cno_cc_superior_management_level_sort_num,
        cno_report.worker_id as cno_rpt_worker_id,
        cno_report.workday_emp_key as cno_rpt_emp_key,
        cno_report.display_name as possible_acno
    from
        cno_data
        inner join {{ ref('worker') }} as cno_worker
            on cno_data.cno_worker_wid = cno_worker.worker_wid
        inner join {{ ref('lookup_management_level') }} as lookup_management_level
            on cno_worker.management_level = lookup_management_level.management_level
        inner join {{ ref('worker') }} as cno_report
            on cno_data.cno_worker_wid = cno_report.manager_worker_wid
        inner join {{ ref('lookup_management_level') }} as report_management_level
            on cno_report.management_level = report_management_level.management_level
    where
        report_management_level.vp_and_up_ind = 1
),

cc_finance_rollup as (
    select
        cost_center_id,
        finance_partner_worker_id
	from
        {{ ref('lookup_nursing_nccs_rollup') }}
	where
        finance_partner_worker_id is not null

)

/* add CNO for all applicable VPs that report to system chief nursing officer
and set the applicable Phila indicator for all CNO cc's minus VP Daneen Smith cc's  */
select
    cno_worker_id as cc_superior_id,
    vp_and_dir.cost_center_id,
    cno as cc_superior_name,
    null as cc_finance_partner,
    cno as cc_nccs_leader_nm,
    cno as cc_nurse_upper_management_nm,
    vp_and_dir.cc_nurse_mid_management_nm,
    cno_cc_superior_management_level_sort_num as cc_superior_management_level_sort_num,
    case when vp_and_dir.cc_superior_id = '790600' -- Smith, Daneen currently has KOP nursing cc's
        then 0 -- exclude KOPH from Phila even if report up to CNO
        else 1
    end as phila_ind,
    0 as koph_ind, /* only applies to Daneen in other union below*/
    case phila_ind
        when 1
        then case nursing_cost_center_attributes.cost_center_group
            when 'Acute Nursing Unit'
                --then 1 -- temporary use case below until cost center groups are set to something
                then case nursing_cost_center_attributes.cost_center_parent
                    when 'Day Medicine' then 0
                    when 'Nursing Behavioral Health' then 0
                    when 'Nursing Cardiac Surgical Services' then 0
                    when 'SDU Services' then 0
                    else 1
                end
            else 0
        end
    end as ip_ed_nod_subset_ind,
    case ip_ed_nod_subset_ind
        when 1 then 1
        when 0 then 1
    end as campus_all_ind,
    nursing_cost_center_attributes.cost_center_type,
    nursing_cost_center_attributes.cost_center_group,
    nursing_cost_center_attributes.cost_center_parent,
    nursing_cost_center_attributes.cost_center_display,
    vp_and_dir.cc_superior_management_level_abbreviation
from
    vp_and_dir
    inner join cno_reports
        on vp_and_dir.cc_superior_id = cno_reports.cno_rpt_worker_id
    inner join {{ ref('nursing_cost_center_attributes') }} as nursing_cost_center_attributes
        on vp_and_dir.cost_center_id = nursing_cost_center_attributes.cost_center_id

union all

/* carry all the VP and director rollups along, and set the applicable KOPH indicator, currently VP Daneen Smith */
select
    vp_and_dir.cc_superior_id,
    vp_and_dir.cost_center_id,
    vp_and_dir.cc_superior_name,
    null as cc_finance_partner,
    vp_and_dir.cc_nccs_leader_nm,
    vp_and_dir.cc_nurse_upper_management_nm,
    vp_and_dir.cc_nurse_mid_management_nm,
    vp_and_dir.cc_superior_management_level_sort_num,
    0 as phila_ind, /* handled above as CNO cc's minus the KOP ones */
    case when vp_and_dir.cc_superior_id = 790600 -- 'Smith, Daneen'
        then 1
        else 0
    end as koph_ind,
    case koph_ind
        when 1
            then case nursing_cost_center_attributes.cost_center_group
                when 'Acute Nursing Unit' then 1
                else 0
            end
    end as ip_ed_nod_subset_ind,
    case ip_ed_nod_subset_ind
        when 1 then 1
        when 0 then 1
    end as campus_all_ind,
    nursing_cost_center_attributes.cost_center_type,
    nursing_cost_center_attributes.cost_center_group,
    nursing_cost_center_attributes.cost_center_parent,
    nursing_cost_center_attributes.cost_center_display,
    vp_and_dir.cc_superior_management_level_abbreviation
from
    vp_and_dir
    inner join {{ ref('nursing_cost_center_attributes') }} as nursing_cost_center_attributes
        on vp_and_dir.cost_center_id = nursing_cost_center_attributes.cost_center_id

union all

/* add finance partner rollups */
select
    worker.worker_id as cc_superior_id,
    cc_finance_rollup.cost_center_id,
    worker.legal_reporting_name as cc_superior_name,
    worker.legal_reporting_name as cc_finance_partner,
    null as cc_nccs_leader_nm,
    null as cc_nurse_upper_management_nm,
    null as cc_nurse_mid_management_nm,
    99 as cc_superior_management_level_sort_num,
    0 as phila_ind, /* n/a when filtering by finance person */
    0 as koph_ind,
    0 as ip_ed_nod_subset_ind,
    0 as campus_all_ind,
    nursing_cost_center_attributes.cost_center_type,
    nursing_cost_center_attributes.cost_center_group,
    nursing_cost_center_attributes.cost_center_parent,
    nursing_cost_center_attributes.cost_center_display,
    'n/a' as cc_superior_management_level_abbreviation
from
    cc_finance_rollup
    inner join
		{{ ref('worker') }} as worker
            on cc_finance_rollup.finance_partner_worker_id = worker.worker_id
    inner join {{ ref('nursing_cost_center_attributes') }} as nursing_cost_center_attributes
        on cc_finance_rollup.cost_center_id = nursing_cost_center_attributes.cost_center_id
