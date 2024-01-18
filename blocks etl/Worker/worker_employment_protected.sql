/* worker_employment_protected
along with some basic dimensions to support filtering capture groupers for
the involuntary or voluntary termination and gender, generation, and ethnicity
filters/dimensions for the current workforce
*/
select
    --w.worker_id, /* only use for trouble shooting */
    j_attrb.job_family,
    j_grp.use_job_group_id as job_group_id,
    j_grp.rn_job_ind,
    j_grp.nursing_category,
    w.active_ind,
    case
        when w.termination_date >= add_months(current_date, -12)
        then date_trunc('month', w.termination_date) end as recent_termination_month,
    case
        when w.hire_date >= add_months(current_date, -12)
        then date_trunc('month', w.hire_date) end as recent_hire_month,

    protected.gender,
    protected.ethnicity_id,
    protected.birth_date, -- recoommnedation: use generation
    protected.death_date,
    protected.hispanic_or_latino_ind, /* please note:  NOT used to determine minority */

    /* these can come from worker_sensitive once put there officially by HR */
    case protected.gender
        when 'F' then 'Female'
        when 'M' then 'Male'
        else 'unknown' end as gender_label,
    case protected.ethnicity_id
        when 'USA_White' then 'White'
        when 'USA_Black_or_African_American' then 'Black or Af Am'
        when 'USA_Hispanic_or_Latino' then 'Hispanic/Latino'
        when 'USA_Asian' then 'Asian'
        when 'USA_Two_or_More_Races' then '2 or More'
        when 'USA_Native_Hawaiian_or_Other_Pacific_Islander' then 'Hawaiian/Islander'
        when 'USA_American_Indian_or_Alaska_Native' then 'Am Indian/Alaska'
        else 'unk'
        end as ethnicity_label, /* TDLneed */
    /*  first logic in data governance review -- did NOT stick with this ack in 2022
    case when wS.ETHNICITY_ID = 'USA_White' and wS.HISPANIC_OR_LATINO_IND <> 1
        then 0 else 1 end as minority_ind,
    case when wS.ETHNICITY_ID = 'USA_White' and wS.HISPANIC_OR_LATINO_IND <> 1
        then 'White and non-Hispanic/Latino' else 'Minority' end as minority_desc_label,
    case when wS.ETHNICITY_ID = 'USA_White' and wS.HISPANIC_OR_LATINO_IND <> 1
        then 'No' else 'Yes' end as minority_YN_label,
    */

/* this logic as of latest from Megan Song in early January 2022, probably what will be added to
    Worker_sensitive, HISPANIC_OR_LATINO_IND not used at all for the criteria */
    case
        when protected.ethnicity_id = 'USA_White' then 0
        when protected.ethnicity_id is not null then 1
        end as minority_ind,
    case
        when protected.ethnicity_id = 'USA_White' then 'White' else 'Minority'
        end as minority_desc_label,
    case
        when protected.ethnicity_id = 'USA_White' then 'No' else 'Yes'
        end as minority_yn_label,
/* END --- worker_sensitive once put there officially by HR */

    case
        when protected.termination_involuntary_ind = -2
        then null
        else protected.termination_involuntary_ind end as termination_involuntary_ind,

    case
        when w.active_ind = 0
        then case protected.termination_involuntary_ind
            when 1 then 'Involuntary'
            when 0 then 'Voluntary'
            else 'unknown' end
        end as term_category,

    case when protected.active_ind = 0
        then case protected.termination_involuntary_ind
            when 1 then 'Invol'
            when 0 then 'Vol'
            else 'unk' end
        end as term_category_abbr,
    mgr_w.worker_id as direct_supervisor_worker_id,
    mgr_w.display_name_formatted as direct_supervisor_name_formatted,
    w.worker_wid,
    w.position_wid,
    w.total_years_as_employee,
    w.employee_ind,
    case
        when protected.birth_date < '01-JAN-1928' then 'very old'
        when protected.birth_date between '01-jan-1928' and '31-DEC-1945' then 'Silent Generation'
        when protected.birth_date between '01-JAN-1946' and '31-DEC-1964' then 'Baby Boomers'
        when protected.birth_date between '01-JAN-1965' and '31-DEC-1980' then 'Generation X'
        when protected.birth_date between '01-JAN-1981' and '31-DEC-1996' then 'Generation Y/Millennials'
        when protected.birth_date >= '01-JAN-1997' then 'Generation Z'
        else 'Birth year of ' || year(protected.birth_date)
        end as generation,
    case when w.active_ind = 1 then protected.latest_age end as current_age,
    case when w.magnet_reporting_ind = 1 and j_grp.nursing_category is not null
        then 1 else 0 end as nursing_dashboard_use_ind

from
    {{ ref('worker') }} as w
    left join {{ ref('worker_sensitive') }} as protected
        on w.worker_wid = protected.worker_wid
    left join {{ ref('worker') }} as mgr_w
        on w.manager_worker_wid = mgr_w.worker_wid
    left join {{ ref('job_code_profile') }} as j_attrb
        on w.job_code = j_attrb.job_code
    left join {{ ref('stg_nursing_job_code_group_statistic') }} as j_grp
        on w.job_code = j_grp.job_code
