with
all_tasks as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_healthcloud_tasks'), ref('stg_salescloud_tasks')]
    ) }}
),

dates as (
   select * from  {{source('cdw', 'master_date')}}
)

select id as task_id,
strleft(id, 15) as short_id,
activitydate as activity_date,
status,
ownerid as owner_id,
case isdeleted when true then 1 when false then 0 else null end as is_deleted_ind,
accountid as account_id,
case isclosed when true then 1 when false then 0 else null end as is_closed_ind,
createddate as created_date,
lastmodifieddate as last_modified_date,
systemmodstamp as system_modified_timestamp,
case isarchived when true then 1 when false then 0 else null end as is_archived_ind,
date_time_closed_hcm__c as date_time_closed,
date_time_opened_hcm__c as date_time_opened,
sales_cloud_taskid_hcm__c as sales_cloud_taskid,
recordtypeid as record_type_id,
coalesce(call_line_hcm__c, call_line__c, 'Provider Line') as call_line,
md.fy_yyyy_mm as fiscal_year_month_number,
md.fy_yyyy_mm_nm as fiscal_year_month_name,
md.f_qtr as fiscal_year_quarter,
md.f_yy as fiscal_year,
md.cy_yyyy_mm as calendar_year_month_number,
md.day_nm as day_name,
md.dt_key as calendar_date_key,
md.full_dt as date,
case
    when md.f_day <= (select md.f_day from dates as md where md.full_dt = date(now())) then 1 else 0
end as fiscal_year_day_indicator,
case
    when
        md.f_yy = (select md.f_yy from dates as md where md.full_dt = date(now() - interval '1 days')) then 1
    else 0
end as current_fiscal_year_indicator,
case
    when
        md.f_day <= (
            select md.f_day from dates as md where md.full_dt = date(now())
        ) and md.f_yy = (
            select md.f_yy from dates as md where md.full_dt = date(now() - interval '1 days')
        ) - 1 then 1
    else 0
end as last_fiscal_year_indicator,
case
    when
        md.f_yy >= (select md.f_yy - 3 from dates as md where md.full_dt = date(now() - interval '1 days')) then 1
    else 0
end as fiscal_year_cap_indicator,
md.c_wk_start_dt +  interval '1 days' as week_start,
md.month_nm as month_name,
/*these long case statements are to bring historic data in line with current organization and 
to correct some migration errors from the healthcloud implementation. 
They shouldn't generally need much updating going forward. */
---------------------------------Department---------------------------------------------
case coalesce(departmentpicklist_hcm__c, department_orig__c)
   when 'Adolescent Specialty Care' then 'Adolescent Medicine'
   when 'Behavioral Health' then 'Behavioral Health (DCAPS)'
   when 'ED' then 'Emergency Medicine'
   when 'Endocrinology' then 'Endocrinology and Diabetes'
   when 'Healthy Weight Program' then 'Healthy Weight'
   when 'Infectious Disease' then 'Infectious Diseases'
   when 'International Medicine' then 'Global Medicine'
   when 'Plastic surgery' then 'Plastic and Reconstructive Surgery'
   when 'Pulmonary' then 'Pulmonology and Sleep Medicine'
   when 'Rehabilitation' then 'Rehabilitation Medicine'
   when 'Special Immunology' then 'Allergy and Immunology'
   when 'Special Babies' then 'Special Delivery Unit'
   when 'Neuro-Oncology' then 'Neurology'
   when 'Neuromuscular' then 'Neurology'
   when 'Neonatal Follow UP' then 'Neonatology'
   when 'Feeding Team' then 'Feeding and Swallowing'
   when 'Child Development' then 'Developmental Pediatrics'
   when 'Immunology' then 'Allergy and Immunology'
   when 'Allergy & Immunology' then 'Allergy and Immunology'
   when 'Fetal Surgery' then 'Fetal Medicine'
   when 'Fetal Diagnostic & Treatment' then 'Fetal Medicine'
   when 'Center for Fetal Diagnostic & Treatment' then 'Fetal Medicine'
   when 'Diagnostic and Complex Care' then
        case when original_department = 'Critical Care' then 'Anesthesiology and Critical Care'
        else 'Diagnostic and Complex Care' end
   when 'Complex Care' then 'Diagnostic and Complex Care'
   when 'Diagnostic Center' then 'Diagnostic and Complex Care'
   when 'Critical Care' then 'Anesthesiology and Critical Care'
   when 'Anesthesiology' then 'Anesthesiology and Critical Care'
   when 'GI' then
        case when original_department = 'Nutrition Dietician' then 'Clinical Nutrition'
        else 'Gastroenterology and Hepatology' end
    when 'GI, Hepatology and Nutrition' then 'Gastroenterology and Hepatology'
    when 'Nutrition' then 'Clinical Nutrition'
    when 'Orthopaedics' then 'Orthopedics'
    when 'ENT' then 'Otolaryngology'
    when 'Pulmonology' then 'Pulmonology and Sleep Medicine'
    when 'Link2CHOP' then 'Other Division'
    when 'MyCHOP' then 'Other Division'
    when 'PT/OT' then 'Physical Therapy'
    when 'Physicial Therapy' then 'Physical Therapy'
    when '1 800 Try CHOP' then 'Other Division'
    when 'King of Prussia (KOPH)' then 'King of Prussia - Hospitalist'
    when 'Other' then 'Other Division'
   else coalesce(departmentpicklist_hcm__c, department_orig__c, 'Other Division')
   end as department,
  ---------------------Division---------------------------------------
   case coalesce(departmentpicklist_hcm__c, department_orig__c)
    when 'Infectious Diseases' then 'CHCA'
    when 'Orthopaedics' then 'CSA'
    when 'Rehabilitation Medicine' then 'CHCA'
    when 'Immunology' then 'CHCA'
    when 'Fetal Medicine' then 'CSA'
    when 'Center for Fetal Diagnostic & Treatment' then 'CSA'
    when 'Fetal Diagnostic & Treatment' then 'CSA'
    when 'Fetal Surgery' then 'CSA'
    when 'Nutrition' then 'CHCA'
    when 'Nutrition Dietician' then 'CHCA'
    when 'GI' then 'CHCA'
    when 'Neonatal Follow UP' then 'CHCA'
    when 'Neonatology' then 'CHCA'
    when 'Adoption' then 'Other'
    when 'Audiology' then 'Other'
    when 'CARE Clinic' then 'Other'
    when 'Mitochondrial Medicine' then 'Other'
    when 'Neurofibromatosis' then 'Other'
    when 'Diagnostic and Complex Care' then 'Other'
    when 'Complex Care' then 'Other'
    when 'Diagnostic Center' then 'Other'
    when 'Oral Surgery' then 'CSA'
    when 'Special Babies' then 'Other'
    when 'Pulmonology' then 'CHCA'
    when 'Dentistry' then 'CSA'
    when 'Neuro-Oncology' then 'CHCA'
    when 'Neuromuscular' then 'CHCA'
    when 'PT/OT' then 'Other'
    when 'Physical Therapy' then 'Other'
    when 'Physicial Therapy' then 'Other'
    when 'Emergency Medicine' then 'ED'
    else coalesce(division_hcm__c, division__c)
    end as division,
--------------Historic Reason for Call---------------------------
 case  departmentpicklist_hcm__c
    when 'MyCHOP' then
        case reason_for_call_hcm__c
            when 'General Info - Questions' then 'MyCHOP'
            when 'Non-Provider Call' then 'MyCHOP'
            else 'Patient Care Coordination'
            end
    when 'Link2CHOP' then 'Link 2 CHOP issue'
    else coalesce(reason_for_call_hcm__c, reasonforcall__c)
 end as reason_for_call_historic,
 ------------------reason for call------------------------------
 case coalesce(reason_for_call_hcm__c, reasonforcall__c)
    when 'Admission notification quest/issue' then 'Patient Care Coordination'
    when 'Complaint' then 'General Info - Questions'
    when 'Direct Admission' then  'Patient Care Coordination'
    when 'Fax issue' then 'General Info - Questions'
    when 'HIM' then 'General Info - Questions'
    when 'Hospital to Hospital Inpatient Transfer' then 'Patient Care Coordination'
    when 'Insurance Info' then 'General Info - Questions'
    when 'Other' then 'General Info - Questions'
    when 'Follow up' then 'Non-urgent Appointment Request'
    when 'General Info - Questions' then
        case departmentpicklist_hcm__c
            when 'MyCHOP' then 'MyCHOP'
            when 'Link2CHOP' then 'Link 2 CHOP issue'
            else 'General Info - Questions'
            end
    when 'Non-Provider Call' then
        case departmentpicklist_hcm__c
        when 'MyCHOP' then 'MyCHOP'
            else 'Non-Provider Call'
            end
    when 'General Info - CHOP question' then 'General Info - Questions'
    when 'Process Question' then 'General Info - Questions'
    when 'Social Work' then 'General Info - Questions'
    when 'Outpatient visit notification quest/issue' then 'General Info - Questions'
    when 'Discharge summary question' then 'General Info - Questions'
    when 'Parent Call' then 'General Info - Questions'
    when 'Discharge notification quest/issue' then 'General Info - Questions'
    when 'Telehealth' then 'General Info - Questions'
    when 'Dropped Call' then 'Incomplete Call'
    when 'Results' then 'Test Results'
    when 'ED Referral/Transport' then 'Referral/transport'
    when 'Advice/ clinical guidance from specialist' then 'Advice/Clinical Guidance From Specialist'
    when 'Secret Shopper Call' then 'Wrong Number'
    else  coalesce(reason_for_call_hcm__c, reasonforcall__c, 'General Info - Questions')
    end as reason_for_call,
upd_dt,
coalesce(description2show__c, calldisposition) as description,
time_to_close_in_minutes__c as time_to_close_in_minutes,
source__c as source,
name as created_by_name,
contact_id,
contact_name,
contact_mailing_street,
contact_mailing_city,
contact_mailing_state,
contact_mailing_postal_code,
case when lower(contact_mailing_state) in ('pennsylvania', 'new jersey', 'connecticut', 'massachusetts', 'maine', 'new hampshire', 'rhode island', 'vermont') 
    then
    case when length(contact_mailing_postal_code) = 4 then '0' || contact_mailing_postal_code
         when length(contact_mailing_postal_code) = 8 then '0' || strleft(contact_mailing_postal_code, 4)
         else contact_mailing_postal_code
    end
    else contact_mailing_postal_code end as adjusted_mailing_postal_code,
contact_mailing_country,
contact_mailing_latitude, 
contact_mailing_longitude,
contact_phone,
contact_fax,
contact_email,
contact_title,
contact_department,
case contact_chop_employee when true then 1 when false then 0 else null end as contact_chop_employee_ind,
contact_chop_location,
contact_epic_id,
contact_account_name,
onekeyid__c as contact_from_iqvia,
case secure_chat_sent__c when true then 1 when false then 0 else null end as secure_chat_sent_ind,
_dbt_source_relation as source_system
from all_tasks
left join dates as md on all_tasks.date_opened = md.full_dt
