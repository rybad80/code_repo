{{
  config(
    meta = {
      'critical': true
    }
  )
}}
with ranked_activity as (
    select
        activity_object_key.object_id,
        activity.transaction_cd,
        activity.transaction_datetime,
        row_number() over(partition by activity_object_key.object_id order by activity.transaction_datetime desc) as activity_rank
    from {{source('safetrace_ods', 'safetrace_activity_object_key')}} as activity_object_key
    inner join {{source('safetrace_ods', 'safetrace_activity')}} as activity
        on activity.activity_id = activity_object_key.activity_id
    where activity_object_key.object_type_cd = 'PRODINV_ID'
        and activity.transaction_cd in ('TX', 'TA')
),
transfusion_activity as (
    select
        ranked_activity.object_id,
        ranked_activity.transaction_cd
    from
        ranked_activity
    where
        ranked_activity.activity_rank = 1
),
xm_test as (
    select
        test_outcome.test_outcome_id,
        test_outcome.test_id,
        test_outcome_interpretation.test_interpretation,
        test_ext.descript as test_name,
        test_interpretation.literal,
        test_outcome.patient_specimen_id
    from {{source('safetrace_ods', 'safetrace_test_outcome')}} as test_outcome
    left join {{source('safetrace_ods', 'safetrace_test_outcome_interpretation')}} as test_outcome_interpretation
        on test_outcome_interpretation.test_outcome_id = test_outcome.test_outcome_id
    left join {{source('safetrace_ods', 'safetrace_test_ext')}} as test_ext
        on test_ext.test_id = test_outcome.test_id
    left join {{source('safetrace_ods', 'safetrace_test_interpretation')}} as test_interpretation
        on test_interpretation.test_id = test_outcome.test_id
            and test_outcome_interpretation.test_interpretation = test_interpretation.test_interpretation
    where
        test_outcome.test_id in (
            'XMAGT',
            'XME',
            'XMGEL',
            'XMIS',
            'XMN',
            'XMPW'
        )
        and test_outcome.test_outcome_status_cd = 'C'  -- test was completed
),
clarity_provider as (
    select
        clarity_ser_ext_id.ext_system_id,
        dim_provider.prov_id,
        dim_provider.full_name as full_nm
    from {{source('clarity_ods', 'clarity_ser_ext_id')}} as clarity_ser_ext_id
    left join {{ref('dim_provider')}} as dim_provider
	    on dim_provider.prov_id = clarity_ser_ext_id.prov_id
    where clarity_ser_ext_id.line = 1
),
clarity_specimen as (
    select distinct
        stg_specimen_order.specimen_id,
        stg_specimen_order.specimen_number
    from {{ref('stg_specimen_order')}} as stg_specimen_order
)
select
{{
        dbt_utils.surrogate_key(["'SAFETRACE'", 'product_inventory.prodinv_id'])
}} as blood_bank_transfusion_key,
'SAFETRACE~' || product_inventory.prodinv_id as integration_id,
product_inventory.prodinv_id as product_inventory_id,
product_inventory.unit_no,
product_inventory.division,
product_inventory.product_id,
product_ext.descript as product_name,
product_inventory.standard_product_code as ecode,
standard_product_class_ext.descript as ecode_description,
product_inventory.available_quantity,
product_inventory.abo_cd as product_abo_type,
product_inventory.rh_cd as product_rh_factor,
case
    when product_inventory.division = '00' then 0
    else 1
end as aliquot_ind,
case
    when lower(standard_product_class_ext.descript) like '%irradiated%' then 1
    else 0
end as irradiated_ind,
case
    when lower(standard_product_class_ext.descript) like '%washed%' then 1
    else 0
end as washed_ind,
case
    when lower(standard_product_class_ext.descript) like '%washed%none%' then 'Washed'
    when lower(standard_product_class_ext.descript) like '%supernat rem%' then 'Adsol removed'
    when lower(standard_product_class_ext.descript) like '%>as1%' then 'AS1'
    when lower(standard_product_class_ext.descript) like '%>as3%' then 'AS3'
    when lower(standard_product_class_ext.descript) like '%>as5%' then 'AS5'
    when lower(standard_product_class_ext.descript) like '%cpda-1%' then 'CPDA-1'
    when lower(standard_product_class_ext.descript) like '%cp2d%' then 'CP2D'
    when lower(standard_product_class_ext.descript) like '%cpd%' then 'CPD'
    when lower(standard_product_class_ext.descript) like '%acd-a%' then 'ACD-A'
    when lower(standard_product_class_ext.descript) like '%none%' then 'None'
end as product_preservative,
xm_test.test_id as crossmatch_test_id,
xm_test.test_name as crossmatch_test_name,
xm_test.test_interpretation as crossmatch_test_result,
xm_test.literal as crossmatch_test_result_details,
clarity_specimen.specimen_id as crossmatch_test_specimen_id,
xm_test.patient_specimen_id as crossmatch_test_specimen_number,
coalesce(dim_blood_bank_patient.patient_key, 0) as patient_key,
orders.patient_id as blood_bank_patient_id,
external_order_number.ext_order_value::int as procedure_order_id,
product_inventory.location_id as product_location_id,
orders.order_sublocation_id,
clarity_provider.prov_id as provider_id,
orders.order_provider_id as blood_bank_provider_id,
clarity_provider.full_nm as provider_name,
cast(timezone(orders.order_datetime, 'UTC', 'America/New_York') as datetime) as order_datetime,
cast(timezone(order_product_inventory.issue_datetime, 'UTC', 'America/New_York') as datetime) as issue_datetime,
-- Non-BPAM transfusions don't get a real start time, so we use the issue time
case
    when transfusion_activity.transaction_cd = 'TX'
        then cast(timezone(order_product_inventory.transfusion_date_starttime, 'UTC', 'America/New_York') as datetime)
    when transfusion_activity.transaction_cd = 'TA'
        then cast(timezone(order_product_inventory.issue_datetime, 'UTC', 'America/New_York') as datetime)
end as transfusion_start_datetime,
cast(timezone(order_product_inventory.transfusion_date_endtime, 'UTC', 'America/New_York') as datetime) as transfusion_end_datetime,
/* The draw date is calculated in Safetrace by subtracting a certain number of days from the expiration date.
 * This does not work when daylight savings starts or ends during that interval.
 * So, instead of converting from UTC to local, we simply subtract one day. */
date(date(product_inventory.draw_datetime) - interval '1 days') as product_draw_date,
cast(timezone(product_inventory.expiration_datetime, 'UTC', 'America/New_York') as datetime) as product_expiration_datetime
from {{source('safetrace_ods', 'safetrace_product_inventory')}} as product_inventory
inner join {{source('safetrace_ods', 'safetrace_order_product_inventory')}} as order_product_inventory
    on product_inventory.prodinv_id = order_product_inventory.prodinv_id
inner join {{source('safetrace_ods', 'safetrace_orders')}} as orders
    on orders.order_id = order_product_inventory.order_id
inner join transfusion_activity
    on transfusion_activity.object_id = product_inventory.prodinv_id
left join xm_test
    on xm_test.test_outcome_id = order_product_inventory.xm_test_outcome_id
left join clarity_specimen
    on clarity_specimen.specimen_number = xm_test.patient_specimen_id
left join {{ref('dim_blood_bank_patient')}} as dim_blood_bank_patient
    on dim_blood_bank_patient.blood_bank_patient_id = orders.patient_id
left join {{source('safetrace_ods', 'safetrace_provider')}} provider
    on provider.provider_id = orders.order_provider_id
left join {{source('safetrace_ods', 'safetrace_provider_member')}} as provider_member
    on provider_member.member_provider_id = provider.provider_id
left join clarity_provider
    on clarity_provider.ext_system_id = provider_member.member_id
inner join {{source('safetrace_ods', 'safetrace_product_ext')}} as product_ext
    on product_ext.product_id = product_inventory.product_id
inner join {{source('safetrace_ods', 'safetrace_standard_product_class_ext')}}
    as standard_product_class_ext
        on standard_product_class_ext.standard_product_code = product_inventory.standard_product_code
left join {{source('safetrace_ods', 'safetrace_external_order_number')}} as external_order_number
    on external_order_number.order_id = order_product_inventory.order_id
        and external_order_number.item_no = order_product_inventory.item_no
where
    order_product_inventory.order_product_inv_stat_cd = 'T'
    and order_product_inventory.issue_datetime is not null
    and product_inventory.inventory_status_cd = 'T'
