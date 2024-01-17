{{
  config(
    meta = {
      'critical': false
    }
  )
}}
select distinct
stg_specimen_order.procedure_order_id,
stg_specimen_order.specimen_id,
stg_specimen_order.lab_test_key,
stg_specimen_order.test_id,
spec_test_ctnr_rm.performing_ovc_id as container_id
from {{source('clarity_ods','spec_test_ctnr_rm')}} as spec_test_ctnr_rm
inner join {{ref('stg_specimen_order')}} as stg_specimen_order
	on stg_specimen_order.specimen_id = spec_test_ctnr_rm.specimen_id
	and stg_specimen_order.spec_test_rel_line = spec_test_ctnr_rm.group_line
