select
    surgery_procedure.or_key,
    surgery_procedure.or_proc_id,
    case
        when surgery_procedure.or_proc_id in (
            '2716.003', '3598.003', '4869', '4870', '5090.003', '5243.003', '7875', '8600'
            ) then 'expansion'
        when surgery_procedure.or_proc_id in (
            '1130.003', '5573'
            ) then 'exploration'
        when surgery_procedure.or_proc_id in (
            '1132.003',
            '5456', '7870', '7871', '7872', '7874',
            '8435', '8440', '8443', '8445', '8694', '9400'
            ) then 'fusion'
        when surgery_procedure.or_proc_id in (
            '2789.003', '4437.003', '5206.003',
            '7876', '8596', '8597', '8598', '8599', '8601', '8603'
            ) then 'instrumentation - rib'
        when surgery_procedure.or_proc_id in (
            '1135.003', '1698.003', '1877.003',
            '2908.003', '2910.003',
            '3466.003', '3594.003', '3608.003', '3616.003',
            '4855.003', '5092.003', '5096.003', '5535.003', '5577', '5578'
            ) then 'instrumentation - spine'
        when surgery_procedure.or_proc_id in (
            '8922'
            ) then 'mehta cast'
        when surgery_procedure.or_proc_id in (
            '5552', '5553', '5554', '5555', '5556', '5557'
            ) then	'osteotomy'
        when surgery_procedure.or_proc_id in (
            '7928'
            ) then 'other'
        when surgery_procedure.or_proc_id in (
            '2912.003', '5578.003', '5583', '5585', '9397'
        ) then 'removal'
        when surgery_procedure.or_proc_id in (
            '1775.003', '3435.003', '4529.003', '4817.003',
            '5337.003', '7877', '8602', '9023'
        ) then	'revision'
        end as scoliosis_category
from
    {{ref('surgery_procedure')}} as surgery_procedure
where
    lower(surgery_procedure.service) = 'orthopedics'
    and scoliosis_category is not null --noqa: L028
group by
    surgery_procedure.or_key,
    surgery_procedure.or_proc_id
