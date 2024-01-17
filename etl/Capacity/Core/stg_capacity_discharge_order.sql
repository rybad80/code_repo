with transord as (
    select distinct
        order_proc.pat_enc_csn_id,
        coalesce(order_proc.order_inst, order_proc.order_time) as order_dt,
        order_proc.display_name,
        order_proc_2.pat_loc_id as dept_id
    from
        {{source('clarity_ods','order_proc')}} as order_proc
        inner join {{source('clarity_ods','order_proc_2')}} as order_proc_2
            on order_proc.order_proc_id = order_proc_2.order_proc_id
        inner join {{source('cdw_analytics', 'fact_department_rollup')}} as fact_department_rollup
            on fact_department_rollup.dept_id = order_proc_2.pat_loc_id
            and fact_department_rollup.dept_align_dt
                = date(coalesce(order_proc.order_inst, order_proc.order_time))
    where
        (
            display_name like 'Transfer Bed Request%'   -- NICU only can use this for MRFT, PICU
            or display_name = 'IP Transfer Bed Request' -- NICU only can use this for MRFT, PICU
            or display_name = 'Transfer Patient: Use to finalize transfer and move patient to a new unit'
        )
),

getdcord as (
    select
        order_proc.pat_enc_csn_id,
        coalesce(order_proc.order_inst, order_proc.order_time) as order_dt,
        order_proc.display_name,
        order_proc_2.pat_loc_id as dept_id,
        dense_rank() over (partition by order_proc.pat_enc_csn_id
            order by
            instantiated_time desc, order_proc.order_proc_id desc) as last_dc_ord,
        order_proc.order_proc_id as discharge_proc_ord_id
    from
        {{source('clarity_ods','order_proc')}} as order_proc
        inner join {{source('clarity_ods','order_proc_2')}} as order_proc_2
            on order_proc_2.order_proc_id = order_proc.order_proc_id
        inner join {{source('cdw_analytics', 'fact_department_rollup')}} as fact_department_rollup
            on fact_department_rollup.dept_id = order_proc_2.pat_loc_id
            and fact_department_rollup.dept_align_dt
                = date(coalesce(order_proc.order_inst, order_proc.order_time))
    where
        upper(display_name) = 'DISCHARGE PATIENT' -- PICU uses
        and coalesce(reason_for_canc_c, '0') != '51' --'Discontinue' we don't want the ones that did not happen
        and instantiated_time is null -- the one that is filled in does not get an order acknowledgement
),

unionset as (
    select
        case
            when
                display_name like '%Transfer%'
            then
                'Xfer Order'
            else
                'should not be'
        end
        as ordvstevtaactn_nm,
        order_dt,
        pat_enc_csn_id,
        dept_id as from_dept_id,
        0 as ord_id
    from
        transord
    union distinct
    select
        'Discharge Order' as ordvstevtaactn_nm,
        order_dt,
        pat_enc_csn_id,
        coalesce(dept_id, 0) as from_dept_id,
        discharge_proc_ord_id as ord_id
    from
        getdcord
    where
        last_dc_ord = 1
)

select
    cast(pat_enc_csn_id as bigint) as pat_enc_csn_id,
    ordvstevtaactn_nm as event_action_nm,
    order_dt as event_action_dt_tm,
    cast(coalesce(from_dept_id, 0) as bigint) as from_dept_id,
    ord_id
from unionset
