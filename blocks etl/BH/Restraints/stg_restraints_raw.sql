with flowsheet_restraints as (
        --region identify the unique flowsheets, their restraint type, and the limb restrainted
        select
            group_name,
            group_id,
            flowsheet_name,
            flowsheet_id,
            fs_key,
            --group by limb in case the device is misrecorded
   case when flowsheet_name like '%LUE%' then 'LUE'
                when flowsheet_name like '%RUE%' then 'RUE'
                when flowsheet_name like '%LLE%' then 'LLE'
                when flowsheet_name like '%RLE%' then 'RLE'
                when flowsheet_name like 'Manual%' then 'Manual Hold'
                when flowsheet_id = 23208 --Restraint Start/Continue/Discontinue; new Non-Violent Restraint item
                then 'Universal'
            end as limb_grouper
        from
            {{ ref('flowsheet_group_lookup') }}
         where
            flowsheet_id in (
                /*Left Lower Extremity*/
                40071759, --Limb Holders LLE
                40071763, --TATs LLE
                40071767, --No-Nos LLE
                40071772, --Other Restraint LLE
                40371717, --LLE Limb Holders
                40371721, --LLE TATs
                40371725, --LLE No-Nos
                40371728,  --LLE Other Restraint
                /*Left Upper Extremity*/
                11904, --LUE Limb Holders
                40071731, --Other Restraint LUE
                40071757, --Limb Holders LUE
                40071761, --TATs LUE
                40071765, --No-Nos LUE
                40072724, --LUE Peek-a-boo mitt
                40371719, --LUE TATs
                40371723, --LUE No-Nos
                40371726, --LUE Other Restraint
                /*Right Lower Extremity*/
                40071760, --Limb Holders RLE
                40071764, --TATs RLE
                40071768, --No-Nos RLE
                40071769, --Other Restraint RLE
                40371716, --RLE Limb Holders
                40371720, --RLE TATs
                40371724, --RLE No-Nos
                40371727, --RLE Other Restraint
                /*Right Upper Extremity*/
                40071715, --RUE Limb Holders
                40071717, --Other Restraint RUE
                40071719, --RUE TATs
                40071723, --RUE No-Nos
                40071726, --RUE Other Restraint
                40071758, --Limb Holders RUE
                40071762, --TATs RUE
                40071766, --No-Nos RUE
                40072723, --RUE Peek-a-boo mitt
                /*New Non-Violent Restraint Item: Universal*/
                23208,
                /*Manual Physical Hold*/
                40371756
            )

        group by
            group_name,
            group_id,
            flowsheet_name,
            flowsheet_id,
            fs_key,
            flowsheet_name
    --end region
)

--region gather all flowsheet start, stop, and continued times
select
    flowsheet_all.visit_key,
    flowsheet_restraints.group_name,
    flowsheet_restraints.group_id,
    flowsheet_restraints.flowsheet_name,
    flowsheet_restraints.flowsheet_id,
    flowsheet_restraints.limb_grouper,
     case when lower(flowsheet_all.meas_val) = 'start' then 'Start'
          when flowsheet_all.meas_val in ('Continued', 'CONTINUE') then 'Continued'
          else 'Discontinued'
    end as meas_val,
    flowsheet_all.recorded_date
from
    {{ ref('flowsheet_all') }} as flowsheet_all
    inner join flowsheet_restraints
        on flowsheet_restraints.fs_key = flowsheet_all.fs_key
  where
        flowsheet_all.meas_val in (
            'Start',
            'Continued',
            'Discontinued',
            'START',
            'CONTINUE',
            'DISCONTINUE'
        )
