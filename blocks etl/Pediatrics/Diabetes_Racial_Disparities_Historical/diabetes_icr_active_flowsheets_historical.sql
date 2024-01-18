select
    flowsheet_all.pat_key,
    flowsheet_all.patient_key,
    flowsheet_all.visit_key,
    flowsheet_all.encounter_key,
    flowsheet_all.mrn,
    flowsheet_all.patient_name,
    flowsheet_all.recorded_date,
    case
        when flowsheet_all.flowsheet_id = 7261 then 'diab_type'
        when flowsheet_all.flowsheet_id = 10060215 then 'team'
        when flowsheet_all.flowsheet_id = 15773 then 'ed visits'
        when flowsheet_all.flowsheet_id = 15778 then 'ip visits'
        when flowsheet_all.flowsheet_id = 9118 then 'endo date'
        when flowsheet_all.flowsheet_id = 7251 then 'dx date'
        when flowsheet_all.flowsheet_id = 10060217 then 'a1c'
        when flowsheet_all.flowsheet_id = 9403 then 'np'
        end as fs_type,
    case --updated FLO logic ON 8/3/22, replaced BY NEW SDE since CY23
        when fs_type != 'np' then flowsheet_all.meas_val --noqa: L028
        when flowsheet_all.meas_val = '16' then 'LIPMAN'
        when flowsheet_all.meas_val = '25' then 'MOSER'
        when flowsheet_all.meas_val = '26' then 'MONTGOMERY'
        when flowsheet_all.meas_val = '39' then 'DOUGHERTY'
        when flowsheet_all.meas_val = '43' then 'BUZBY'
        when flowsheet_all.meas_val = '62' then 'DEA'
        when flowsheet_all.meas_val = '75' then 'ATTENDING'
        when flowsheet_all.meas_val = '85' then 'FELLOW'
        when flowsheet_all.meas_val = '77' then 'MINNOCK'
        when flowsheet_all.meas_val = '8' then 'REARSON'
        when flowsheet_all.meas_val = '80' then 'LEBOEUF'
        when flowsheet_all.meas_val = '81' then 'ATH'
        when flowsheet_all.meas_val = '82' then 'PINES'
        when flowsheet_all.meas_val = '83' then 'DEVER'
        when flowsheet_all.meas_val = '84' then 'MAROWITZ'
        when flowsheet_all.meas_val = '86' then 'LIPINSKI'
        when flowsheet_all.meas_val = '87' then 'MCLOUGHLIN'
        when flowsheet_all.meas_val = '88' then 'MEIGHAN'
        else 'UNASSIGNED'
    end as meas_val
from
    {{ ref('flowsheet_all')}} as flowsheet_all
where
    flowsheet_all.flowsheet_id in (7261,    --type of diabetes
                    10060215, --diabetes team
                    15773, --# of episodes
                    15778, --# of episodes
                    9118, --year of diagnosis (yyyy)
                    7251, --date of diagnosis
                    10060217, --most recent a1c value
                    9403 --primary diabetes provider
                    )
    --LAST DATA reload date OF diabetes_icr_active_flowsheets_historical:
    and flowsheet_all.recorded_date <=  '2023-02-09'
    --icr flowsheets has launched since 2012, identify active patients since 2011:
    and flowsheet_all.recorded_date >=  '2011-01-01'
group by
    flowsheet_all.pat_key,
    flowsheet_all.patient_key,
    flowsheet_all.visit_key,
    flowsheet_all.encounter_key,
    flowsheet_all.mrn,
    flowsheet_all.patient_name,
    flowsheet_all.recorded_date,
    fs_type, --noqa: L028
    meas_val --noqa: L028
