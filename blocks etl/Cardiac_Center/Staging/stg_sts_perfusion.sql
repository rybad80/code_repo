select
    cast(log_id as int) as caselinknum,
    cast(cpbtm as int) as cpbtm,
    cast(xclamptm as int) as xclamptm,
    cast(dhcatm as int) as dhcatm,
    cast(tempsitebla as int) as tempsitebla,
    cast(lowctmpbla as decimal(8)) as lowctmpbla,
    cast(tempsiteeso as int) as tempsiteeso,
    cast(lowctmpeso as decimal(8)) as lowctmpeso,
    cast(tempsitenas as int) as tempsitenas,
    cast(lowctmpnas as decimal(8)) as lowctmpnas,
    cast(tempsiterec as int) as tempsiterec,
    cast(lowctmprec as decimal(8)) as lowctmprec,
    cast(tempsitetym as int) as tempsitetym,
    cast(lowctmptym as decimal(8)) as lowctmptym,
    cast(tempsiteoth as int) as tempsiteoth,
    cast(lowctmpoth as decimal(8)) as lowctmpoth,
    cast(rewarmtime as int) as rewarmtime,
    cast(cperfutil as int) as cperfutil,
    cast(cperftime as int) as cperftime,
    cast(cperfcaninn as int) as cperfcaninn,
    cast(cperfcanrsub as int) as cperfcanrsub,
    cast(cperfcanrax as int) as cperfcanrax,
    cast(cperfcanrcar as int) as cperfcanrcar,
    cast(cperfcanlcar as int) as cperfcanlcar,
    cast(cperfcansvc as int) as cperfcansvc,
    cast(cperfper as int) as cperfper,
    cast(cperfflow as int) as cperfflow,
    cast(cperftemp as int) as cperftemp,
        case
        when total_cool_minutes > 0 then 448
        else 446
    end as abldgasmgt,
    cast(hctprior as numeric(8, 2)) as hctpricirca,
    cast(cplegiadose as int) as cplegiadose,
    cast(cplegsol as int) as cplegsol,
    cast(inflwoccltm as float) as inflwoccltm,
    cast(cerebralflowtype as int) as cerebralflowtype,
    cast(cpbprimed as int) as cpbprimed,
    cast(cplegiadeliv as int) as cplegiadeliv,
    cast(cplegiatype as int) as cplegiatype,
    cast(hctfirst as decimal(3)) as hctfirst,
    cast(hctlast as decimal(3)) as hctlast,
    cast(hctpostprot as decimal(3)) as hctpost,
    cast(prbc as int) as prbc,
    cast(ffp as int) as ffp,
    cast(wholeblood as int) as wholeblood,
    cast(inducedfib as int) as inducedfib,
    cast(inducedfibtmmin as int) as inducedfibtmmin,
    cast(inducedfibtmsec as int) as inducedfibtmsec,
    coalesce(cast(total_cool_minutes as int), 0) as cooltimeprior,
    cast(ultrafilperform as int) as ultrafilperform,
    cast(ultrafilperfwhen as int) as ultrafilperfwhen,
    cast(anticoagused as int) as anticoagused,
    cast(anticoagunfhep as int) as anticoagunfhep,
    cast(anticoagarg as int) as anticoagarg,
    cast(anticoagbival as int) as anticoagbival,
    cast(anticoagoth as int) as anticoagoth,
    cast(heightcm as real) as heightcm,
    cast(weightkg as real) as weightkg,
    cast(now() as datetime) as loaddt
from
    {{ref('stg_sts_perfusion_stg')}} as staging
    left join {{ref('stg_sts_perfusion_cooling')}} as cooling
      on staging.anes_visit_key = cooling.visit_key
    left join {{ref('stg_sts_perfusion_hct')}} as hct
      on hct.log_key = staging.log_key
