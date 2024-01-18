with pat_curr_wt as (
select distinct
         cath_study.refno,
         cath_study.pat_key,
         studate,
         first_value(flowsheet_vitals.weight_kg ignore nulls) over
         (partition by cath_study.refno order by recorded_date desc rows
                    between unbounded preceding and unbounded following)
         as pat_wt
from
    {{ref('stg_impact_cathstudy')}} as cath_study
    inner join {{ref('flowsheet_vitals')}} as flowsheet_vitals
       on cath_study.pat_key = flowsheet_vitals.pat_key
       and studate > recorded_date
where weight_kg is not null
),

pat_curr_ht as (
select   distinct
         cath_study.refno,
         cath_study.pat_key,
         studate,
         first_value(flowsheet_vitals.height_cm ignore nulls) over
         (partition by cath_study.refno order by recorded_date desc rows
                    between unbounded preceding and unbounded following)
         as pat_ht
from
    {{ref('stg_impact_cathstudy')}} as cath_study
    inner join {{ref('flowsheet_vitals')}} as flowsheet_vitals
       on cath_study.pat_key = flowsheet_vitals.pat_key
       and studate > recorded_date
where height_cm is not null
),

proc_perf as (
select cath.pat_key,
        cath.studate,
        cath.refno,
         pedpp.ip5000 as procdxcath,
         pedpp.ip5001 as procasd,
         pedpp.ip5002 as proccoarc,
         pedpp.ip5003 as procaorticvalv,
         pedpp.ip5004 as procpulmonaryvalv,
         pedpp.ip5005 as procpda,
         pedpp.ip5006 as procproxpastent,
         pedpp.ip5007 as procepcath,
         pedpp.ip5008 as procepablation,
         pedpp.ip5009 as proctpvr,
         pedpp.ip5010 as procother
 from
      {{ref('stg_impact_cathstudy')}} as cath
      inner join {{source('ccis_ods', 'sensis_pedpp')}} as pedpp
         on cath.refno = pedpp.refno
),

airway as (
select
         cath.refno,
         ip5071 as airmng,
         max(case when ip5075 = '2' then '1' else '2' end ) as airmnglma,
         max(case when ip5075 = '3' then '1' else '2' end ) as airmngtrach,
         max(case when ip5075 = '4' then '1' else '2' end ) as airmngbagmask,
         max(case when ip5075 = '5' then '1' else '2' end ) as airmngcpap,
         max(case when ip5075 = '6' then '1' else '2' end ) as airmngelecintub,
         max(case when ip5075 = '7' then '1' else '2' end ) as airmngprevintub,
         row_number() over (partition by cath.refno order by airmng) as airway_row
 from
      {{ref('stg_impact_cathstudy')}} as cath
      inner join {{source('ccis_ods', 'sensis_imarwy')}} as imarwy
         on cath.refno = imarwy.refno
      left join {{source('ccis_ods', 'sensis_dicip5075')}} as ip5075
         on imarwy.ip5075 = ip5075.code
 group by
      cath.refno, ip5071
),

vein_artery as (
select
      cath.refno,
      max(case when upper(site.meaning) like '%vein%' then 1 else 0 end) as vein,
      max(case when upper(site.meaning) like '%artery%' then 1 else 0 end) as artery
from
      {{ref('stg_impact_cathstudy')}} as cath
      inner join {{source('ccis_ods', 'sensis_asr')}} as asr
            on cath.refno = asr.refno
      inner join {{source('ccis_ods', 'sensis_dicshesiz')}} as dicshesiz
            on asr.shesize = dicshesiz.code
      left join {{source('ccis_ods', 'sensis_dices1te')}} as site
            on site.code = asr.entsit
group by
      cath.refno
),

vein_sheath as (
select
      cath.refno,
      site.meaning,
      asr.entsit,
      shtime,
      asr.seqno,
      case
      when length(regexp_replace(trim(both ' '
                  from substring(dicshesiz.meaning, instr(dicshesiz.meaning, 'f ') - 2, 2)), '[^0-9]', '')) >=  1
      then cast(regexp_replace(trim(both ' '
                  from substring(dicshesiz.meaning, instr(dicshesiz.meaning, 'f ') - 2, 2)), '[^0-9]', '')
           as integer)
      else 0 end as venlargsheath
from
      {{ref('stg_impact_cathstudy')}} as cath
      inner join {{source('ccis_ods', 'sensis_asr')}} as asr
        on cath.refno = asr.refno
      inner join {{source('ccis_ods', 'sensis_dicshesiz')}} as dicshesiz
        on asr.shesize = dicshesiz.code
      left join {{source('ccis_ods', 'sensis_dices1te')}} as site
        on site.code = asr.entsit
where
      upper(site.meaning) like '%vein%'
),

access as (
select
      refno,
      entsit,
      venlargsheath,
      row_number() over (partition by refno order by venlargsheath desc, shtime, seqno) as sizeorder
from
    vein_sheath
),

venous_loc as (
select  vein_artery.refno,
        case when vein + artery = 2 then 1456
             when vein = 1 then 1454
             when artery = 1 then 1455
             else null end as accessloc,
        case when entsit = 1 then 1564
             when entsit = 2 then 1565
             when entsit = 3 then 1566
             when entsit = 4 then 1567
             when entsit = 5 then 1568
             when entsit = 6 then 1569
             when entsit = 7 then 1570
             when entsit = 8 then 1571
             when entsit = 9 then 1572
             when entsit = 10 then 1573
             when entsit = 11 then 1574
         else 1575 end as venaccess,
        venlargsheath as venlargsheath,
        2 as venclosuremethodnd,
        coalesce(sizeorder, 1) as sizeorder
   from
     vein_artery
     left join access on vein_artery.refno = access.refno
),

arterial_sheath as (
select cath.refno,
      site.meaning,
      asr.entsit,
      shtime,
      asr.seqno,
      --,dicshesiz.meaning shesize
      case when length(regexp_replace(trim(both ' '
             from substring(dicshesiz.meaning, instr(dicshesiz.meaning, 'f ') - 2, 2)), '[^0-9]', '')) >=  1
           then cast(regexp_replace(trim(both ' '
             from substring(dicshesiz.meaning, instr(dicshesiz.meaning, 'f ') - 2, 2)), '[^0-9]', '') as integer)
           else 0 end as artlargsheath
from
      {{ref('stg_impact_cathstudy')}} as cath
      inner join {{source('ccis_ods', 'sensis_asr')}} as asr
         on cath.refno = asr.refno
      inner join {{source('ccis_ods', 'sensis_dicshesiz')}} as dicshesiz
         on asr.shesize = dicshesiz.code
      left join {{source('ccis_ods', 'sensis_dices1te')}} as site
         on site.code = asr.entsit
where
      upper(site.meaning) like '%artery%'
),

arterial as (
select
      refno,
      entsit,
      artlargsheath,
      row_number() over (partition by refno order by artlargsheath desc, shtime, seqno) as sizeorder
from
    arterial_sheath

),

arterial_loc as (
select
      refno,
      case when entsit = 12 then 1457
           when entsit = 13 then 1458
           when entsit = 14 then 1459
           when entsit = 15 then 1460
           when entsit = 16 then 1461
           when entsit = 17 then 1462
           when entsit = 18 then 1463
           when entsit = 19 then 1464
           when entsit = 20 then 1465
           when entsit = 21 then 1466
      end as artaccess,
      artlargsheath,
      2 as artclosuremethodnd,
      sizeorder
from
    arterial
),

xray_plane as (
select
         study.refno,
         max(case when xray.plane = 'a' then 1 else 0 end) as planea,
         max(case when xray.plane = 'b' then 1 else 0 end) as planeb
  from
      {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_xray')}} as xray
         on study.refno = xray.refno
  group by study.refno
),

heparin as (
select
         hep.refno
  from
  (
  select me.refno,
         me.autotime,
         me.medica,
         me.amount
  from {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_me')}} as me
          on study.refno = me.refno
  where
       medica in (8, 26)

  union

  select za.refno,
         za.autotime,
         za.medica,
         za.amount
  from {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_za')}} as za
          on study.refno = za.refno
  where
       medica in (8, 26)
  ) as hep
group by hep.refno
),

act as (
select
       act.refno,
       max(act.act) as actpeak
  from {{ref('stg_impact_cathstudy')}} as study
       inner join {{source('ccis_ods', 'sensis_act')}} as act
          on study.refno = act.refno
group by
       act.refno

),

asr as (
select
       refno,
       min(shtime) as shtime
from
     {{source('ccis_ods', 'sensis_asr')}}
group by
     refno
),

angio as (
select
       refno,
       round(sum(cost), 0) as tot_cost
  from
     {{source('ccis_ods', 'sensis_angio')}}
group by
       refno
),

phys as (
select *,
       row_number() over (partition by refno order by seqno) as row
  from
      {{source('ccis_ods', 'sensis_phys')}}
),

second_phys as (
select refno,
       count(npi1) as multiphys
  from
     {{source('ccis_ods', 'sensis_phys')}}
group by
      refno
),

cath_procedure as (
select  cath.pat_key,
         cath.studate,
         cath.refno,
         case when aptver2.ptstat = 2 then 1522
              when aptver2.ptstat = 3 then 1523
              when aptver2.ptstat = 4 then 1524
              when aptver2.ptstat = 5 then 1525
              when aptver2.ptstat = 6 then 1526
              when aptver2.ptstat = 7 then 1527
        else null end as hospstatus,
         case when aptver2.dprstat = 1 then 982
              when aptver2.dprstat = 2 then 983
              when aptver2.dprstat = 3 then 984
              when aptver2.dprstat = 4 then 985
        else null end as procstatus,
         phys.doper as operatorname,
         phys.npi1 as operatornpi,
         case when isnull(second_phys.multiphys, 0) = 1 then 2
              when isnull(second_phys.multiphys, 0) > 1 then 1
         else null end as secondparticipating,
         aptver2.ip5025 as trainee,
         ct.caseid as aux5,
         ct.patim as schedarrivaldate,
         asr.shtime as procstartdate,
         asr.shtime as procstarttime,
         cast(poct.pltim as date) as procenddate,
         poct.prend as procendtime,
         aptver2.ip5060 as anespresent,
         case when aptver2.ip5060 = 1 then null
              when aptver2.ip5060 = 2 and aptver2.ip5065 > 1 then 2
              when aptver2.ip5060 = 2 and aptver2.ip5065 = 1 then 1
              else null end as anescalledin,
         case when aptver2.ip5070 = 1 then 1557
              when aptver2.ip5070 = 2 then 1558
              when aptver2.ip5070 = 3 then 1559
              when aptver2.ip5070 = 4 then 1560
              when aptver2.ip5070 = 5 then 1561
              when aptver2.ip5070 = 6 then 1562
              when aptver2.ip5070 = 7 then 1563
         else null end as sedation,
         airmng,
         airmnglma,
         airmngtrach,
         airmngbagmask,
         airmngcpap,
         airmngelecintub,
         airmngprevintub,
         xraybsum.fltime as fluorotime,
         angio.tot_cost as contrastvol,
         aptver3.ip5160 as inotrope,
         case when aptver3.ip5165 = 1 then 1528
              when aptver3.ip5165 = 2 then 1529
              when aptver3.ip5165 = 3 then 1530
              when aptver3.ip5165 = 4 then 1531
              when aptver3.ip5165 = 5 then 1532
         else null end as inotropeuse,
         case when aptver3.ip5170 = 548 then 1515
              when aptver3.ip5170 = 549 then 1516
              when aptver3.ip5170 = 551 then 1517
        else null end as ecmouse,
         case when aptver3.ip5175 = 548 then 1515
              when aptver3.ip5175 = 549 then 1516
              when aptver3.ip5175 = 551 then 1517
        else null end as lvaduse,
         case when aptver3.ip5180 = 548 then 1515
              when aptver3.ip5180 = 549 then 1516
              when aptver3.ip5180 = 551 then 1517
        else null end as iabpuse,
         case when xray_plane.planea + xray_plane.planeb = 2 then 4090
              when xray_plane.planea + xray_plane.planeb = 1 then 4089
              else null end as planeused,
         cast(xraybsum.dose as integer) as fluorodosedap,
         4084 as fluorodosedap_units,
         cast(xraybsum.sknds as integer) as fluorodosekerm,
         4087 as fluorodosekerm_units
from
     {{ref('stg_impact_cathstudy')}} as cath
     left join {{source('ccis_ods', 'sensis_aptver2')}} as aptver2
        on cath.refno = aptver2.refno
     left join {{source('ccis_ods', 'sensis_aptver3')}} as aptver3
        on cath.refno = aptver3.refno
     left join phys
        on phys.refno = cath.refno and row = 1
     left join second_phys
        on second_phys.refno = cath.refno
     left join {{source('ccis_ods', 'sensis_ct')}} as ct
        on cath.refno = ct.refno
     left join {{source('ccis_ods', 'sensis_poct')}} as poct
        on cath.refno = poct.refno
     left join airway
        on cath.refno = airway.refno
          and airway_row = 1
     left join {{source('ccis_ods', 'sensis_xraybsum')}} as xraybsum
        on cath.refno = xraybsum.refno
     left join angio
        on cath.refno = angio.refno
     left join asr
        on asr.refno = cath.refno
     left join xray_plane
        on cath.refno = xray_plane.refno

),

recorder as (
select
         pn.refno,
         min(dicpnname.code) as recorder
   from
        {{source('ccis_ods', 'sensis_pn')}} as pn
        left join {{source('ccis_ods', 'sensis_dicpnname')}} as dicpnname
           on pn.pnname = dicpnname.code
  where
        pn.staff = 6
 group by
        pn.refno
)


select
       surg_enc_id,
       cath_case_id,
       procdxcath,
       procasd,
       cast(coalesce(height, ht.pat_ht) as numeric(5, 2)) as height,
       cast(coalesce(weight, wt.pat_wt) as numeric(5, 2)) as weight,
       preprochgb,
       preproccreat,
       preproco2,
       nec,
       sepsis,
       preg,
       preprocantiarr,
       preprocanticoag,
       preprocantihyp,
       preprocantiplatelet,
       preprocbb,
       preprocdiuretic,
       preprocprosta,
       preprocvaso,
       preprocsinus,
       preprocaet,
       preprocsvt,
       preprocafib,
       preprocjunct,
       preprocidio,
       preprocavb2,
       preprocavb3,
       preprocpaced,
       proccoarc,
       procaorticvalv,
       procpulmonaryvalv,
       procpda,
       procproxpastent,
       hospstatus,
       procstatus,
       trainee,
       operatorname as operatorid,
       anespresent,
       anescalledin,
       sedation,
       case when airmng = 1 then airmnglma else null end as airmnglma,
       case when airmng = 1 then airmngtrach else null end as airmngtrach,
       case when airmng = 1 then airmngbagmask else null end as airmngbagmask,
       case when airmng = 1 then airmngcpap else null end as airmngcpap,
       case when airmng = 1 then airmngelecintub else null end as airmngelecintub,
       case when airmng = 1 then airmngprevintub else null end as airmngprevintub,
       accessloc,
       case when accessloc in (1456, 1454) then venaccess else null end as venaccess,
       case when accessloc in (1456, 1454) then venlargsheath else null end as venlargsheath,
       case when accessloc in (1456, 1454) then venclosuremethodnd else null end as venclosuremethodnd,
       case when accessloc in (1456, 1455) then artaccess else null end as artaccess,
       case when accessloc in (1456, 1455) then artlargsheath else null end as artlargsheath,
       case when accessloc in (1456, 1455) then artclosuremethodnd else null end as artclosuremethodnd,
       cast(fluorotime as numeric(4, 1)) as fluorotime,
       coalesce(cast(contrastvol as integer), 0) as contrastvol,
       case when not(heparin.refno is null) then 1
            else 2
            end as sysheparin,
       case when not(heparin.refno is null) and not(actpeak is null) then 1
            when not(heparin.refno is null) and actpeak is null then 2
                else null end as actmonitor,
       case when not(heparin.refno is null) then actpeak else null end as actpeak,
       inotrope,
       inotropeuse,
       ecmouse,
       lvaduse,
       aux5,
       schedarrivaldate,
       procother,
       recorder.recorder,
       preprochgbnd,
       preproccreatnd,
       svdefect,
       preprocmed,
       procepcath,
       procepablation,
       proctpvr,
       secondparticipating,
       procstartdate,
       procstarttime,
       procenddate,
       procendtime,
       airmng,
       iabpuse,
       planeused,
       fluorodosekerm,
       fluorodosekerm_units,
       fluorodosedap,
       fluorodosedap_units
from
    {{ref('stg_impact_cathstudy')}} as cath
     left join pat_curr_wt as wt
        on cath.refno = wt.refno
     left join pat_curr_ht as ht
        on cath.refno = ht.refno
     left join {{ref('stg_impact_cathdata_preproc')}} as stg_preproc
        on stg_preproc.refno = cath.refno
     left join proc_perf on cath.refno = proc_perf.refno
     left join cath_procedure on cath.refno = cath_procedure.refno
     left join venous_loc
        on cath.refno = venous_loc.refno
        and venous_loc.sizeorder = 1
     left join arterial_loc
        on cath.refno = arterial_loc.refno
         and arterial_loc.sizeorder = 1
     left join heparin on heparin.refno = cath.refno
     left join act on act.refno = cath.refno
     left join recorder on recorder.refno = cath.refno
