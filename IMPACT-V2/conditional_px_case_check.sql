select c.cathid,
case when PROCDXCATH = 1 then (case when px.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533,535,540,1055,1060,1065,1070,1075,1080,1085,1090,1095,1100,1105,1110,1115,1120,1125,1130,1135) then 1 else 2 end) else null end as procdxcath_chk,
case when PROCASD = 1 then (case when  px.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533,1115) then 1 else 2 end) else null end as procasd_chk,
case when PROCCOARC = 1 then (case when   px.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533) then 1 else 2 end) else null end as proccoarc_chk,
case when PROCAORTICVALV = 1 then (case when   px.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533) then 1 else 2 end) else null end as procaorticvalv_chk,
case when ProcPulmonaryValv = 1  then (case when  px.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533) then 1 else 2 end) else null end as procpulmonaryvalv_chk,
case when ProcPDA = 1 then (case when   px.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533) then 1 else 2 end) else null end as procpda_chk,
case when ProcProxPAStent = 1 then (case when  px.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533,1235,1240) then 1 else 2 end) else null end as procproxpastent_chk,
case when ProcEPCath = 1 then (case when  px.SpecificProcID IN (5,10,15,20,25,531,532,533) then 1 else 2 end)  else null end as procepcath_chk,
case when ProcEPAblation = 1  then (case when  px.SpecificProcID IN (5,10,15,20,25,531,532,533) then 1 else 2 end)  else null end as procepablation_chk,
case when ProcTPVR = 1  then (case when px.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533,1565) then 1 else 2 end) else null end as proctpvr_chk,
case when (PROCDXCATH = 2  AND PROCASD = 2 and PROCCOARC = 2 AND PROCAORTICVALV = 2 AND ProcPulmonaryValv = 2 AND ProcPDA = 2
		 and ProcProxPAStent = 2 AND ProcEPCath = 2 AND ProcEPAblation = 2 AND ProcTPVR = 2 and px.SpecificProcID =  px.SpecificProcID) then 1 else null end as procdxnone_chk

from cathdata c join cathprocedures px on c.cathid = px.cathid

select * from cathdata where cathid = 13698
select * from cathprocedures where cathid = 13698

select * from CHOP_IMPACT_CATHPROCEDURES where surg_enc_id = 2062118293