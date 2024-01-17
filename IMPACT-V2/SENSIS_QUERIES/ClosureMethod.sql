select refno
     ,case 
   	     when ip5100 = 772 then 1
         when ip5100 = 773 then 2
	     when ip5100 = 774 then 3
	     when ip5100 = 775 then 9
	     when ip5100 = 776 then 72
		 else null end as closure_method
     ,row_number() over (partition by refno order by ip5100) sort		 --select *
from cdw_ods..sensis_asr asr
     left join cdw_ods..sensis_dices1te site on asr.entsit = site.code
where 
     ip5100 is not null
	 and lower(meaning) like '%vein%'
group by
     refno,
	 ip5100
	 
	 
	 
	 select 
	     refno
        ,case 
   	       when ip5100 = 772 then 1
           when ip5100 = 773 then 2
	       when ip5100 = 774 then 3
	       when ip5100 = 775 then 9
	       when ip5100 = 776 then 72
		 else null end as closure_method
     ,row_number() over (partition by refno order by ip5100) sort		 --select *
from cdw_ods..sensis_asr asr
     left join cdw_ods..sensis_dices1te site on asr.entsit = site.code
where 
     ip5100 is not null
	 and lower(meaning) like '%artery%'
 group by
     refno,
	 ip5100
