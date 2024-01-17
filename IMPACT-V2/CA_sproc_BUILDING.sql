 SELECT  top 1
    SUBSTRING(
        (
            SELECT ','+C1.NAME  AS [text()]
            from sys.columns c1 join sys.tables t1 on c1.object_id = t1.object_id
            WHERE 1=1
		--	and C1.NAME = C2.NAME
			AND t1.name = 'CATHHISTORY'
            ORDER BY C1.column_id
            FOR XML PATH ('')
        ), 2, 1000) NA
FROM  sys.columns c2 join sys.tables T2 on c2.object_id = t2.object_id
WHERE t2.name = 'CATHHISTORY'  
order by c2.column_id


select 'CATH_TRGT.'+ cast(c.name as nvarchar(255)) +'=CATH_SRC.'+ cast(c.name as nvarchar(255))+','
from sys.columns c join sys.tables t on c.object_id = t.object_id
where t.name = 'chop_impact_CATHHISTORY'
 and c.name <> 'SURG_ENC_ID'
 and c.name <> 'LOADDT'
 and c.name <> 'MD5'
 and c.name <> 'PENDINGIMPORT'


 SELECT C.NAME
 from sys.columns c join sys.tables t on c.object_id = t.object_id
where t.name = 'chop_impact_CATHHISTORY'
 and c.name <> 'SURG_ENC_ID'
 and c.name <> 'LOADDT'
 and c.name <> 'MD5'
 and c.name <> 'PENDINGIMPORT'

 SELECT DISTINCT 
    SUBSTRING(
        (
            SELECT ','+C1.NAME  AS [text()]
            from sys.columns c1 join sys.tables t1 on c1.object_id = t1.object_id
            WHERE 1=1
		--	and C1.NAME = C2.NAME
			AND t1.name = 'chop_impact_CATHHISTORY'
			 and c1.name <> 'SURG_ENC_ID'
			 and c1.name <> 'LOADDT'
			 and c1.name <> 'MD5'
			 and c1.name <> 'PENDINGIMPORT'
            ORDER BY C1.NAME
            FOR XML PATH ('')
        ), 2, 1000) NA
FROM  sys.columns c2 join sys.tables T2 on c2.object_id = t2.object_id
WHERE t2.name = 'chop_impact_CATHHISTORY'
			 and c2.name <> 'SURG_ENC_ID'
			 and c2.name <> 'LOADDT'
			 and c2.name <> 'MD5'
			 and c2.name <> 'PENDINGIMPORT'

			 
select * from chop_impact_CATHHISTORY

SELECT * FROM CATHHISTORY
WHERE CATHID > 11968

