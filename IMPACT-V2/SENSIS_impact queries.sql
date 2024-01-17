SELECT *
FROM STUDY JOIN ip2ppt ON STUDY.REFNO = ip2ppt.refno
           JOIN ip2rvot on study.refno = ip2rvot.refno
		   JOIN ip2cart ON STUDY.REFNO = IP2CART.REFNO
--where PATNO = '01234567'


SELECT      c.name  AS 'ColumnName'
            ,t.name AS 'TableName' --
			select distinct ','+c.name
FROM        sys.columns c
JOIN        sys.tables  t   ON c.object_id = t.object_id
WHERE       UPPER(c.name) LIKE 'ip%' or UPPER(c.name) LIKE 'i1%' 
ORDER BY    1



SELECT      distinct
            t.name AS 'TableName'
FROM        sys.columns c
JOIN        sys.tables  t   ON c.object_id = t.object_id
WHERE       UPPER(c.name) LIKE '%3045%' 


SELECT      distinct
            t.name AS 'TableName',
			C.NAME AS "COLUMN"
FROM        sys.columns c
JOIN        sys.tables  t   ON c.object_id = t.object_id
WHERE       UPPER(C.name) LIKE '%FLTIM%'

dicip% or dici% tables for dictionaries

select * from dici11010

select * from iphxrsk

SELECT      distinct
            t.name AS 'TableName'
FROM        sys.columns c
JOIN        sys.tables  t   ON c.object_id = t.object_id
WHERE       UPPER(C.name) LIKE 'ip3245'.
Ambiguous column name 'ip4000'.
Ambiguous column name 'ip5070'.
Ambiguous column name 'ip7025'.
Ambiguous column name 'ip7230'.
Ambiguous column name 'ip7705'.
Invalid column name 'ip7090'.
\
select *
 from  podg 
		--	left join prdg ON STUDY.REFNO = prdg.REFNO

		select name
		from sys.tables
		where tables.name like '%dicip%' or tables.name like '%dici1%'


		select *
		 from study
