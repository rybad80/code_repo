select * from chop_impact_cathprocedures 
--where md5 = 'DELETED'
where surg_enc_id = 2061174817

select * from cathprocedures
where cathid = (select cathid from cathdata where aux5 = '19-0761')

select * from  cathdata
where emreventid = 2061174817

select count(*) from cathprocedures
--19011
--19075
--19078
--19077


update chop_impact_cathprocedures set pendingimport = 9 where surg_enc_id = 2061174817 and md5 = 'DELETED'
INSERT INTO cathprocedures (cathid, specificprocid, procedurename, sort)
SELECT 12036, 531, 'tEST insert', 4
insert into chop_impact_cathprocedures (surg_enc_id, specificprocid, loaddt, md5, pendingimport)
SELECT 2061174817, 531, getdate(), 'A1B2C3', 1
update chop_impact_cathprocedures set pendingimport = 9 where surg_enc_id = 2061174817 and specificprocid = 531
update chop_impact_cathprocedures set MD5 = 'DELETED' where surg_enc_id = 2061174817 and specificprocid = 531

select t.name tblname, 
       c.name colname
from sys.columns c join sys.tables t on c.object_id = t.object_id
where c.name = 'SORT'
 and t.name like 'CHOP_IMPACT%'