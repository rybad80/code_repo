 select t.name,c.name
  from sys.tables t join sys.columns c on t.object_id = c.object_id
  where c.name in
  ('PDAProcInd')

select id, acccode
from globallookup
where shortname IN (select DISTINCT SHORTNAME
				     from globallookup
					where id in (select PDAResShunt
								   from cathpdaclosure 
								 )
				   )




				   select Section,
				          FieldName,
						  CATblName,
						  CAFldName --SELECT *
				   from  DBSpecs
				   where --DataVrsn = '3.3'
				     and CATblName like ''

SELECT * FROM CathDeviceAssn


order by 1


