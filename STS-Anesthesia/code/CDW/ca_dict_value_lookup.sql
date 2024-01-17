select *
from globallookup
where shortname = (select distinct shortname
				     from globallookup
					where id in ( 'IntNerveInf'
								 )
				   )


				   select Section,
				          FieldName,
						  CATblName,
						  CAFldName --SELECT *
				   from  DBSpecs
				   where DataVrsn = '3.3'
				     and CATblName like 'Procoagulants'

SELECT * FROM AnesthesiaTechnique

 select t.name,c.name
  from sys.tables t join sys.columns c on t.object_id = c.object_id
  where t.name in
  ('AnesthesiaAirway',
'AnesthesiaMonitor',
'AnesthesiaPostopICU',
'AnesthesiaPreopMeds',
'AnesthesiaTechnique',
'Cases',
'Procoagulants',
'Transfusion'
)
order by 1
