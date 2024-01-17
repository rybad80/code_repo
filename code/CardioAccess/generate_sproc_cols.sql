SELECT 'ANEST_TRGT.'+c.name+'=ANEST_SRC.'+c.name+',' --select *
from sys.columns c join sys.tables t on c.object_id = t.object_id
where t.name = 'AnesthesiaMonitor'




SELECT c.name+',' --select *
from sys.columns c join sys.tables t on c.object_id = t.object_id
where t.name = 'AnesthesiaMonitor'




