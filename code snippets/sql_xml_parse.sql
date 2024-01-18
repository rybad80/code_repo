select 
     ID,
	 UserName,
	 Action,
	 TableName,
	 PatID,
	 EventID,
	 DateTimestamp,
	 OriginalValue.value('(Case/CaseLinkNum)[1]','varchar(10)') as OldCaseLinkNum,
	 NewValue.value('(Case/CaseLinkNum)[1]','varchar(10)') as NewCaseLinkNum
from
    AuditTable