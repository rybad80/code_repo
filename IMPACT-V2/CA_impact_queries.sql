
  select v1.*,
         v2.*,
		 case when v1.seqno <> v2.seqno then v1.seqno end as prior_seq_no
  from 

  (select SeqNo,
         FieldName,
		 ShortName,
		 CATblName,
		 CAFldName,
		 datavrsn

  from dbspecs
  where db = 4
    and datavrsn = 1.0 and seqno = 5010
) v1

full outer join

  (select SeqNo,
         FieldName,
		 ShortName,
		 CATblName,
		 CAFldName,
		 datavrsn

  from dbspecs
  where db = 4
    and datavrsn = 2.0
) v2

on (v1.cafldname = v2.CAFldName and v1.catblname = v2.catblname) or v1.seqno = v2.seqno
where ISNULL(v1.seqno,'') <> ISNULL(v2.seqno,'')
order by v2.seqno