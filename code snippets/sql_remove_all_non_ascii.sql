set quoted_identifier on

with tbl as (select * from <yourtable>)

,RunningNumbers AS
(
    SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Nmbr
    FROM sys.objects
)
,SingleChars AS
(
    SELECT <tbl.pk_col>,rn.Nmbr,SUBSTRING(<tbl.col_with_non_ascii>,rn.Nmbr,1) AS Chr
    FROM tbl
    CROSS APPLY (SELECT TOP(LEN(<tbl.col_with_non_ascii>)) Nmbr FROM RunningNumbers) AS rn 
)
SELECT <tbl.pk_col>
      ,(
        SELECT '' + Chr 
        FROM SingleChars AS sc
        WHERE <sc.pk_col>=<tbl.pk_col> AND ASCII(Chr)<128
        ORDER BY sc.Nmbr
        FOR XML PATH(''),TYPE
      ).value('(text())[1]','varchar(8000)') AS GoodString
FROM tbl


with
x as
(select'éèçàqsdfù' badstring
)

select
regexp_replace(badstring,'['+char(128)+'-'+char(255)+']')
from
x