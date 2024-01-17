
library(ODBC)
con <- dbConnect(odbc::odbc(),
                 Driver = "SQLServer",
                 Server = "PSALA012",
                 Database = "Centripetus",
                 UID = "cardio",
                 PWD = "access")
                 Port = 1433)

query <- ("


      SELECT  C.CASENUMBER,
              C.CASELINKNUM,
              C.UPDATEBY,
              MEDRECN,
              C.SURGDT,
              STG.*,
              TGT.*
              
       FROM CASES C JOIN DEMOGRAPHICS D ON C.PATID = D.PATID
              LEFT JOIN CHOP_CCAS_ANESTHESIA_NIRS STG ON STG.CASELINKNUM = C.CASELINKNUM
              LEFT JOIN NIRS TGT ON TGT.CASENUMBER = C.CASENUMBER
      WHERE  C.CASELINKNUM IS NOT NULL
             AND STG.CASELINKNUM IS NOT NULL
             AND TGT.CaseNumber IS NULL
")


send_case_check_query <- dbSendQuery(con, query)

case_check <- dbFetch(send_case_check_query)

