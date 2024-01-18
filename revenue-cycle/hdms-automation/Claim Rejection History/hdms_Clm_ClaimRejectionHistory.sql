SELECT REJ.CLAIMNUMBER
,REJ.DATEOFSERVICE
,REJ.HCPC
,REJ.QUANTITY
,REJ.ITEMTYPE
,REJ.ITEMTYPEDESC
,REJ.PROVIDERIDENTIFIER
,REJ.PROVIDERNAME
,REJ.REJECTIONRESPONSEDATE
,REJ.REJECTION1CODE
,REJ.REJECTION1DESCRIPTION
,REJ.REJECTION1NARRATIVE
,REJ.REJECTION2CODE
,REJ.REJECTION2DESCRIPTION
,REJ.REJECTION2NARRATIVE
,REJ.REJECTION3CODE
,REJ.REJECTION3DESCRIPTION
,REJ.REJECTION3NARRATIVE
,REJ.LASTREJECTIONTRANSDATE
,REJ.PAYERIDENTIFIER
,REJ.PAYERNAME
,REJ.BILLABLEASOFDATE
,REJ.LINEITEMBALANCEONCLAIM
,REJ.LASTSUBMITDATE
,REJ.LASTNOTEDATE
,REJ.ACCOUNT
,REJ.PATIENTLASTNAME
,REJ.PATIENTMEDICALRECORDNUMBER
,REJ.COLLECTIONFOLLOWUPDATE
,REJ.COLLECTIONSTATUS
,REJ.COLLECTIONSTATUSCHANGEDBY
,REJ.COLLECTIONSTATUSCHANGEDDATE
,REJ.UPD_DT
FROM cdw_ods.admin.HDMS_DS_CLM_CLAIMREJECTIONHISTORY REJ