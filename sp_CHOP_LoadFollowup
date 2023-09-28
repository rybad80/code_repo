ALTER PROCEDURE "dbo"."sp_CHOP_LoadFollowup" as

/*
DECLARE @EventID as int
DECLARE @DB as int
DECLARE @DBVrsn as varchar(10)
DECLARE @casenumber as int
DECLARE @i int
DECLARE @numrows int 
DECLARE @PediperformValidationList TABLE (CaseNumber int, DB int, DBVrsn varchar(5), RowID int)

SET @DB = 9
SET @DBVrsn = '1.0'
SET @EventID = @CaseNumber
*/

SET NOCOUNT ON
			BEGIN
			/*	BEGIN TRAN
					UPDATE TARGET_TBL
					SET 
						target_tbl.mt30stat = stg_tbl.mt30stat ,
						target_tbl.Mt30StatMeth = stg_tbl.Mt30StatMeth ,
						target_tbl.mt365stat = stg_tbl.mt365stat ,
						target_tbl.Mt365StatMeth = stg_tbl.Mt365StatMeth ,
						target_tbl.MortCase = stg_tbl.mortcase ,
						target_tbl.mtopd = stg_tbl.mtopd --select *
			        FROM 
					    dbo.MortCases TARGET_TBL
					    INNER JOIN chop_sts_mortcases STG_TBL on TARGET_TBL.CaseNumber = STG_TBL.CaseNumber
					where 
					    (TARGET_TBL.mt30stat is null and stg_tbl.mt30stat is not null) or
						(TARGET_TBL.mt365stat is null and stg_tbl.mt365stat is not null)
					      


				COMMIT TRAN	
			*/	  
				BEGIN TRAN
					INSERT INTO dbo.Followup (PatID, LFUDate, LFUMortStat, CreateDate, LastUpdate, UpdateBy)
					SELECT distinct  
					  stg.patid, stg.lfudate, stg.lfumortstat, getdate(), getdate(),'CHOP_AUTOMATION'
			         FROM chop_sts_followup STG 
					      LEFT JOIN dbo.Followup TGT ON STG.Patid = TGT.PatID
                    where 
					      tgt.lfudate is null 
						  or stg.lfudate <> tgt.lfudate
 
                COMMIT TRAN

				
          END
		  ;
