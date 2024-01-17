USE [Centripetus]
GO
/****** Object:  StoredProcedure [dbo].[sp_CHOP_IMPACT_CATHPATANATOMY]    Script Date: 5/10/2021 1:58:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

ALTER PROCEDURE [dbo].[sp_CHOP_IMPACT_CATHPATANATOMY] 

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @DBVrsn varchar(5), @PAT_MRN_ID int, @PatID int
DECLARE @CathCaseList TABLE (PatID int, PAT_MRN_ID int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int

exec sp_CHOP_IMPACT_BIRTHINFO;
							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(PatID, PAT_MRN_ID, DBVrsn, RowID)
	SELECT d.PatID, CATH_SRC.PAT_MRN_ID, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY d.PatID) 
	FROM [dbo].[CHOP_IMPACT_CATHPATANATOMY] CATH_SRC INNER JOIN dbo.Demographics d on d.medrecn = CATH_SRC.PAT_MRN_ID
	                                                 INNER JOIN (select patid, max(impactdatavrsn) IMPACTDataVrsn from Hospitalization group by patid) H ON h.PatID = d.PatID
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	--AND PAT_MRN_ID = '55786857'

	ORDER BY D.PatID


		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @PatID = PatID, @PAT_MRN_ID = PAT_MRN_ID, @DBVrsn = DBVrsn  FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.PatID FROM CATHPATANATOMY ASD WHERE ASD.PatID = @PatID)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET CATH_TRGT.HospitalID = 4,
							    CATH_TRGT.PatID = AC.PatID,
								CATH_TRGT.DIGEORGESYND=CATH_SRC.DIGEORGESYND,
								CATH_TRGT.ALAGILLESYND=CATH_SRC.ALAGILLESYND,
								CATH_TRGT.HERNIA=CATH_SRC.HERNIA,
								CATH_TRGT.MARFANSYND=CATH_SRC.MARFANSYND,
								CATH_TRGT.DOWNSYND=CATH_SRC.DOWNSYND,
								CATH_TRGT.HETEROTAXY=CATH_SRC.HETEROTAXY,
								CATH_TRGT.NOONANSYND=CATH_SRC.NOONANSYND,
								CATH_TRGT.RUBELLA=CATH_SRC.RUBELLA,
								CATH_TRGT.TRISOMY13=CATH_SRC.TRISOMY13,
								CATH_TRGT.TRISOMY18=CATH_SRC.TRISOMY18,
								CATH_TRGT.TURNERSYND=CATH_SRC.TURNERSYND,
								CATH_TRGT.WILLIAMSBEURENSYND=CATH_SRC.WILLIAMSBEURENSYND
			
							FROM CATHPATANATOMY CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.PatID = AC.PatID  
							INNER JOIN [dbo].[CHOP_IMPACT_CATHPATANATOMY] CATH_SRC ON AC.PAT_MRN_ID = CATH_SRC.PAT_MRN_ID  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHPATANATOMY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.PAT_MRN_ID = AC.PAT_MRN_ID  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHPATANATOMY',@PAT_MRN_ID,4,@DBVrsn;
					  END	
					END	TRY
					--IF ERROR IN TRANSACTION, PROVIDE ROLLBACK PROCEDURE
					BEGIN CATCH
						--PRINT 'BEGIN CATCH'
						IF @@TRANCOUNT > 0
							BEGIN
								--PRINT 'BEGIN ROLLBACK'
								
								ROLLBACK TRAN
								
								--PRINT 'BEGIN ERROR UPDATE'
								
								UPDATE CATH_SRC 
								SET CATH_SRC.PendingImport = 3
								FROM [dbo].[CHOP_IMPACT_CATHPATANATOMY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.PAT_MRN_ID = AC.PAT_MRN_ID  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT D.PatID FROM Demographics D INNER JOIN @CathCaseList AC ON D.PatID = AC.PatID WHERE AC.RowID = @i)
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHPATANATOMY (HospitalID,PatID,DiGeorgeSynd,AlagilleSynd,Hernia,MarfanSynd,DownSynd,Heterotaxy,NoonanSynd,Rubella,Trisomy13,Trisomy18,TurnerSynd,WilliamsBeurenSynd,CreateDate,LastUpdate,UpdatedBy) 
						SELECT 4,AC.PatID,DiGeorgeSynd,AlagilleSynd,Hernia,MarfanSynd,DownSynd,Heterotaxy,NoonanSynd,Rubella,Trisomy13,Trisomy18,TurnerSynd,WilliamsBeurenSynd,getdate(),getdate(),'CHOP_AUTOMATION'
						FROM [dbo].[CHOP_IMPACT_CATHPATANATOMY] CATH_SRC INNER JOIN @CathCaseList AC ON AC.PAT_MRN_ID = CATH_SRC.PAT_MRN_ID 
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHPATANATOMY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.PAT_MRN_ID = AC.PAT_MRN_ID  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHPATANATOMY',@PAT_MRN_ID,4,@DBVrsn;
		
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHPATANATOMY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.PAT_MRN_ID = AC.PAT_MRN_ID 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHPATANATOMY] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.PAT_MRN_ID = AC.PAT_MRN_ID  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
