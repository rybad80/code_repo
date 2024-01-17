/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/

CREATE PROCEDURE sp_CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY 

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int
DECLARE @CathCaseList TABLE (CathID int, EMREventID int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	INSERT INTO @CathCaseList(CathID, EMREventID, DBVrsn, RowID)
	SELECT CathID, CATH_SRC.SURG_ENC_ID, h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY CathID) 
	FROM [dbo].CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                 INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
 --   WHERE CATHID = 11821
	WHERE PendingImport IN (1,2)   --This is to test an individual record
	ORDER BY CathID
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CathID, @DBVrsn = DBVrsn, @EMREventID = EMREventID FROM @CathCaseList AC WHERE AC.RowID = @i

			IF EXISTS(SELECT ASD.CathID FROM CATHPULMONARYVALVULOPLASTY ASD WHERE ASD.CathID = @EventID)  
				BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET CATH_TRGT.PVProcInd=	CATH_SRC.PVProcInd	,
								CATH_TRGT.PVMorphology=	CATH_SRC.PVMorphology	,
								CATH_TRGT.PVSubStenosis=	CATH_SRC.PVSubStenosis	,
								CATH_TRGT.PVDiameter=	CATH_SRC.PVDiameter	,
								CATH_TRGT.PVPrePkSysGrad=	CATH_SRC.PVPrePkSysGrad	,
								CATH_TRGT.PVDefectTreated=	CATH_SRC.PVDefectTreated	,
								CATH_TRGT.PVPrePkSysGradNA=	CATH_SRC.PVPrePkSysGradNA	,
								CATH_TRGT.PVBallTech=	CATH_SRC.PVBallTech	,
								CATH_TRGT.PVBall1DevID=	CATH_SRC.PVBall1DevID	,
								CATH_TRGT.PVBall2DevID=	CATH_SRC.PVBall2DevID	,
								CATH_TRGT.PVBallStab=	CATH_SRC.PVBallStab	,
								CATH_TRGT.PVBallPressure=	CATH_SRC.PVBallPressure	,
								CATH_TRGT.PVBallOutcome=	CATH_SRC.PVBallOutcome	,
								CATH_TRGT.PVPostPkSysGrad=	CATH_SRC.PVPostPkSysGrad	,
								CATH_TRGT.PVPostPkSysGradNA=	CATH_SRC.PVPostPkSysGradNA	

			
							FROM CATHPULMONARYVALVULOPLASTY CATH_TRGT INNER JOIN @CathCaseList AC ON CATH_TRGT.CathID = AC.CathID  
							INNER JOIN [dbo].CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  
							WHERE AC.RowID = @i
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHPULMONARYVALVULOPLASTY',@EventID,4,@DBVrsn;
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
								FROM [dbo].CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				END
			ELSE IF EXISTS(SELECT C.CathID FROM CathData C INNER JOIN @CathCaseList AC ON C.CathID = AC.CathID WHERE AC.RowID = @i)
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHPULMONARYVALVULOPLASTY (CathID,PVProcInd,	PVMorphology,	PVSubStenosis,	PVDiameter,	PVPrePkSysGrad,	PVDefectTreated,	PVPrePkSysGradNA,	PVBallTech,	PVBall1DevID,	PVBall2DevID,	PVBallStab,	PVBallPressure,	PVBallOutcome,	PVPostPkSysGrad,	PVPostPkSysGradNA
) 
						SELECT AC.CathID,PVProcInd,	PVMorphology,	PVSubStenosis,	PVDiameter,	PVPrePkSysGrad,	PVDefectTreated,	PVPrePkSysGradNA,	PVBallTech,	PVBall1DevID,	PVBall2DevID,	PVBallStab,	PVBallPressure,	PVBallOutcome,	PVPostPkSysGrad,	PVPostPkSysGradNA
						FROM [dbo].CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID 
						WHERE AC.RowID = @i

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHPULMONARYVALVULOPLASTY',@EventID,4,@DBVrsn;
					EXEC Validation_Call_ByTableEventID 'othertables' ,@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].CHOP_IMPACT_CATHPULMONARYVALVULOPLASTY CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
