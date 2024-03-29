USE [Centripetus]
GO
/****** Object:  StoredProcedure [dbo].[sp_CHOP_IMPACT_CATHDIAGNOSIS]    Script Date: 11/8/2019 4:04:57 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
alter PROCEDURE [dbo].[sp_CHOP_IMPACT_CATHDIAGNOSIS]


AS
DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @Sort int, @DiagID int
DECLARE @CathCaseList TABLE (CathID int, DiagID int, EMREventID int, Sort int , DBVrsn varchar(5), RowID int)
DECLARE @DeleteList TABLE (CathID int, EMREventID int, DiagID int, RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int

							
	SET NOCOUNT ON 
	
	--list of procedures to be deleted
	INSERT INTO @DeleteList (CathID, EMREventID, DiagID, RowID)
	SELECT C.CathID,  CATH_SRC.SURG_ENC_ID, CATH_SRC.PREPROCCARDDIAGID, ROW_NUMBER() OVER (ORDER BY C.CathID)
	FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	WHERE 
	     PendingImport = 9

	--delete records
				 BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--DELETE FROM THE TARGET
							DELETE CATH_TRGT --select *
							FROM CATHDIAGNOSIS CATH_TRGT INNER JOIN @DeleteList DL ON CATH_TRGT.CathID = DL.CathID  AND DL.DiagID = CATH_TRGT.PREPROCCARDDIAGID
			
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @DeleteList DL ON CATH_SRC.SURG_ENC_ID = DL.EMREventID  AND DL.DiagID = CATH_SRC.PREPROCCARDDIAGID
							WHERE PendingImport = 9 
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHDIAGNOSIS',@EventID,4,@DBVrsn;
					 END


	--list of new procedures to be added
	INSERT INTO @CathCaseList(CathID, EMREventID, DiagID, Sort, DBVrsn, RowID)
	SELECT C.CathID, CATH_SRC.SURG_ENC_ID, CATH_SRC.PREPROCCARDDIAGID, ROW_NUMBER() OVER (PARTITION BY CATH_SRC.SURG_ENC_ID ORDER BY CATH_SRC.PREPROCCARDDIAGID)+COALESCE(cathSORT,0), h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY C.CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
													LEFT JOIN CATHDIAGNOSIS CATH_TGT ON C.CathID = CATH_TGT.CathID AND CATH_SRC.PREPROCCARDDIAGID = CATH_TGT.PREPROCCARDDIAGID
													LEFT JOIN (SELECT CATHID, MAX(SORT) cathSORT FROM CATHDIAGNOSIS GROUP BY CATHID) cathsort ON C.CathID = cathsort.CathID
    WHERE PendingImport IN (1,2)   --This is to test an individual record
	AND CATH_TGT.PREPROCCARDDIAGID IS NULL
	--and CATH_SRC.surg_enc_id = 2061174817
	ORDER BY C.CathID  

		
	SELECT @numrows = @@RowCount, @i = 1 
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CathID, @DBVrsn = DBVrsn, @DiagID = DiagID, @Sort = Sort, @EMREventID = EMREventID FROM @CathCaseList AC WHERE AC.RowID = @i 

             IF EXISTS(SELECT C.CathID FROM CathData C INNER JOIN @CathCaseList AC ON C.CathID = AC.CathID WHERE AC.RowID = @i) 
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHDIAGNOSIS (CathID,PREPROCCARDDIAGID,DiagnosisName, Sort) 
						SELECT AC.CathID,CATH_SRC.PREPROCCARDDIAGID, PX_LU.DiagnosisName, AC.SORT
						FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID AND CATH_SRC.PREPROCCARDDIAGID = AC.DiagID
						                                                 INNER JOIN [dbo].[CathDiagnosisMaster_LU] PX_LU ON PX_LU.DIAGNOSISID = CATH_SRC.PreProcCardDiagID  
						WHERE AC.RowID = @i 

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID   AND CATH_SRC.PREPROCCARDDIAGID = AC.DiagID
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHDIAGNOSIS',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i 
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHDIAGNOSIS] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
		
		