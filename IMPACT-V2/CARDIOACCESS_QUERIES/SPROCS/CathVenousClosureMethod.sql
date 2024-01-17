USE [Centripetus]
GO
/****** Object:  StoredProcedure [dbo].[sp_CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD]    Script Date: 5/12/2020 8:53:13 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE  PROCEDURE [dbo].[sp_CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD]

AS

DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @Sort int, @VENCLOSUREDEVID int
DECLARE @CathCaseList TABLE (CathID int, VENCLOSUREDEVID int, EMREventID int, Sort int , DBVrsn varchar(5), RowID int)
DECLARE @DeleteList TABLE (CathID int, EMREventID int, VENCLOSUREDEVID int, RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int

							
	SET NOCOUNT ON 
	
	--list of closures to be deleted
	INSERT INTO @DeleteList (CathID, EMREventID, VENCLOSUREDEVID, RowID)
	SELECT C.CathID,  CATH_SRC.SURG_ENC_ID, CATH_SRC.VENCLOSUREDEVID, ROW_NUMBER() OVER (ORDER BY C.CathID)
	FROM [dbo].[CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	WHERE 
	     PendingImport = 9

	--delete records
				 BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--DELETE FROM THE TARGET
							DELETE CATH_TRGT --select *
							FROM CATHVENOUSCLOSUREMETHOD CATH_TRGT INNER JOIN @DeleteList DL ON CATH_TRGT.CathID = DL.CathID  AND DL.VENCLOSUREDEVID = CATH_TRGT.VENCLOSUREDEVID
			
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD] CATH_SRC INNER JOIN @DeleteList DL ON CATH_SRC.SURG_ENC_ID = DL.EMREventID  AND DL.VENCLOSUREDEVID = CATH_SRC.VENCLOSUREDEVID
							WHERE PendingImport = 9 
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHVENOUSCLOSUREMETHOD',@EventID,4,@DBVrsn;
					 END


	--list of new closures to be added
	INSERT INTO @CathCaseList(CathID, EMREventID, VENCLOSUREDEVID, Sort, DBVrsn, RowID)
	SELECT C.CathID, CATH_SRC.SURG_ENC_ID, CATH_SRC.VENCLOSUREDEVID, ROW_NUMBER() OVER (PARTITION BY CATH_SRC.SURG_ENC_ID ORDER BY CATH_SRC.VENCLOSUREDEVID)+COALESCE(cathSORT,0), h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY C.CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
													LEFT JOIN CATHVENOUSCLOSUREMETHOD CATH_TGT ON C.CathID = CATH_TGT.CathID AND CATH_SRC.VENCLOSUREDEVID = CATH_TGT.VENCLOSUREDEVID
													LEFT JOIN (SELECT CATHID, MAX(SORT) cathSORT FROM CATHVENOUSCLOSUREMETHOD GROUP BY CATHID) cathsort ON C.CathID = cathsort.CathID
    WHERE PendingImport IN (1,2)   --This is to test an individual record
	AND CATH_TGT.VENCLOSUREDEVID IS NULL
	--and CATH_SRC.surg_enc_id = 2061174817
	ORDER BY C.CathID  

		
	SELECT @numrows = @@RowCount, @i = 1 
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @EventID = CathID, @DBVrsn = DBVrsn, @VENCLOSUREDEVID = VENCLOSUREDEVID, @Sort = Sort, @EMREventID = EMREventID FROM @CathCaseList AC WHERE AC.RowID = @i 

             IF EXISTS(SELECT C.CathID FROM CathData C INNER JOIN @CathCaseList AC ON C.CathID = AC.CathID WHERE AC.RowID = @i) 
				
				BEGIN TRY
				  BEGIN
					BEGIN TRAN
						INSERT INTO dbo.CATHVENOUSCLOSUREMETHOD (CathID,VENCLOSUREDEVID, Sort) 
						SELECT AC.CathID,CATH_SRC.VENCLOSUREDEVID, AC.SORT
						FROM [dbo].[CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID AND CATH_SRC.VENCLOSUREDEVID = AC.VENCLOSUREDEVID
						                                                 
						WHERE AC.RowID = @i 

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID   AND CATH_SRC.VENCLOSUREDEVID = AC.VENCLOSUREDEVID
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHVENOUSCLOSUREMETHOD',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i 
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHVENOUSCLOSUREMETHOD] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
		
		