USE [Centripetus]
GO
/****** Object:  StoredProcedure [dbo].[sp_CHOP_IMPACT_CATHPROCEDURES]    Script Date: 11/8/2019 4:04:57 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_CHOP_IMPACT_CATHPROCEDURES]


AS
DECLARE @i int 
DECLARE @numrows int 
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @Sort int, @ProcID int
DECLARE @CathCaseList TABLE (CathID int, ProcID int, EMREventID int, Sort int , DBVrsn varchar(5), RowID int)
DECLARE @DeleteList TABLE (CathID int, EMREventID int, ProcID int, RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int


							
	SET NOCOUNT ON 
	
	--list of procedures to be deleted
	INSERT INTO @DeleteList (CathID, EMREventID, ProcID, RowID)
	SELECT C.CathID,  CATH_SRC.SURG_ENC_ID, CATH_SRC.SpecificProcID, ROW_NUMBER() OVER (ORDER BY C.CathID)
	FROM [dbo].[CHOP_IMPACT_CATHPROCEDURES] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	WHERE PendingImport = 9


	--list of new procedures to be added
	INSERT INTO @CathCaseList(CathID, EMREventID, ProcID, Sort, DBVrsn, RowID)
	SELECT C.CathID, CATH_SRC.SURG_ENC_ID, CATH_SRC.SpecificProcID,ROW_NUMBER() OVER (PARTITION BY CATH_SRC.SURG_ENC_ID ORDER BY CATH_SRC.SpecificProcID)+COALESCE(cathSORT,0), h.IMPACTDataVrsn , ROW_NUMBER() OVER (ORDER BY C.CathID) 
	FROM [dbo].[CHOP_IMPACT_CATHPROCEDURES] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID 
	                                                 INNER JOIN dbo.Hospitalization H on C.HospID = H.HospitalizationID
													 LEFT JOIN CATHPROCEDURES CATH_TGT ON C.CathID = CATH_TGT.CathID AND CATH_SRC.SPECIFICPROCID = CATH_TGT.SpecificProcID
													 LEFT JOIN (SELECT CATHID, MAX(SORT) cathSORT FROM CATHPROCEDURES GROUP BY CATHID) cathsort ON C.CathID = cathsort.CathID
    WHERE PendingImport IN (1,2)   
	  AND CATH_TGT.SpecificProcID IS NULL 
	  and  (--(PROCDXCATH = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,225,230,235,367,368,531,532,533,535,540,1055,1060,1065,1070,1075,1080,1085,1090,1095,1100,1105,1110,1115,1120,1125,1130,1135)) or
	    (PROCASD = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533,1115)) or
		(PROCCOARC = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533)) or
		(PROCAORTICVALV = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533)) or
		(ProcPulmonaryValv = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533)) or
		(ProcPDA = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533)) or
		(ProcProxPAStent = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533,1235,1240)) or
		(ProcEPCath = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,531,532,533)) or
		(ProcEPAblation = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,531,532,533)) or
		(ProcTPVR = 1 AND CATH_SRC.SpecificProcID IN (5,10,15,20,25,367,368,531,532,533,1565))
		)
		or 
		(PendingImport IN (1,2)   
	     AND CATH_TGT.SpecificProcID IS NULL and
		(PROCDXCATH = 1  AND PROCASD = 2 and PROCCOARC = 2 AND PROCAORTICVALV = 2 AND ProcPulmonaryValv = 2 AND ProcPDA = 2
		 and ProcProxPAStent = 2 AND ProcEPCath = 2 AND ProcEPAblation = 2 AND ProcTPVR = 2 and procother = 2)
		 )
		or 
		(PendingImport IN (1,2)   
	     AND CATH_TGT.SpecificProcID IS NULL and
		(PROCDXCATH = 1  AND PROCASD = 2 and PROCCOARC = 2 AND PROCAORTICVALV = 2 AND ProcPulmonaryValv = 2 AND ProcPDA = 2
		 and ProcProxPAStent = 2 AND ProcEPCath = 2 AND ProcEPAblation = 2 AND ProcTPVR = 2 and procother = 1 )
		 )
	ORDER BY C.CathID 

		
	SELECT @numrows = @@RowCount, @i = 1 
	WHILE (@i <= @numrows) 
		BEGIN
			SELECT @CathID = CathID, @ProcID = ProcID FROM @DeleteList DL WHERE DL.RowID = @i 

		      IF EXISTS(SELECT ASD.CathID FROM CATHPROCEDURES ASD WHERE ASD.CathID = @CathID and ASD.SpecificProcID = @ProcID)  
			 	BEGIN
					BEGIN TRY
					   BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--DELETE FROM THE TARGET
							DELETE CATH_TRGT --select *
							FROM CATHPROCEDURES CATH_TRGT INNER JOIN @DeleteList DL ON CATH_TRGT.CathID = DL.CathID  AND DL.ProcID = CATH_TRGT.SpecificProcID
							WHERE DL.RowID = @i and CATH_TRGT.SpecificProcID = @PROCID
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							FROM [dbo].[CHOP_IMPACT_CATHPROCEDURES] CATH_SRC INNER JOIN @DeleteList DL ON CATH_SRC.SURG_ENC_ID = DL.EMREventID  
							WHERE PendingImport = 9 and DL.RowID = @i 
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						EXEC Validation_Call_ByTableEventID 'CATHPROCEDURES',@EventID,4,@DBVrsn;
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
								FROM [dbo].[CHOP_IMPACT_CATHPROCEDURES] CATH_SRC INNER JOIN @DeleteList DL ON CATH_SRC.SURG_ENC_ID = DL.EMREventID  
								WHERE DL.RowID = @i 
								
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
						INSERT INTO dbo.CATHPROCEDURES (CathID,SPECIFICPROCID,PROCEDURENAME, Sort) 
						SELECT AC.CathID,CATH_SRC.SPECIFICPROCID, DX_LU.PROCEDURENAME, AC.SORT
						FROM [dbo].[CHOP_IMPACT_CATHPROCEDURES] CATH_SRC INNER JOIN @CathCaseList AC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID 
						                                                 INNER JOIN [dbo].[CathProcMaster_LU] DX_LU ON DX_LU.PROCEDUREID = CATH_SRC.SPECIFICPROCID  
						WHERE AC.RowID = @i 

						UPDATE CATH_SRC 
						SET CATH_SRC.PendingImport = 0
						FROM [dbo].[CHOP_IMPACT_CATHPROCEDURES] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
						WHERE AC.RowID = @i
					COMMIT TRAN
					--COMMIT THE PENDING TRANSACTION IF NO ERRORS
					
					-- VALIDATE THE RECORD FOR THE END USER
					EXEC Validation_Call_ByTableEventID 'CATHPROCEDURES',@EventID,4,@DBVrsn;
                   END
				END	TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0
						BEGIN
							ROLLBACK TRAN
							
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 4
							FROM [dbo].[CHOP_IMPACT_CATHPROCEDURES] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID 
							WHERE AC.RowID = @i 
							
							SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
							RAISERROR(@ErrMsg, @ErrSeverity, 1)

							
						END						
				END CATCH
			ELSE
				BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHPROCEDURES] CATH_SRC INNER JOIN @CathCaseList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i
					 
				END

			SET @i = @i + 1
		END
