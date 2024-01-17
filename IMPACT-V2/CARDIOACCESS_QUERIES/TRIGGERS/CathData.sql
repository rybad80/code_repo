/*
PENDING IMPORT ID LEGEND
	0 = IMPORT SUCCESSFUL
	1 = SOURCE RECORD WAS UPDATED AND WAITING FOR DATA UPLOAD TO DESTINATION DB
	2 = NO MATCHING CASE EXISTS IN DESTINATION DB (CASES.EMREventID)
	3 = ERROR UPDATING DESTINATION RECORD
	4 = ERROR INSERTING DESTINATION RECORD
*/
CREATE TRIGGER tr_CHOP_IMPACT_CATHDATA ON CHOP_IMPACT_CATHDATA
AFTER UPDATE, INSERT  

as


DECLARE @i int 
DECLARE @numrows int  
DECLARE @EventID int, @DBVrsn varchar(5), @EMREventID int, @CathCaseID varchar(10)
DECLARE @CathEventList TABLE (CathID int, CathCaseID varchar(10), EMREventID int, DBVrsn varchar(5), RowID int)
DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
DECLARE @DB int
DECLARE @AccessLoc int
DECLARE @ProcAorticValv int
DECLARE @ProcASD INT
DECLARE @ProcCoarc int
DECLARE @ProcProxPAStent int
DECLARE @ProcPDA int
DECLARE @ProcOther int
DECLARE @ProcPulmonaryValv int
DECLARE @ProcEPAblation int
DECLARE @ProcEPCath int
DECLARE @ProcTPVR int
DECLARE @ProcEndTime datetime
DECLARE @CathID int
DECLARE @PAC3DBVrsn nvarchar(10)
DECLARE @ProcStartDate datetime
DECLARE @HospID int
DECLARE @ProcDxCath int



							
	SET NOCOUNT ON 
	
	INSERT INTO @CathEventList(CathID, CathCaseID, EMREventID, DBVrsn, RowID)
	SELECT c.CathID, CATH_SRC.CATH_CASE_ID, CATH_SRC.SURG_ENC_ID, H.IMPACTDATAVRSN, ROW_NUMBER() OVER (ORDER BY c.CathID)  
	FROM [dbo].[CHOP_IMPACT_CATHDATA] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.CATH_CASE_ID = C.Aux5
	                                           INNER JOIN dbo.Hospitalization h ON C.HOSPID = H.HOSPITALIZATIONID --comment out when interface is liveR_HSP_NM
	--FROM [dbo].[CHOP_IMPACT_CATHDATA] CATH_SRC INNER JOIN dbo.CathData C ON CATH_SRC.SURG_ENC_ID = C.EMREventID
	--                                           INNER JOIN dbo.Hospitalization h ON C.HOSPID = H.HOSPITALIZATIONID
	WHERE PendingImport IN (1,2)   --This is to test an individual record 
	--AND CATH_SRC.CATH_CASE_ID  = '19-0760' 
	ORDER BY CathID
		
	SELECT @numrows = @@RowCount, @i = 1
	WHILE (@i <= @numrows) 
		BEGIN
			--SELECT @EventID = CathID, @DBVrsn = DBVrsn, @EMREventID = EMREventID FROM @CathEventList AC WHERE AC.RowID = @i 
			  SELECT @EventID = CathID, @DBVrsn = DBVrsn, @CathCaseID = CathCaseID, @EMREventID = EMREventID FROM @CathEventList AC WHERE AC.RowID = @i --remove when interface in place
			--IF EXISTS(SELECT APM.CathID FROM CATHDATA APM WHERE APM.CathID = @EventID)  
				--BEGIN
					BEGIN TRY
					  BEGIN
						--BEGIN THE TRANSACTION
						BEGIN TRAN
							--UPDATE THE TARGET RECORD
							UPDATE CATH_TRGT
							SET 
								CATH_TRGT.PROCDXCATH = CATH_SRC.PROCDXCATH,
								CATH_TRGT.PROCASD = CATH_SRC.PROCASD,
								CATH_TRGT.HEIGHT = CATH_SRC.HEIGHT,
								CATH_TRGT.WEIGHT = CATH_SRC.WEIGHT,
								CATH_TRGT.PREPROCHGB = CATH_SRC.PREPROCHGB,
								CATH_TRGT.PREPROCCREAT = CATH_SRC.PREPROCCREAT,
								CATH_TRGT.PREPROCO2 = CATH_SRC.PREPROCO2,
								CATH_TRGT.NEC = CATH_SRC.NEC,
								CATH_TRGT.SEPSIS = CATH_SRC.SEPSIS,
								CATH_TRGT.PREG = CATH_SRC.PREG,
								CATH_TRGT.PREPROCANTIARR = CATH_SRC.PREPROCANTIARR,
								CATH_TRGT.PREPROCANTICOAG = CATH_SRC.PREPROCANTICOAG,
								CATH_TRGT.PREPROCANTIHYP = CATH_SRC.PREPROCANTIHYP,
								CATH_TRGT.PREPROCANTIPLATELET = CATH_SRC.PREPROCANTIPLATELET,
								CATH_TRGT.PREPROCBB = CATH_SRC.PREPROCBB,
								CATH_TRGT.PREPROCDIURETIC = CATH_SRC.PREPROCDIURETIC,
								CATH_TRGT.PREPROCPROSTA = CATH_SRC.PREPROCPROSTA,
								CATH_TRGT.PREPROCVASO = CATH_SRC.PREPROCVASO,
								CATH_TRGT.PREPROCSINUS = CATH_SRC.PREPROCSINUS,
								CATH_TRGT.PREPROCAET = CATH_SRC.PREPROCAET,
								CATH_TRGT.PREPROCSVT = CATH_SRC.PREPROCSVT,
								CATH_TRGT.PREPROCAFIB = CATH_SRC.PREPROCAFIB,
								CATH_TRGT.PREPROCJUNCT = CATH_SRC.PREPROCJUNCT,
								CATH_TRGT.PREPROCIDIO = CATH_SRC.PREPROCIDIO,
								CATH_TRGT.PREPROCAVB2 = CATH_SRC.PREPROCAVB2,
								CATH_TRGT.PREPROCAVB3 = CATH_SRC.PREPROCAVB3,
								CATH_TRGT.PREPROCPACED = CATH_SRC.PREPROCPACED,
								CATH_TRGT.PROCCOARC = CATH_SRC.PROCCOARC,
								CATH_TRGT.PROCAORTICVALV = CATH_SRC.PROCAORTICVALV,
								CATH_TRGT.PROCPULMONARYVALV = CATH_SRC.PROCPULMONARYVALV,
								CATH_TRGT.PROCPDA = CATH_SRC.PROCPDA,
								CATH_TRGT.PROCPROXPASTENT = CATH_SRC.PROCPROXPASTENT,
								CATH_TRGT.HOSPSTATUS = CATH_SRC.HOSPSTATUS,
								CATH_TRGT.PROCSTATUS = CATH_SRC.PROCSTATUS,
								CATH_TRGT.TRAINEE = CATH_SRC.TRAINEE,
								CATH_TRGT.OPERATORID = CONTACTS.CONTACTID,
								CATH_TRGT.ANESPRESENT = CATH_SRC.ANESPRESENT,
								CATH_TRGT.ANESCALLEDIN = CATH_SRC.ANESCALLEDIN,
								CATH_TRGT.SEDATION = CATH_SRC.SEDATION,
								CATH_TRGT.AIRMNGLMA = CATH_SRC.AIRMNGLMA,
								CATH_TRGT.AIRMNGTRACH = CATH_SRC.AIRMNGTRACH,
								CATH_TRGT.AIRMNGBAGMASK = CATH_SRC.AIRMNGBAGMASK,
								CATH_TRGT.AIRMNGCPAP = CATH_SRC.AIRMNGCPAP,
								CATH_TRGT.AIRMNGELECINTUB = CATH_SRC.AIRMNGELECINTUB,
								CATH_TRGT.AIRMNGPREVINTUB = CATH_SRC.AIRMNGPREVINTUB,
								CATH_TRGT.ACCESSLOC = CATH_SRC.ACCESSLOC,
								CATH_TRGT.VENACCESS = CATH_SRC.VENACCESS,
								CATH_TRGT.VENLARGSHEATH = CATH_SRC.VENLARGSHEATH,
								CATH_TRGT.VENCLOSUREMETHODND = CATH_SRC.VENCLOSUREMETHODND,
								CATH_TRGT.ARTACCESS = CATH_SRC.ARTACCESS,
								CATH_TRGT.ARTLARGSHEATH = CATH_SRC.ARTLARGSHEATH,
								CATH_TRGT.ARTCLOSUREMETHODND = CATH_SRC.ARTCLOSUREMETHODND,
								CATH_TRGT.FLUOROTIME = CATH_SRC.FLUOROTIME,
								CATH_TRGT.CONTRASTVOL = CATH_SRC.CONTRASTVOL,
								CATH_TRGT.SYSHEPARIN = CATH_SRC.SYSHEPARIN,
								CATH_TRGT.ACTMONITOR = CATH_SRC.ACTMONITOR,
								CATH_TRGT.ACTPEAK = CATH_SRC.ACTPEAK,
								CATH_TRGT.INOTROPE = CATH_SRC.INOTROPE,
								CATH_TRGT.INOTROPEUSE = CATH_SRC.INOTROPEUSE,
								CATH_TRGT.ECMOUSE = CATH_SRC.ECMOUSE,
								CATH_TRGT.LVADUSE = CATH_SRC.LVADUSE,
								--CATH_TRGT.AUX5 = CATH_SRC.AUX5,
								CATH_TRGT.SCHEDARRIVALDATE = CATH_SRC.SCHEDARRIVALDATE,
								--CATH_TRGT.PROCOTHER = CATH_SRC.PROCOTHER,
								CATH_TRGT.PREPROCHGBND = CATH_SRC.PREPROCHGBND,
								CATH_TRGT.PREPROCCREATND = CATH_SRC.PREPROCCREATND,
								CATH_TRGT.SVDEFECT = CATH_SRC.SVDEFECT,
								CATH_TRGT.PREPROCMED = CATH_SRC.PREPROCMED,
								CATH_TRGT.PROCEPCATH = CATH_SRC.PROCEPCATH,
								CATH_TRGT.PROCEPABLATION = CATH_SRC.PROCEPABLATION,
								CATH_TRGT.PROCTPVR = CATH_SRC.PROCTPVR,
								CATH_TRGT.SECONDPARTICIPATING = CATH_SRC.SECONDPARTICIPATING,
								CATH_TRGT.PROCSTARTDATE = CATH_SRC.PROCSTARTDATE,
								CATH_TRGT.PROCSTARTTIME = CATH_SRC.PROCSTARTTIME,
								CATH_TRGT.PROCENDDATE = CATH_SRC.PROCENDDATE,
								CATH_TRGT.PROCENDTIME = CATH_SRC.PROCENDTIME,
								CATH_TRGT.AIRMNG = CATH_SRC.AIRMNG,
								CATH_TRGT.IABPUSE = CATH_SRC.IABPUSE,
								CATH_TRGT.PLANEUSED = CATH_SRC.PLANEUSED,
								CATH_TRGT.FLUORODOSEKERM = CATH_SRC.FLUORODOSEKERM,
								CATH_TRGT.FLUORODOSEKERM_UNITS = CATH_SRC.FLUORODOSEKERM_UNITS,
								CATH_TRGT.FLUORODOSEDAP = CATH_SRC.FLUORODOSEDAP,
								CATH_TRGT.FLUORODOSEDAP_UNITS = CATH_SRC.FLUORODOSEDAP_UNITS,
								CATH_TRGT.EMREventID = CATH_SRC.SURG_ENC_ID--,								
								--LastUpdate = GetDate(), 
								--UpdateBy = 'CHOP_AUTOMATION' --SELECT *
							FROM CATHDATA CATH_TRGT INNER JOIN @CathEventList AC ON CATH_TRGT.CathID = AC.CathID  
							--INNER JOIN [dbo].[CHOP_IMPACT_CATHDATA] CATH_SRC ON AC.EMREventID = CATH_SRC.SURG_ENC_ID  
							INNER JOIN [dbo].[CHOP_IMPACT_CATHDATA] CATH_SRC ON AC.CathCaseID = CATH_SRC.CATH_CASE_ID   --remove when interface in place
							LEFT JOIN CONTACTS ON CATH_SRC.OPERATORID = CONTACTS.CONTACTIDFT
							WHERE AC.RowID = @i
							
							--add fields here
							SELECT  @AccessLoc = AccessLoc ,
									 @ProcAorticValv = ProcAorticValv,
									 @ProcASD = ProcASD,
									 @ProcCoarc = ProcCoarc,
									 @ProcProxPAStent = ProcProxPAStent,
									 @ProcPDA = ProcPDA,
									 @ProcPulmonaryValv = ProcPulmonaryValv,
									 @ProcEPAblation = ProcEPAblation,
									 @ProcEPCath = ProcEPCath,
									 @ProcTPVR = ProcTPVR
							FROM DBO.CHOP_IMPACT_CATHDATA 
							WHERE SURG_ENC_ID = @EMREventID
							
							--UPDATE THE SOURCE RECORD RESETTING THE PENDING IMPORT FLAG TO 0
							UPDATE CATH_SRC 
							SET CATH_SRC.PendingImport = 0
							--FROM [dbo].[CHOP_IMPACT_CATHDATA] CATH_SRC INNER JOIN @CathEventList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
							FROM [dbo].[CHOP_IMPACT_CATHDATA] CATH_SRC INNER JOIN @CathEventList AC ON AC.CathCaseID = CATH_SRC.CATH_CASE_ID  --remove when interface in place 
							WHERE AC.RowID = @i
						
						COMMIT TRAN
						--COMMIT THE PENDING TRANSACTION IF NO ERRORS
						
						-- VALIDATE THE RECORD FOR THE END USER
						SET @DB = 4 --ACC IMPACT REGISTRY ID
						EXEC Validation_Call_ByTableEventID 'CathData',@EventID,@DB,@DBVrsn;
						IF @AccessLoc = 1455
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathArterialClosureMethod',@EventID,@DB,@DBVrsn;
								
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathVenousClosureMethod') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END
						ELSE IF @AccessLoc = 1454
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathVenousClosureMethod',@EventID,@DB,@DBVrsn;
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathArterialClosureMethod') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END
						ELSE IF @AccessLoc = 1456
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathVenousClosureMethod',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathArterialClosureMethod' ,@EventID,@DB,@DBVrsn;
							END
						
						IF @ProcAorticValv = 1
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathInflationCounter' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathInflationCounterInflationCounter',@EventID,@DB,@DBVrsn;
							END
						ELSE
							BEGIN
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathInflationCounter','CathInflationCounterInflationCounter') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END
						
						IF @ProcASD = 1
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathASDClosure',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathASDDefect',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathASDDevice',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathASDDeviceAssn',@EventID,@DB,@DBVrsn;
							END
						ELSE
							BEGIN
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathASDClosure','CathASDDefect','CathASDDevice','CathASDDeviceAssn') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END
						
						IF @ProcCoarc = 1
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathCoarcProc',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathCoarcProcDevice',@EventID,@DB,@DBVrsn;
							END
						ELSE
							BEGIN
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathCoarcProc','CathCoarcProcDevice') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END
						
						IF @ProcProxPAStent = 1
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathPAStent',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathPAStentDefect',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathPAStentDevice',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathPAStentDeviceAssn',@EventID,@DB,@DBVrsn;
							END
						ELSE
							BEGIN
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathPAStent','CathPAStentDefect','CathPAStentDevice','CathPAStentDeviceAssn') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END
						
						IF @ProcPDA = 1
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathPDAClosure',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathPDAClosureDevice',@EventID,@DB,@DBVrsn;
							END
						ELSE
							BEGIN
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathPDAClosure','CathPDAClosureDevice') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END
						
						IF @ProcPulmonaryValv = 1
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathPulmonaryValvuloplasty',@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathPulmonaryValvuloplastyInflationCounter',@EventID,@DB,@DBVrsn;
							END
						ELSE
							BEGIN
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathPulmonaryValvuloplasty','CathPulmonaryValvuloplastyInflationCounter') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END

						IF @ProcEPAblation = 1  --If EP Ablation is yes then run all of the validations 
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathEP' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathEPTachyarrObs' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathEPSedMed' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathEPMapSys' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathEPSSSQ5' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathEPTarget' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathEPAblationIndication' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathEPTargetApproach' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathEPTargetMethod' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathEPDevices' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_OneToManyDetail 'CathEPDeviceAssociation' ,@EventID,@DB,@DBVrsn;
							END
						ELSE IF @ProcEPCath = 1
						--IF EP Ablation is No or NULL then run the following code
						--IF EP Cath is Yes and Ablation is No then run the following and delete the ablation validations if someone changed Abalation from Yes to No
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathEP' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathEPTachyarrObs' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathEPSedMed' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathEPMapSys' ,@EventID,@DB,@DBVrsn;
								EXEC Validation_Call_ByTableEventID 'CathEPSSSQ5' ,@EventID,@DB,@DBVrsn;

								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathEPTarget','CathEPAblationIndication','CathEPTargetApproach','CathEPTargetMethod','CathEPDevices','CathEPDeviceAssociation') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB

							END
						ELSE --Otherwise both Cath and Ablation are NO or NUll so all validations should be removed
							DELETE ValidationErrors 
							FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
							WHERE EventID = @EventID AND CATblName IN('CathEP','CathEPTachyarrObs','CathEPSedMed','CathEPMapSys','CathEPSSSQ5','CathEPTarget','CathEPAblationIndication','CathEPTargetApproach','CathEPTargetMethod','CathEPDevices','CathEPDeviceAssociation') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
									
								
						
						IF @ProcTPVR = 1
							BEGIN
								EXEC Validation_Call_ByTableEventID 'CathTPVR',@EventID,@DB,@DBVrsn;
							END
						ELSE
							BEGIN
								DELETE ValidationErrors 
								FROM ValidationErrors INNER JOIN DBSpecs ON ValidationErrors.SequenceNumber = DBSpecs.SeqNo AND ValidationErrors.DBID  = DBSpecs.DB
								WHERE EventID = @EventID AND CATblName IN('CathTPVR','CathTPVRDevice') AND DBSpecs.DataVrsn = @DBVrsn AND ValidationErrors.DBID = @DB
							END
							
						EXEC Validation_Call_ByTableEventID 'CathHistory',@EventID,@DB,@DBVrsn;
						EXEC Validation_Call_ByTableEventID 'CathDiagnosis',@EventID,@DB,@DBVrsn;
						EXEC Validation_Call_ByTableEventID 'CathProcedures',@EventID,@DB,@DBVrsn;
						EXEC Validation_Call_ByTableEventID 'CathEvents',@EventID,@DB,@DBVrsn;
						EXEC Validation_Call_ByTableEventID 'CathEventOther',@EventID,@DB,@DBVrsn;


						IF EXISTS(SELECT CathID FROM PC4CardiacCath WHERE ACC_CathID_FK = @CathID)
							BEGIN
								SET @EventID = 0
								SET @DBVrsn = 0
								SET @DB = 7

								SELECT @DBVrsn = PC4HospDataVrsn, @PAC3DBVrsn = PAChospVrsn FROM Hospitalization WHERE HospitalizationID = @HospID
								
								UPDATE PC4CardiacCath 
								SET CardCathDt = @ProcStartDate
								, ProcDxCath = @ProcDxCath
								, ProcASD = @ProcASD
								, ProcCoarc = @ProcCoarc
								, ProcAorticValv = @ProcAorticValv
								, ProcPulmonaryValv = @ProcPulmonaryValv
								, ProcPDA = @ProcPDA
								, ProcProxPAStent = @ProcProxPAStent
								, ProcOther = @ProcOther
								, ProcEPCath = @ProcEPCath
								, ProcEPAblation = @ProcEPAblation
								, ProcPVplace = @ProcTPVR
								, CathEndDtTm = @ProcEndTime
								WHERE ACC_CathID_FK = @CathID

								SELECT Top 1 @EventID = EncounterID FROM PC4Encounter WHERE HospitalizationID = @HospID
								IF @EventID IS NOT NULL AND @EventID > 0
								BEGIN
									EXEC Validation_Call_OneToManyDetail 'PC4CardiacCath',@EventID,@DB,@DBVrsn;
									EXEC Validation_Call_OneToManyDetail 'PC4CardiacCathProc',@EventID,@DB,@DBVrsn;
								END

								SET @EventID = 0
								SET @DB = 8
								SELECT Top 1 @EventID = PACEncounterID, @DBVrsn = PAChospVrsn 
								FROM PACEncounter E INNER JOIN Hospitalization H ON E.HospitalizationID = H.HospitalizationID WHERE E.HospitalizationID = @HospID
								IF @EventID IS NOT NULL AND @EventID > 0
									BEGIN
										EXEC Validation_Call_OneToManyDetail 'PC4CardiacCath',@EventID,@DB,@DBVrsn;
								
									END
							END
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
								FROM [dbo].[CHOP_IMPACT_CATHDATA] CATH_SRC INNER JOIN @CathEventList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
								WHERE AC.RowID = @i
								
								--PRINT 'END ERROR UPDATE'
								
								SELECT @ErrMsg = ERROR_MESSAGE(), @ErrSeverity = ERROR_SEVERITY()
								
								RAISERROR(@ErrMsg, @ErrSeverity, 1)
							END
					END CATCH
					
				--END
			
		--	ELSE
				/*BEGIN
					UPDATE CATH_SRC 
					SET CATH_SRC.PendingImport = 2
					FROM [dbo].[CHOP_IMPACT_CATHDATA] CATH_SRC INNER JOIN @CathEventList AC ON CATH_SRC.SURG_ENC_ID = AC.EMREventID  
					WHERE AC.RowID = @i
					 
				END*/

			SET @i = @i + 1
		END
