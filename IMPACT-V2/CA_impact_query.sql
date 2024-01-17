select  MedRecN
        ,Demographics.PatLName
		,Demographics.PatFName
		,Demographics.PatMName
		,Demographics.SSN
		,Demographics.SSNNA
		,Demographics.PatID
		,Demographics.DOB
		,Demographics.Gender
		,Demographics.RaceCaucasian
		,Demographics.RaceBlack
		,Demographics.RaceAsian
		,Demographics.RaceNativeAm
		,Demographics.RaceNativePacific
		,Demographics.Ethnicity
		,Demographics.RaceAsianIndian
		,Demographics.RaceChinese
		,Demographics.RaceFilipino
		,Demographics.RaceJapanese
		,Demographics.RaceKorean
		,Demographics.RaceVietnamese
		,Demographics.RaceAsianOther
		,Demographics.RaceNativeHawaii
		,Demographics.RaceGuamChamorro
		,Demographics.RaceSamoan
		,Demographics.RacePacificIslandOther
		,Demographics.HispEthnicityMexican
		,Demographics.HispEthnicityPuertoRico
		,Demographics.HispEthnicityCuban
		,Demographics.HispEthnicityOtherOrigin
		,Demographics.Aux1
		,Demographics.Aux2
		,Hospitalization.AdmitDt
		,Hospitalization.ZipCode
		,Hospitalization.ZipCodeNA
		,Hospitalization.PayorCom
		,Hospitalization.PayorGovMCare
		,Hospitalization.PayorGovMCaid
		,Hospitalization.PayorGovMil
		,Hospitalization.PayorGovState
		,Hospitalization.PayorGovIHS
		,Hospitalization.PayorNonUS
		,Hospitalization.PayorNS
		,Hospitalization.HICNumber
		,PatAnatomy.FundDiag
		,Hospitalization.PriorCath
		,Hospitalization.NumPriorCath
		,Hospitalization.LastCathDate
		--,CathPriorCathProc.MostRecCathProcID
		,PatAnatomy.Premature
		,PatAnatomy.BirthWtKg
		,PatAnatomy.GestAgeWeeks
		,Hospitalization.PriorSurg
		,Hospitalization.NumPrevCardSurg
		,Hospitalization.LastCardSurgDate
		--,CathPriorSurgProc.MostRecCardSurgID
		,Hospitalization.EnrolledStudy
		,Hospitalization.PtRestriction
		,CathPatAnatomy.DiGeorgeSynd
		,CathPatAnatomy.AlagilleSynd
		,CathPatAnatomy.Hernia
		,CathPatAnatomy.DownSynd
		,CathPatAnatomy.Heterotaxy
		,CathPatAnatomy.MarfanSynd
		,CathPatAnatomy.NoonanSynd
		,CathPatAnatomy.Rubella
		,CathPatAnatomy.Trisomy13
		,CathPatAnatomy.Trisomy18
		,CathPatAnatomy.TurnerSynd
		,CathPatAnatomy.WilliamsBeurenSynd
		,CathHistory.Arrhythmia
		,CathArrhythmiaHistory.ArrhythmiaHx
		,CathHistory.PriorCM
		,CathHistory.PriorCMHx
		,CathHistory.ChronicLungDisease
		,CathHistory.ChronicLungDisease
		,CathHistory.CoagDisorder
		,CathHistory.CoagDisorder
		,CathHistory.HyperCoag
		,CathHistory.HyperCoag
		,CathHistory.HypoCoag
		,CathHistory.HypoCoag
		,CathHistory.Diabetes
		,CathHistory.Diabetes
		,CathHistory.Endocarditis
		,CathHistory.HF
		,CathHistory.NYHA
		,CathHistory.HeartTransplant
		,CathHistory.HepaticDisease
		,CathHistory.HepaticDisease
		,CathHistory.IschemicHD
		,CathHistory.KawasakiDisease
		,CathHistory.RenalInsuff
		,CathHistory.RenalInsuff
		,CathHistory.RheumaticHD
		,CathHistory.Seizures
		,CathHistory.Seizures
		,CathHistory.SickleCell
		,CathHistory.SickleCell
		,CathHistory.PriorStroke
		,CathHistory.PriorStroke
		,Hospitalization.Aux3
		,Hospitalization.Aux3
		,Hospitalization.Aux4
		--,CathDiagnosis.PreProcCardDiagID
		,CATH.Height
		,CATH.Weight
		,CATH.PreProcHgb
		,CATH.PreProcHgbND
		,CATH.PreProcCreat
		,CATH.PreProcCreatND
		,CATH.PreProcO2
		,CATH.SVDefect
		,CATH.NEC
		,CATH.Sepsis
		,CATH.Preg
		,CATH.PreProcMed
		,CATH.PreProcAntiarr
		,CATH.PreProcAnticoag
		,CATH.PreProcAntihyp
		,CATH.PreProcAntiplatelet
		,CATH.PreProcBB
		,CATH.PreProcDiuretic
		,CATH.PreProcProsta
		,CATH.PreProcVaso
		,CATH.PreProcSinus
		,CATH.PreProcAET
		,CATH.PreProcSVT
		,CATH.PreProcAfib
		,CATH.PreProcJunct
		,CATH.PreProcIdio
		,CATH.PreProcAVB2
		,CATH.PreProcAVB3
		,CATH.PreProcPaced
		,CATH.Aux5
		,CATH.Aux6
		,CATH.ProcDxCath
		,CATH.ProcASD
		,CATH.ProcCoarc
		,CATH.ProcAorticValv
		,CATH.ProcPulmonaryValv
		,CATH.ProcPDA
		,CATH.ProcProxPAStent
		,CATH.ProcEPCath
		,CATH.ProcEPAblation
		,CATH.ProcTPVR
--		,CathProcedures.SpecificProcID
		,CATH.HospStatus
		,CATH.ProcStatus
		--,(SELECT TOP 1 Contacts.LastName FROM Contacts INNER JOIN CATH C2 ON Contacts.ContactID = C2.OperatorID WHERE C2.CathID = CATH.CathID)  OperLName
		--,(SELECT TOP 1 Contacts.FirstName FROM Contacts INNER JOIN CATH C2 ON Contacts.ContactID = C2.OperatorID WHERE C2.CathID = CATH.CathID)  OperFName
		--,(SELECT TOP 1 Contacts.MiddleName FROM Contacts INNER JOIN CATH C2 ON Contacts.ContactID = C2.OperatorID WHERE C2.CathID = CATH.CathID)  OperMName
		--,(SELECT TOP 1 Contacts.SurgNPI FROM Contacts INNER JOIN CATH C2 ON Contacts.ContactID = C2.OperatorID WHERE C2.CathID = CATH.CathID)  OperNPI
		,CATH.Trainee
		,CATH.SecondParticipating
		,CATH.ProcStartDate
		,CATH.ProcStartTime
		,CATH.ProcEndDate
		,CATH.ProcEndTime
		,CATH.AnesPresent
		,CATH.AnesCalledIn
		,CATH.Sedation
		,CATH.Sedation
		,CATH.AirMng
		,CATH.AirMngLMA
		,CATH.AirMngTrach
		,CATH.AirMngBagMask
		,CATH.AirMngCPAP
		,CATH.AirMngElecIntub
		,CATH.AirMngPrevIntub
		,CATH.AccessLoc
		,CATH.VenAccess
		,CATH.VenLargSheath
		,CathVenousClosureMethod.Sort
		,CathVenousClosureMethod.VenClosureDevID
		,CATH.VenClosureMethodND
		,CATH.ArtAccess
		,CATH.ArtLargSheath
		,CathArterialClosureMethod.Sort
		,CathArterialClosureMethod.ArtClosureDevID
		,CATH.ArtClosureMethodND
		,CATH.FluoroTime
		,CATH.ContrastVol
		,CATH.SysHeparin
		,CATH.ACTMonitor
		,CATH.ACTPeak
		,CATH.Inotrope
		,CATH.InotropeUse
		,CATH.ECMOUse
		,CATH.LVADUse
		,CATH.IABPUse
		,CATH.PlaneUsed
		,CATH.FluoroDoseKerm
		,CATH.FluoroDoseKerm_Units
		,CATH.FluoroDoseDAP
		,CATH.FluoroDoseDAP_Units
		,CathHemodynamics.SystemicArtSat
		,CathHemodynamics.SystemicArtSatNA
		,CathHemodynamics.MixVenSat
		,CathHemodynamics.MixVenSatNA
		,CathHemodynamics.SystemVentSysPres
		,CathHemodynamics.SystemVentSysPresNA
		,CathHemodynamics.SystemVentEndDiaPres
		,CathHemodynamics.SystemVentEndDiaPresNA
		,CathHemodynamics.SystemSysBP
		,CathHemodynamics.SystemSysBPNA
		,CathHemodynamics.SystemDiaBP
		,CathHemodynamics.SystemDiaBPNA
		,CathHemodynamics.SystemMeanBP
		,CathHemodynamics.SystemMeanBPNA
		,CathHemodynamics.PulmArtSysPres
		,CathHemodynamics.PulmArtSysPresNA
		,CathHemodynamics.PulmArtMeanPres
		,CathHemodynamics.PulmArtMeanPresNA
		,CathHemodynamics.PulmVentSysPres
		,CathHemodynamics.PulmVentSysPresNA
		,CathHemodynamics.PulmVascRestInd
		,CathHemodynamics.PulmVascRestIndNA
		,CathHemodynamics.CardInd
		,CathHemodynamics.CardIndNA
		,CathHemodynamics.QpQsRatio
		,CathHemodynamics.QpQsRatioNA
		,CathASDClosure.ASDProcInd
		,CathASDClosure.ASDSeptLength
		,CathASDClosure.ASDSeptLengthNA
		,CathASDClosure.ASDAneurysm
		,CathASDDefect.Sort
		,CathASDDefect.ASDMultiFenestrated
		,CathASDDefect.ASDSize
		,CathASDDefect.ASDSize
		,CathASDDefect.ASDBallSizPerf
		,CathASDDefect.ASDStretchDiameter
		,CathASDDefect.ASDStretchDiameterSize
		,CathASDDefect.ASDStopFlowTech
		,CathASDDefect.ASDStopFlowTechSize
		,CathASDDefect.ASDRimMeas
		,CathASDDefect.ASDIVCRimLength
		,CathASDDefect.ASDAortRimLength
		,CathASDDefect.ASDPostRimLength
		,CathASDDefect.ASDResShunt
		--,CathASDDevice.DevID
		--,CathASDDeviceAssn.DefectCounterAssn
		--,CathASDDeviceAssn.DefectCounterAssn
		--,CathASDDevice.DevOutcome
		,CathCoarcProc.CoarcProcInd
		,CathCoarcProc.CoarcNature
		,CathCoarcProc.CoarcPriorTreat
		,CathCoarcProc.CoarcPreDiameter
		,CathCoarcProc.CoarcPreDiameterNA
		,CathCoarcProc.CoarcPrePkSysGrad
		,CathCoarcProc.CoarcPrePkSysGradNA
		,CathCoarcProc.CoarcPostDiameter
		,CathCoarcProc.CoarcPostDiameterNA
		,CathCoarcProc.CoarcPostPkSysGradNA
		,CathCoarcProc.CoarcPostPkSysGrad
		,CathCoarcProc.CoarcAddlAortObs
		,CathCoarcProc.CoarcAorticArchInter
		,CathCoarcProc.CoarcPreSysGradient
		,CathCoarcProc.CoarcPostSysGradient
		,CathCoarcProcDevice.CoarcDevID
		,CathCoarcProcDevice.CoarcDevType
		,CathCoarcProcDevice.CoarcBallPurp
		,CathCoarcProcDevice.CoarcBallPressure
		,CathCoarcProcDevice.CoarcBallOutcome
		,CathCoarcProcDevice.CoarcStentOutcome
		,CathCoarcProcDevice.CoarcPostInStentDiamAssessed
		,CathCoarcProcDevice.CoarcPostInStentDiameter
		,CathAorticValvuloplasty.AVProcInd
		,CathAorticValvuloplasty.AVMorphology
		,CathAorticValvuloplasty.AVPreInsuff
		,CathAorticValvuloplasty.AVDiameter
		,CathAorticValvuloplasty.AVPrePkSystGrad
		,CathInflationCounter.BallTech
		,CathInflationCounter.SingDevID
		,CathInflationCounter.DoubDevID2
		,CathInflationCounter.SingBallStab
		,CathInflationCounter.SingBallPressure
		,CathInflationCounter.SingBallOutcome
		,CathInflationCounter.PostDilSysGrad
		,CathInflationCounter.PostDilRegurg
		,CathPulmonaryValvuloplasty.PVProcInd
		,CathPulmonaryValvuloplasty.PVMorphology
		,CathPulmonaryValvuloplasty.PVSubStenosis
		,CathPulmonaryValvuloplasty.PVDiameter
		,CathPulmonaryValvuloplasty.PVPrePkSysGrad
		,CathPulmonaryValvuloplasty.PVPrePkSysGradNA
		,CathPulmonaryValvuloplasty.PVBallTech
		,CathPulmonaryValvuloplasty.PVBall1DevID
		,CathPulmonaryValvuloplasty.PVBall2DevID
		,CathPulmonaryValvuloplasty.PVBallStab
		,CathPulmonaryValvuloplasty.PVBallPressure
		,CathPulmonaryValvuloplasty.PVBallOutcome
		,CathPulmonaryValvuloplasty.PVPostPkSysGrad
		,CathPulmonaryValvuloplasty.PVPostPkSysGradNA
		,CathPDAClosure.PDAProcInd
		,CathPDAClosure.PDADiameterAortSide
		,CathPDAClosure.PDAMinLumDiameter
		,CathPDAClosure.PDALength
		,CathPDAClosure.PDAClass
		,CathPDAClosure.PDAPAObst
		,CathPDAClosure.PDAAortObst
		,CathPDAClosure.PDAResShunt
		,CathPDAClosureDevice.PDADevID
		,CathPDAClosureDevice.PDADevOutcome
		,CathPAStent.PASProcInd
		,CathPAStentDefect.PASDefectLoc
		,CathPAStentDefect.PASDisObstruction
		,CathPAStentDefect.PASSideJail
		,CathPAStentDefect.PASSideJailIntended
		,CathPAStentDefect.PASSideJailArtery
		,CathPAStentDefect.PASDSideJailDecFlow
		,CathPAStentDefect.PASPreProxSysPress
		,CathPAStentDefect.PASPreDistSysPress
		,CathPAStentDefect.PASPreProxMeanPress
		,CathPAStentDefect.PASPreDistMeanPress
		,CathPAStentDefect.PASPreProxDiameter
		,CathPAStentDefect.PASPreDistDiameter
		,CathPAStentDefect.PASPreMinDiameter
		,CathPAStentDefect.PASPostProxSysPress
		,CathPAStentDefect.PASPostDistSysPress
		,CathPAStentDefect.PASPostProxMeanPress
		,CathPAStentDefect.PASPostDistMeanPress
		,CathPAStentDefect.PASPostProxDiameter
		,CathPAStentDefect.PASPostDistDiameter
		,CathPAStentDefect.PASPostMinDiameter
		,CathPAStentDevice.PASDevID
		,CathPAStentDeviceAssn.PASDefectCounterAssn
		,CathPAStentDevice.PASDevOutcome
		,CathEvents.CArrest
		,CathEvents.PostArrhyth
		,CathEvents.PostAVBlock
		,CathEvents.PostArrhythResolved
		,CathEvents.PostArrhythMed
		,CathEvents.PostArrhythCardiovers
		,CathEvents.PostArrhythTempPM
		,CathEvents.PostArrhythPermPM
		,CathEvents.PostNewRegurge
		,CathEvents.PostTamponade
		,CathEvents.PostAirEmbolus
		,CathEvents.PostEmbStroke
		,CathEvents.PostDevMalposThrom
		,CathEvents.PostDevMalposThromRetCT
		,CathEvents.PostDevMalposThromRetSurg
		,CathEvents.PostDevEmbol
		,CathEvents.PostDevRetrievePCT
		,CathEvents.PostDevRetrieveSurg
		,CathEvents.PostDialysis
		,CathEvents.PostCorArteryComp
		,CathEvents.PostErosion
		,CathEvents.PostEsoFistula
		,CathEvents.PostLBBB
		,CathEvents.PostIntubation
		,CathEvents.PostRBBB
		,CathEvents.PostECMO
		,CathEvents.PostLVAD
		,CathEvents.PostBleed
		,CathEvents.PostBleedAccessSite
		,CathEvents.PostBleedHematoma
		,CathEvents.PostRetroBleed
		,CathEvents.PostGIBleed
		,CathEvents.PostGUBleed
		,CathEvents.PostOtherBleed
		,CathEvents.PostTransfusion
		,CathEvents.PostDropHgb
		,CathEvents.PostPriorAnemia
		,CathEvents.PostBloodLoss
		,CathEvents.PostECMOBloodReplace
		,CathEvents.PostOtherVasComp
		,CathEvents.PostOtherEvents
		,CathEventOther.PostOtherEventID
		,CathEvents.PostPlanCardiacSurg
		,CathEvents.PostUnplanCardSurg
		,CathEvents.PostUnplanVasSurg
		,CathEvents.PostUnplanOtherSurg
		,CathEvents.PostOtherSurgCathComp
		,CathEvents.PostSubsCath
		,CathEvents.PostPeriNerveInjury
		,CathEvents.PostPhNerveParalysis
		,CathEvents.PostPneumothorax
		,CathEvents.PostPulEmbolism
		,CathEvents.PostPulVeinStenosis
		,CathEvents.PostRadiationBurn
		,CathEvents.PostDVT
		,CathEvents.PostConduitTear
		,CathEvents.PostConduitTearLoc 
		,CathEventsPostConduitTearTreatment.PostConduitTearTreat
		,Hospitalization.CardSurg
		,Hospitalization.DischDt
		,Hospitalization.MtDCStat
		,Mortality.DeathLab
		,Mortality.DeathCause
		,CathEP.EPPrimaryInd
		,CathEP.EPHxCHD
		,CathEP.EPPrevTherapy
		,CathEP.PriorCathAblation
		,CathEP.PriorPharmaTherapy
		,CathEP.PriorChemCard
		,CathEP.PriorDCCard
		,CathEP.PriorPaceInsert
		,CathEP.PriorICDInsert
		,CathEP.PriorArrSurgery
		,CathEP.PriorCathAblaNum
		,CathEP.SSSQ1
		,CathEP.SSSQ2
		,CathEP.SSSQ3
		,CathEP.SSSQ4
		,CathEPSSSQ5.SSSQ5
		,CathEP.SSSQ6
		,CathEP.SSSQ7
		,CathEPTachyarrObs.EPTachyarrObs
		,CathEPSedMed.EPSedMed
		,CathEPMapSys.EPMapSys
		,CathEPAblationIndication.EPAblationIndication
		,CathEPTargetApproach.EPTargetApproach
		,CathEPTarget.EPTargetSub
		,CathEPTarget.EPTargetLocID
		,CathEPTargetMethod.EPTargetMethod
		,CathEPTarget.EPAblationAttempted
		,CathEPTarget.EPAblationNotAttempted
		,CathEPTarget.EPAblationOutcome
		,CathEPDevices.EPAblationCathID
		,CathEPDeviceAssociation.EPTargetAssn
		,CathEPDevices.EPTargetSeconds
		,CathEPDevices.EPActivationsNum
		,CathTPVR.TPVRClinInd
		,CathTPVR.TPVRHemoInd
		,CathTPVR.TPVRRVOTDysfunction
		,CathTPVR.TPVREcho
		,CathTPVR.TPVREchoMeanGradient
		,CathTPVR.TPVREchoMaxGradient
		,CathTPVR.TPVREchoPVRegurg
		,CathTPVR.TPVREchoLVEF
		,CathTPVR.TPVREchoTRS
		,CathTPVR.TPVRMRI
		,CathTPVR.TPVRMRIRVEF
		,CathTPVR.TPVRMRILVEF
		,CathTPVR.TPVRMRIRVEDVIndex
		,CathTPVR.TPVRMRIRVESVIndex
		,CathTPVR.TPVRMRILVEDVIndex
		,CathTPVR.TPVRMRILVESVIndex
		,CathTPVR.TPVRMRIPRFraction
		,CathTPVR.TPVRRVOTType
		,CathTPVR.TPVROriginalConduit
		,CathTPVR.TPVRExistingStent
		,CathTPVR.TPVRPriorTPVR
		,CathTPVR.TPVRCathPeakGradient
		,CathTPVR.TPVRNarrowDia
		,CathTPVR.TPVRAortoPerf
		,CathTPVR.TPVRSelectiveAngio
		,CathTPVR.TPVRCorCompressTest
		,CathTPVR.TPVRMaxBalloonSize
		,CathTPVR.TPVRCorCompressPresent
		,CathTPVR.TPVRPredilationPerf
		,CathTPVR.TPVRFirstBallSize
		,CathTPVR.TPVRMaxBallSize
		,CathTPVR.TPVRHighInflaPerf
		,CathTPVR.TPVRNewPreStent
		,CathTPVR.TPVRNewStentsNum
		,CathTPVR.TPVRAccessVessel
		,CathTPVR.TPVRDeliBallSize
		,CathTPVR.TPVRTPVDeployed
		,CathTPVR.TPVRTPVPostDilation
		,CathTPVR.TPVRFinalBallSize
		,CathTPVR.TPVRFinalPressure
		,CathTPVR.TPVRPeakRVOTGrad
		,CathTPVR.TPVRPostProcPVRegurg
		,CathTPVR.TPVRFinalDiameter
		,CathTPVR.TPVRNotDeployedReason
	--	,CathTPVRDevice.DevID
	--	,CathTPVRDevice.DevOutcome
		,CathTPVR.TPVRPostEcho
		,CathTPVR.TPVRPostEchoMeanGrad
		,CathTPVR.TPVRPostEchoMaxGrad
		,CathTPVR.TPVRPostEchoPulValveRegurg
		,CathFollowUp.F_AssessmentDate
		,CathFollowUpAssn.CathID
		,CathFollowUpAssn.CathID
		,CathFollowUp.F_Method_Office
		,CathFollowUp.F_Method_MedRecord
		,CathFollowUp.F_Method_MedProvider
		,CathFollowUp.F_Method_Phone
		,CathFollowUp.F_Method_SSFile
		,CathFollowUp.F_Method_Hospital
		,CathFollowUp.F_Method_Other
		,CathFollowUp.F_Status
		,CathFollowUp.F_DeathDate
		,CathFollowUp.F_DeathCause
		,CathFollowUp.F_Readmitted
		,CathFollowUp.F_ReadmissionLOS
		,CathFollowUp.F_ReadmissionDate
		,CathFollowUp.F_Hosp
		,CathFollowUp.F_ASDErosion
		,CathFollowUp.F_ASDDeviceEmbol
		,CathFollowUp.F_ASDRetrieveCath
		,CathFollowUp.F_ASDRetrieveSurgery
		,CathFollowUp.F_ASDEndocarditis
		,CathFollowUp.F_ASDEndocarditisDxDate
		,CathFollowUp.F_ASDEndocarditisFactors
		,CathFollowUp.F_ASDRx
		,CathFollowUp.F_ASDResShunt
		,CathFollowUp.F_SSSQ1
		,CathFollowUp.F_SSSQ2
		,CathFollowUp.F_SSSQ3
		,CathFollowUp.F_SSSQ4
		,CathFollowUp.F_SSSQ5
		,CathFollowUp.F_SSSQ6
		,CathFollowUp.F_SSSQ7
		,CathFollowUp.F_TPVInPlace
		,CathFollowUp.F_TPVPertubationReason
		,CathFollowUp.F_TPVReintervention
		,CathFollowUp.F_TPVSurgReinter
		,CathFollowUp.F_TPVSurgReinterDate
		,CathFollowUp.F_TPVCathReinter
		,CathFollowUp.F_TPVCathReinterDate
		,CathFollowUp.F_TPVReinterReason
		,CathFollowUp.F_TPVEndocarditis
		,CathFollowUp.F_TPVEndocarditisDxDate
		,CathFollowUp.F_TPVEndocarditisFactors
		,CathFollowUp.F_TPVEndocarditisRx
		,CathFollowUp.F_TPVMeanValveGradient
		,CathFollowUp.F_TPVMaxValveGradient
		,CathFollowUp.F_TPVPulValveRegurg  

 from CathData CATH
                LEFT JOIN CathPatAnatomy ON CATH.patid = CathPatAnatomy.patid
				LEFT JOIN Hospitalization ON CATH.HospID = Hospitalization.HospitalizationID
                LEFT JOIN CathAorticValvuloplasty ON CATH.CATHID = CathAorticValvuloplasty.CATHID
				LEFT JOIN CathArrhythmiaHistory ON CATH.HOSPID = CathArrhythmiaHistory.HospitalizationID
				LEFT JOIN CathArterialClosureMethod ON CATH.CATHID = CathArterialClosureMethod.CATHID
				LEFT JOIN CathASDClosure ON CATH.CATHID = CathASDClosure.CATHID
				LEFT JOIN CathASDDefect ON CATH.SVDefect = CathASDDefect.DefectID
				LEFT JOIN CathCoarcProc ON CATH.CATHID = CathCoarcProc.CATHID
				LEFT JOIN CathCoarcProcDevice ON CATH.ProcCoarc = CathCoarcProcDevice.CoarcProcID
				--LEFT JOIN CathDiagnosis ON CATH.CATHID = CathDiagnosis.CATHID --separate
				LEFT JOIN CathEP ON CATH.CATHID = CathEP.CATHID
			    LEFT JOIN CathEPTarget ON CathEP.CathEPId = CathEPTarget.CathEPId
			    LEFT JOIN CathEPTargetApproach ON CathEPTarget.TargetID = CathEPTargetApproach.TargetID
				LEFT JOIN CathEPTargetMethod ON CathEPTarget.TargetID = CathEPTargetMethod.TargetID
				LEFT JOIN CathEPAblationIndication ON CathEPTarget.TargetID = CathEPAblationIndication.TargetID
				LEFT JOIN CathEPDevices ON CathEPTarget.CathEPId = CathEPDevices.CathEPId
			    LEFT JOIN CathEPDeviceAssociation ON CathEPDevices.EPDeviceID = CathEPDeviceAssociation.EPDeviceID
		        LEFT JOIN CathInflationCounter ON CathInflationCounter.CATHID = CATH.CATHID
				LEFT JOIN CathEPMapSys ON CATH.CATHID = CathEPMapSys.CATHID
				LEFT JOIN CathEPSedMed ON CATH.CATHID = CathEPSedMed.CATHID
				LEFT JOIN CathEPSSSQ5 ON CATH.CATHID = CathEPSSSQ5.CATHID
				LEFT JOIN CathEPTachyarrObs ON CATH.CATHID = CathEPTachyarrObs.CATHID
				LEFT JOIN CathEvents ON CATH.CATHID = CathEvents.CATHID
				LEFT JOIN CathEventOther ON CathEvents.EventID = CathEventOther.EventID
				LEFT JOIN CathEventsPostConduitTearTreatment ON CATH.CATHID = CathEventsPostConduitTearTreatment.CATHID
				LEFT JOIN CathFollowUp ON CATH.HOSPID = CathFollowUp.HospitalizationID
				LEFT JOIN CathFollowUpAssn ON CATH.CATHID = CathFollowUpAssn.CATHID
				LEFT JOIN CathHemodynamics ON CATH.CATHID = CathHemodynamics.CATHID
				LEFT JOIN CathHistory ON CATH.HOSPID = CathHistory.HospitalizationID
				LEFT JOIN CathPAStent ON CATH.CATHID = CathPAStent.CATHID
				LEFT JOIN CathPAStentDefect ON CathPAStentDefect.PAStentID = CathPAStent.PAStentID
				LEFT JOIN CathPAStentDevice ON CathPAStentDevice.PAStentID = CathPAStent.PAStentID
				LEFT JOIN CathPAStentDeviceAssn ON CathPAStentDeviceAssn.DeviceID = CathPAStentDevice.DeviceID
				LEFT JOIN CathPDAClosure ON CATH.CATHID = CathPDAClosure.CATHID
				LEFT JOIN CathPDAClosureDevice ON CathPDAClosure.PDAClosureID = CathPDAClosure.PDAClosureID
				--LEFT JOIN CathPriorCathProc ON CATH.HOSPID = CathPriorCathProc.HospitalizationID --separate 
				--LEFT JOIN CathPriorSurgProc ON CATH.HOSPID = CathPriorSurgProc.HospitalizationID --separate 
				LEFT JOIN CathPulmonaryValvuloplasty ON CATH.CATHID = CathPulmonaryValvuloplasty.CATHID
				LEFT JOIN CathTPVR ON CATH.CATHID = CathTPVR.CATHID
				LEFT JOIN CathVenousClosureMethod ON CATH.CATHID = CathVenousClosureMethod.CATHID
				LEFT JOIN Demographics ON CATH.patid = Demographics.patid
				LEFT JOIN Mortality ON CATH.PATID = Mortality.PATID
				LEFT JOIN PatAnatomy ON CATH.patid = PatAnatomy.patid
WHERE 1=1
  and demographics.medrecn = '55832029'
  and AdmitDt = '2018-08-13 00:00:00.000'
  --and MEDRECN = '01952241'