 SELECT 
	--	 pt.lastname PatLName
	--	,pt.firname PatFName
	--	,PT.patid MedRecN
	--	,1 AS SSNNA
	--	,pt.patbirth DOB
	--	,pt.patsex
	--	,SUBSTRING(pt.address,LEN(PT.ADDRESS)-4,5) PatientZip
	--	,STUDY.studate ArrivalDate
	--	-- do we need these ... what does Calc Prev Cath/Surg do?
	--	,priorcaths.prior_cath_flag 
	--	,priorcaths.prior_studies
	--	,priorcaths.last_cath_date
	--    --,Aux3
	--	--,Aux4
	--	--prior procedures is a long list to map
	--	,gencond.ip3100 DiGeorgeSynd
	--	,gencond.ip3105 AlagilleSynd
	--	,gencond.ip3110 Hernia
	--	,gencond.ip3115 DownSynd
	--	,gencond.ip3120 Heterotaxy
	--	,gencond.ip3125 MarfanSynd
	--	,gencond.ip3130 NoonanSynd
	--	,gencond.ip3135 Rubella
	--	,gencond.ip3140 Trisomy13
	--	,gencond.ip3145 Trisomy18
	--	,gencond.ip3150 TurnerSynd
	--	,gencond.ip3155 WilliamsBeurenSynd
	--	,ip2hrsk.ip3160 Arrhythmia
	--	,ip2hrsk.ip3161 ArrhythmiaHx
	--	,ip2hrsk.ip3170 PriorCM
	--	,ip2hrsk.ip3175 PriorCMHx
	--	,ip2hrsk.ip3200 ChronicLungDisease
	--	,ip2hrsk.ip3205 CoagDisorder 
	--	,ip2hrsk.ip3210 HyperCoag
	--	,ip2hrsk.ip3215 HypoCoag
	--	,ip2hrsk.ip3220 Diabetes
	--	,ip2hrsk.ip3221 Endocarditis
	--	,ip2hrsk.ip3222 HF
	--	,ip2hrsk.ip3223 NYHA
	--	,ip2hrsk.ip3224 HeartTransplant
	--	,ip2hrsk.ip3225 HepaticDisease
	--	,ip2hrsk.ip3226 IschemicHD
	--	,ip2hrsk.ip3227 KawasakiDisease
	--	,ip2hrsk.ip3230 RenalInsuff
	--	,ip2hrsk.ip3231 RheumaticHD
	--	,ip2hrsk.ip3235 Seizures
	--	,ip2hrsk.ip3240 SickleCell
	--	,ip2hrsk.ip3250 PriorStroke
	----	,podg.ip4000 PreProcCardDiagID
	--	,prdg.Dx1 PreProcCardDiagID1
	--	,prdg.Dx2 PreProcCardDiagID2
	--	,prdg.Dx3 PreProcCardDiagID3
	--	,PD.HEIGHT Height
	--	,PD.WEIGHT Weight
	--	-- after sheath insert "poc blood gas"
	--	--Hemoglobin (right after cath arrival else most recent in 30 days)
	--	--O2Sat (epic)
	----	,prmeds.ip4045 SVDefect
	--	,prepcon.ip4030 NEC
	--	,prepcon.ip4035 Sepsis
	--	,prepcon.ip4040 Preg
	--	,prmeds.PreProcMed
	--	,prmeds.PreProcAntiarr
	--	,prmeds.PreProcAnticoag
	--	,prmeds.PreProcAntihyp
	--	,prmeds.PreProcAntiplatelet
	--	,prmeds.PreProcBB
	--	,prmeds.PreProcDiuretic
	--	,prmeds.PreProcProsta
	--	,precg.ip4060 PreProcVaso
	--	,precg.ip4060 PreProcSinus
	--	,precg.ip4060 PreProcAET
	--	,precg.ip4060 PreProcSVT
	--	,precg.ip4060 PreProcAfib
	--	,precg.ip4060 PreProcJunct
	--	,precg.ip4060 PreProcIdio
	--	,precg.ip4060 PreProcAVB2
	--	,precg.ip4060 PreProcAVB3
	--	,precg.ip4060 PreProcPaced
	--	,pedpp.ip5000 ProcDxCath
	--	,pedpp.ip5001 ProcASD
	--	,pedpp.ip5002 ProcCoarc
	--	,pedpp.ip5003 ProcAorticValv
	--	,pedpp.ip5004 ProcPulmonaryValv
	--	,pedpp.ip5005 ProcPDA
	--	,pedpp.ip5006 ProcProxPAStent
	--	,pedpp.ip5007 ProcEPCath
	--	,pedpp.ip5008 ProcEPAblation
	--	,pedpp.ip5009 ProcTPVR
	--	,pedpr.Proc1 SpecificProcID1
	--	,pedpr.Proc2 SpecificProcID2
	--	,pedpr.Proc3 SpecificProcID3 
		,aptver2.PTSTAT HospStatus
		,aptver2.DPRSTAT ProcStatus
		,PHYS.doper OperatorName
		,phys.npi1 OperatorNPI
		,aptver2.ip5025 Trainee
		,PHYS.IOPER SecondParticipating
		,CT.PATIM ProcStartDate --Sheath Access
		,CT.PATIM ProcStartTime
		,POCT.PLTIM ProcEndDate
		,POCT.PLTIM ProcEndTime
		,ACT.ACT ACTMonitor
		--,ACT.ACT ACTMonitor MAX
	    ,XRAYBSUM.SKNDS FluoroDoseKerm
		,XRAYBSUM.DOSE FluoroDoseDAP
		,aptver2.ip5060 AnesPresent
		,aptver2.ip5065 AnesCalledIn
		,aptver2.ip5070 Sedation
		,aptver3.ip5070 Sedation
		,imarwy.ip5071 AirMng
		,imarwy.IP5075 AirMngLMA
		,imarwy.IP5075 AirMngTrach
		,imarwy.IP5075 AirMngBagMask
		,imarwy.IP5075 AirMngCPAP
		,imarwy.IP5075 AirMngElecIntub
		,imarwy.IP5075 AirMngPrevIntub
		,ASR.ENTSIT AccessLoc
		,ASR.SHESIZE VenAccess
		,XRAYBSUM.FLTIME FluoroTime
		,ANGIO.COST ContrastVol
		,aptver3.ip5160 Inotrope
		,aptver3.ip5165 InotropeUse
		,aptver3.ip5170 ECMOUse
		,aptver3.ip5175 LVADUse
		,aptver3.ip5180 IABPUse
		,XRAY.PLANES PlaneUsed --use 50827 as example in Sensis
		-- FluoroTime
		--Cum Air Kerma
		--Dose Area Product
		--Contrast Volume
		--Units
		--Dose Area Product Units
		--,hemodyn.sasat SystemicArtSat
		--,hemodyn.sasat SystemicArtSatNA
		--,hemodyn.mvsat MixVenSat
		--,hemodyn.mvsat MixVenSatNA
		--,hemodyn.lvs SystemVentSysPres
		--,hemodyn.lvs SystemVentSysPresNA
		--,hemodyn.lvd SystemVentEndDiaPres
		--,hemodyn.lvd SystemVentEndDiaPresNA
		--,hemodyn.aos SystemSysBP
		--,hemodyn.aos SystemSysBPNA
		--,hemodyn.aod SystemDiaBP
		--,hemodyn.aod SystemDiaBPNA
		--,hemodyn.aom SystemMeanBP
		--,hemodyn.aom SystemMeanBPNA
		--,hemodyn.mpas PulmArtSysPres
		--,hemodyn.mpas PulmArtSysPresNA
		--,hemodyn.mpam PulmArtMeanPres
		--,hemodyn.mpam PulmArtMeanPresNA
		--,hemodyn.rv PulmVentSysPres
		--,hemodyn.rv PulmVentSysPresNA
		--,hemodyn.pvr PulmVascRestInd
		--,hemodyn.pvr PulmVascRestIndNA
		--,hemodyn.ci CardInd
		--,hemodyn.ci CardIndNA
		--,hemodyn.qpqs QpQsRatio
		--,hemodyn.qpqs QpQsRatioNA
		--,asdata.ip7000 ASDProcInd   
		--,asdata.ip7005 ASDSeptLength
		--,ASDATA.IP7005 ASDSeptLengthNA
		--,asdata.ip7010 ASDAneurysm


		--,asdefct.ip7025 ASDSize
		--,asdata.ip7025 ASDSize
		--,asdefct.ip7030 ASDBallSizPerf
		--,asdefct.ip7035 ASDStretchDiameter
		--,asdefct.ip7040 ASDStretchDiameterSize
		--,asdefct.ip7045 ASDStopFlowTech
		--,asdefct.ip7050 ASDStopFlowTechSize
		--,asdefct.ip7055 ASDRimMeas
		--,asdefct.ip7060 ASDIVCRimLength
		--,asdefct.ip7065 ASDAortRimLength
		--,asdefct.ip7066 ASDPostRimLength
		--,asdefct.ip7080 ASDResShunt
		--,asdefct.ip7020 Sort
		--,I_ASDCL.IMPACT DevID
		,ipdevce.ip7089 DefectCounterAssn
		,ipdevce.DevOutcome1 
		,ipdevce.DevOutcome2 
		,ipdevce.DevOutcome3 
		--,coadata.ip7100 CoarcProcInd
		--,COADATA.CPOTOC CoarcNature
		--,coadata.ip7102 CoarcPriorTreat
		--,coadata.ip7105 CoarcPreDiameter
		--,coadata.ip7105 CoarcPreDiameterNA
		--,coadata.ip7110 CoarcPrePkSysGrad
		--,coadata.ip7110 CoarcPrePkSysGradNA
		--,coadata.ip7120 CoarcPostDiameter
		--,coadata.ip7120 CoarcPostDiameterNA
		--,coadata.ip7125 CoarcPostPkSysGradNA
		--,coadata.ip7125 CoarcPostPkSysGrad
		--,coadata.ip7126 CoarcAddlAortObs
		--,coadata.ip7127 CoarcAorticArchInter
		--,coadata.ip7128 CoarcPreSysGradient
		--,coadata.ip7129 CoarcPostSysGradient
		--,coadfect.ip7135 CoarcDevID
		--,coadfect.ip7140 CoarcDevType
		--,coadfect.ip7145 CoarcBallPurp
		--,coadfect.ip7150 CoarcBallPressure
		--,coadfect.ip7155 CoarcBallOutcome
		--,coadfect.ip7160 CoarcStentOutcome
		--,coadfect.ip7164 CoarcPostInStentDiamAssessed
		--,coadfect.ip7165 CoarcPostInStentDiameter
		--,aovdata.ip7200 AVProcInd
		--,aovdata.ip7205 AVMorphology
		--,aovdata.ip7210 AVPreInsuff
		--,aovdata.ip7215 AVDiameter
		--,aovdata.ip7220 AVPrePkSystGrad
		,sbalteq.ip7245 SingBallStab
		,sbalteq.ip7250 SingBallPressure
		,IP2INT.i11105 PostDilSysGrad
		,IP2INT.i11110 PostDilRegurg
		,pvdata.ip7400 PVProcInd
		,pvdata.ip7405 PVMorphology
		,pvdata.ip7410 PVSubStenosis
		,pvdata.ip7415 PVDiameter
		,pvdata.ip7420 PVPrePkSysGrad
		,ipdindic.ip7525 PVBall1DevID
		,SBALTEQ.IP7245
		,DBALTEQ.IP7275
		,DBALTEQ.IP7295 PVBallStab
	--	,ipdevce.ip7090 PVBallOutcome
		,sbalteq.ip7260 PVPostPkSysGrad
	--	,ipdevce.impact PVBall2DevID	
	--	,sbalteq.ip7250 PVBallPressure		
	--	,I_PDACLOE.IMPACT pdaDevID	
	--	,I_PASTNT.IMPACT pasDevID	
		--,ipdindic.ip7600 PDAProcInd
		--,ipdindic.ip7605 PDADiameterAortSide
		--,ipdindic.ip7610 PDAMinLumDiameter
		--,ipdindic.ip7615 PDALength
		--,ipdindic.ip7620 PDAClass
		--,ipdindic.ip7630 PDAPAObst
		--,ipdindic.ip7635 PDAAortObst
		--,ipdindic.ip7640 PDAResShunt
		--,ipppas.ip7700 PASProcInd
		--,ipprdef.ip7705 
		--,ipvent1.ip7705 
		--,ipvent2.ip7705 
		--,ipprdef.ip7710 PASDefectLoc
		--,ipprdef.ip7720 PASDisObstruction
		--,ipprdef.ip7725 PASSideJail
		--,ipprdef.ip7730 PASSideJailIntended
		--,ipprdef.ip7735 PASSideJailArtery
		--,ipprdef.ip7740 PASDSideJailDecFlow
		--,ipvent1.ip7745 PASPreProxSysPress
		--,ipvent1.ip7750 PASPreDistSysPress
		--,ipvent1.ip7755 PASPreProxMeanPress
		--,ipvent1.ip7760 PASPreDistMeanPress
		--,ipvent1.ip7765 PASPreProxDiameter
		--,ipvent1.ip7770 PASPreDistDiameter
		--,ipvent1.ip7775 PASPreMinDiameter
		--,ipvent2.ip7785 PASPostProxSysPress
		--,ipvent2.ip7790 PASPostDistSysPress
		--,ipvent2.ip7795 PASPostProxMeanPress
		--,ipvent2.ip7800 PASPostDistMeanPress
		--,ipvent2.ip7805 PASPostProxDiameter
		--,ipvent2.ip7810 PASPostDistDiameter
		--,ipvent2.ip7815 PASPostMinDiameter
		--,IPPRDEF.IP7705 DefectCounterAssn
		--,IPDEVCE.IP7090 DevOutcome
-- Intra & Post Procedure Treatments (
		--,ippoevnt.ip8000 CArrest
		--,ippoevnt.ip8005 PostArrhyth
		--,ippoevnt.ip8006 PostAVBlock
		--,ippoevnt.ip8007 PostArrhythResolved
		--,ippoevnt.ip8010 PostArrhythMed
		--,ippoevnt.ip8015 PostArrhythCardiovers
		--,ippoevnt.ip8020 PostArrhythTempPM
		--,ippoevnt.ip8025 PostArrhythPermPM
		--,ippoevnt.ip8030 PostNewRegurge
		--,ippoevnt.ip8035 PostTamponade
		--,ippoevnt.ip8040 PostAirEmbolus
		--,ippoevnt.ip8045 PostEmbStroke
		--,ippoevnt.ip8050 PostDevMalposThrom
		--,IPPOEVNT.IP8050 PostDevMalposThromRetCT
		--,ippoevnt.ip8055 PostDevEmbol
		--,ippoevnt.ip8060 PostDevRetrievePCT
		--,ippoevnt.ip8065 PostDevRetrieveSurg
		--,ippoevnt.ip8070 PostDialysis
		--,ippoevnt.ip8071 PostCorArteryComp
		--,ippoevnt.ip8072 PostErosion
		--,ippoevnt.ip8073 PostEsoFistula
		--,ippoevnt.ip8074 PostLBBB
		--,ippoevnt.ip8075 PostIntubation
		--,ippoevnt.ip8076 PostRBBB
		--,ippoevnt.ip8080 PostECMO
		--,ippoevnt.ip8085 PostLVAD
		--,ippoevnt.ip8090 PostBleed
		--,ippoevnt.ip8095 PostBleedAccessSite
		--,ippoevnt.ip8100 PostBleedHematoma
		--,ippoevnt.ip8110 PostRetroBleed
		--,ippoevnt.ip8115 PostGIBleed
		--,ippoevnt.ip8120 PostGUBleed
		--,ippoevnt.ip8125 PostOtherBleed
		--,ippoevnt.ip8130 PostTransfusion
		--,ippoevnt.ip8131 PostDropHgb
		--,ippoevnt.ip8132 PostPriorAnemia
		--,ippoevnt.ip8133 PostBloodLoss
		--,ippoevnt.ip8134 PostECMOBloodReplace
		--,ippoevnt.ip8140 PostOtherVasComp
		--,ippoevnt.ip8145 PostOtherEvents
		,cm.Event1 PostOtherEventID1
		,cm.Event2 PostOtherEventID2
		,cm.Event3 PostOtherEventID3
		--,ippeven2.ip8155 PostPlanCardiacSurg
		--,ippeven2.ip8160 PostUnplanCardSurg
		--,ippeven2.ip8165 PostUnplanVasSurg
		--,ippeven2.ip8170 PostUnplanOtherSurg
		--,ippeven2.ip8175 PostOtherSurgCathComp
		--,ippeven2.ip8180 PostSubsCath
		--,ippoevnt.ip8200 PostPeriNerveInjury
		--,ippoevnt.ip8205 PostPhNerveParalysis
		--,ippoevnt.ip8210 PostPneumothorax
		--,ippoevnt.ip8215 PostPulEmbolism
		--,ippoevnt.ip8220 PostPulVeinStenosis
		--,ippoevnt.ip8225 PostRadiationBurn
		--,ippoevnt.ip8230 PostDVT
		--,ippoevnt.ip8235 PostConduitTear
		--,ippoevnt.ip8236 PostConduitTearLoc 
		--,ippoevnt.ip8237 PostConduitTearTreat
		--,tpv2ind.TPVRCI TPVRClinInd
		--,tpv2ind.TPVRHI TPVRHemoInd
		--,tpv2ind.i11010 TPVRRVOTDysfunction
		--,ip2ppt.i11015 TPVREcho
		--,ip2ppt.i11016 TPVREchoMeanGradient
		--,ip2ppt.i11017 TPVREchoMaxGradient
		--,ip2ppt.i11018 TPVREchoPVRegurg
		--,ip2ppt.i11019 TPVREchoLVEF
		--,ip2ppt.i11020 TPVREchoTRS
		--,ip2ppt.i11030 TPVRMRI
		--,ip2ppt.i11031 TPVRMRIRVEF
		--,ip2ppt.i11032 TPVRMRILVEF
		--,ip2ppt.i11033 TPVRMRIRVEDVIndex
		--,ip2ppt.i11034 TPVRMRIRVESVIndex
		--,ip2ppt.i11035 TPVRMRILVEDVIndex
		--,ip2ppt.i11036 TPVRMRILVESVIndex
		--,ip2ppt.i11037 TPVRMRIPRFraction
		--,ip2rvot.IP2RVOT TPVRRVOTType
		--,ip2rvot.i11041 TPVROriginalConduit
		--,ip2rvot.i11045 TPVRExistingStent
		--,ip2rvot.i11050 TPVRPriorTPVR
		--,ip2rvot.i11055 TPVRCathPeakGradient
		--,ip2rvot.i11060 TPVRNarrowDia
		--,ip2cart.i11065 TPVRAortoPerf
		--,ip2cart.i11070 TPVRSelectiveAngio
		--,ip2cart.i11075 TPVRCorCompressTest
		--,ip2cart.i11076 TPVRMaxBalloonSize
		--,ip2cart.i11077 TPVRCorCompressPresent
		--,ip2int.i11080 TPVRPredilationPerf
		--,ip2int.i11081 TPVRFirstBallSize
		--,ip2int.i11082 TPVRMaxBallSize
		--,ip2int.i11083 TPVRHighInflaPerf
		--,ip2int.i11085 TPVRNewPreStent
		--,ip2int.i11086 TPVRNewStentsNum
		--,ip2int.i11090 TPVRAccessVessel
		--,ip2int.i11095 TPVRDeliBallSize
		--,ip2int.i11100 TPVRTPVDeployed
		--,ip2int.i11101 TPVRTPVPostDilation
		--,ip2int.i11102 TPVRFinalBallSize
		--,ip2int.i11103 TPVRFinalPressure
		--,ip2int.i11105 TPVRPeakRVOTGrad
		--,ip2int.i11110 TPVRPostProcPVRegurg
		--,ip2int.i11115 TPVRFinalDiameter
		--,ip2int.i11120 TPVRNotDeployedReason
		,ip2dvce.i11135 DevOutcome
		,DISCH.DDEATH DeathLab
		,DISCH.DCAUSE DeathCause
		--SELECT STUDY.REFNO 

		--SELECT T.NAME TABLENAME, C.NAME COLUMNNAME FROM SYS.COLUMNS C JOIN SYS.TABLES T ON C.OBJECT_ID = T.OBJECT_ID WHERE C.NAME = 'AirMngLMA'
 FROM STUDY left join (select current_study.refno
                             ,1 as prior_cath_flag
                             ,count(prior_studies.refno) prior_studies
							 ,max(prior_studies.studate) last_cath_date
                        from study current_study join study prior_studies on current_study.patno = prior_studies.patno and prior_studies.studate < current_study.studate
					 group by current_study.refno
					   ) priorcaths on priorcaths.refno = study.refno
            left join aovdata ON STUDY.REFNO = aovdata.REFNO
			left join (SELECT REFNO, MAX(act) ACT FROM ACT GROUP BY REFNO) ACT on act.refno = study.refno
			left join (select sum(cost) cost, refno from angio group by refno) angio ON STUDY.REFNO = angio.refno
			left join aptver ON STUDY.REFNO = aptver.REFNO
			left join aptver2 ON STUDY.REFNO = aptver2.REFNO
			left join aptver3 ON STUDY.REFNO = aptver3.REFNO
			left join asdata ON STUDY.REFNO = asdata.REFNO
			left join asdefct ON STUDY.REFNO = asdefct.REFNO
			left join (select asr.*,
							   row_number() over (partition by refno order by shesize desc) shesize_ord
					   from ASR) ASR on STUDY.refno = asr.refno and shesize_ord = 1
			left join coadata ON STUDY.REFNO = coadata.REFNO
			left join coadfect ON STUDY.REFNO = coadfect.REFNO
			left join CT on STUDY.REFNO = CT.REFNO
			left join PATIENT PT ON STUDY.patno = PT.patno
			left join DISCH on STUDY.REFNO = DISCH.REFNO
			left join dbalteq ON STUDY.REFNO = dbalteq.REFNO
			left join gencond ON STUDY.REFNO = gencond.REFNO
			left join hemodyn ON STUDY.REFNO = hemodyn.REFNO
			left join imarwy ON STUDY.REFNO = imarwy.REFNO
			--LEFT JOIN I_ASDCL ON I_ASDCL.REFNO = STUDY.refno
			left join ip2cart ON STUDY.REFNO = ip2cart.REFNO
			left join ip2dvce ON STUDY.REFNO = ip2dvce.REFNO
			left join ip2hrsk ON STUDY.REFNO = ip2hrsk.REFNO
			left join ip2int ON STUDY.REFNO = ip2int.REFNO
			left join ip2ppt ON STUDY.REFNO = ip2ppt.REFNO
			left join ip2rvot ON STUDY.REFNO = ip2rvot.REFNO
		--	left join ipdevce ON STUDY.REFNO = ipdevce.REFNO
		    left join (SELECT ipdevce.refno
			                  ,ip7089
			                  ,min(case when ipdevce.seqno = 1 then dicip7090.meaning end) as DevOutcome1
							  ,min(case when ipdevce.seqno = 2 then dicip7090.meaning end) as DevOutcome2
							  ,min(case when ipdevce.seqno = 3 then dicip7090.meaning end) as DevOutcome3
			           FROM ipdevce 
					              --left join dicip7089 on dicip7089.code = ipdevce.ip7089
					                left join dicip7090 on dicip7090.code = ipdevce.ip7090
					   group by ipdevce.refno,ip7089) ipdevce ON STUDY.REFNO = ipdevce.REFNO
			left join ipdindic ON STUDY.REFNO = ipdindic.REFNO
		--	left join iphxrsk ON STUDY.REFNO = iphxrsk.REFNO
			left join ippeven2 ON STUDY.REFNO = ippeven2.REFNO
			left join ippoevnt ON STUDY.REFNO = ippoevnt.REFNO
			left join ipppas ON STUDY.REFNO = ipppas.REFNO
			left join ipprdef ON STUDY.REFNO = ipprdef.REFNO
			left join ipvent1 ON STUDY.REFNO = ipvent1.REFNO
			left join ipvent2 ON STUDY.REFNO = ipvent2.REFNO
			left join pd ON STUDY.REFNO = pd.REFNO
			left join pedpp ON STUDY.REFNO = pedpp.REFNO
		--	left join pedpr ON STUDY.REFNO = pedpr.REFNO
			left join (SELECT REFNO, DOPER, NPI1, IOPER FROM PHYS WHERE SEQNO = 1) PHYS ON STUDY.REFNO = PHYS.REFNO
			left join POCT ON STUDY.REFNO = POCT.REFNO
		--	left join podg ON STUDY.REFNO = podg.REFNO --is this for C3PO?
		--	left join prdg ON STUDY.REFNO = prdg.REFNO
			left join prepcon ON prepcon.refno = STUDY.REFNO
			left join pvdata ON STUDY.REFNO = pvdata.REFNO
			left join sbalteq ON STUDY.REFNO = sbalteq.REFNO
			left join tpv2ind ON STUDY.REFNO = tpv2ind.REFNO
			left join (select case when count(distinct plane) = 1 then 'Single Plane'
			                       when count(distinct plane) = 2 then 'Biplane'
								   end Planes ,refno from XRAY group by refno) xray ON STUDY.REFNO = XRAY.REFNO
            left join XRAYBSUM ON STUDY.REFNO = XRAYBSUM.REFNO
			left join (SELECT pedpr.refno
			                  ,min(case when pedpr.seqno = 1 then meaning end) as Proc1
							  ,min(case when pedpr.seqno = 2 then meaning end) as Proc2
							  ,min(case when pedpr.seqno = 3 then meaning end) as Proc3
			           FROM pedpr left join dicip5010 on dicip5010.code = pedpr.ip5010 --is this mapped correctly?
					   group by pedpr.refno) pedpr ON STUDY.REFNO = pedpr.REFNO
  			left join (SELECT cm.refno
			                  ,min(case when cm.seqno = 1 then heading end) as Event1
							  ,min(case when cm.seqno = 2 then heading end) as Event2
							  ,min(case when cm.seqno = 3 then heading end) as Event3
			           FROM cm left join catip8150 on catip8150.seqno = cm.ip8150 --is this mapped correctly?
					   group by cm.refno) cm ON STUDY.REFNO = cm.REFNO
			left join (SELECT *
			           FROM precg) precg ON STUDY.REFNO = precg.REFNO
			left join (SELECT  prmeds.refno
			                  ,min(case when prmeds.ip4045 = 1	then '1' else '2' end)	 PreProcMed
							  ,min(case when prmeds.ip4045 = 2	then '1' else '2' end)   PreProcAntiarr
							  ,min(case when prmeds.ip4045 = 3	then '1' else '2' end)	 PreProcAnticoag
							  ,min(case when prmeds.ip4045 = 4	then '1' else '2' end)	 PreProcAntihyp
							  ,min(case when prmeds.ip4045 = 5	then '1' else '2' end)	 PreProcAntiplatelet
							  ,min(case when prmeds.ip4045 = 6	then '1' else '2' end)	 PreProcBB
							  ,min(case when prmeds.ip4045 = 7	then '1' else '2' end)	 PreProcDiuretic
							  ,min(case when prmeds.ip4045 = 9	then '1' else '2' end)	 PreProcProsta
			           FROM prmeds left join dicip4045 on dicip4045.code = prmeds.ip4045 
					   group by prmeds.refno) prmeds ON STUDY.REFNO = prmeds.REFNO
  			left join (SELECT prdg.refno
			                  ,min(case when prdg.seqno = 1 then meaning end) as Dx1
							  ,min(case when prdg.seqno = 2 then meaning end) as Dx2
							  ,min(case when prdg.seqno = 3 then meaning end) as Dx3
			           FROM prdg left join dicip4000 on dicip4000.code = prdg.ip4000 --is this mapped correctly?
					   group by prdg.refno) prdg ON STUDY.REFNO = prdg.REFNO
			left join dici11010 on dici11010.code = i11010
			left join dici11077 on dici11077.code = i11077
			left join dici11090 on dici11090.code = i11090
			left join dici11120 on dici11120.code = i11120
			left join dicip3161 on dicip3161.code = ip3161
			left join dicip3175 on dicip3175.code = ip3175
			left join dicip3205 on dicip3205.code = ip2hrsk.ip3205
			left join dicip3223 on dicip3223.code = ip3223
		--	left join dicip4000 on dicip4000.code = ip4000
			left join dicip5000 on dicip5000.code = ip5000
			left join dicip5070 on dicip5070.code = aptver3.ip5070 --or aptver2
			left join dicip5075 on dicip5075.code = ip5075
			left join dicip5165 on dicip5165.code = ip5165
			left join dicip5170 on dicip5170.code = ip5170
			left join dicip5180 on dicip5180.code = ip5180
			left join dicip7000 on dicip7000.code = ip7000
			left join dicip7030 on dicip7030.code = ip7030
			left join dicip7075 on dicip7075.code = ip7075
			left join dicip7080 on dicip7080.code = ip7080
		--	left join dicip7090 on dicip7090.code = ip7090
			left join dicip7100 on dicip7100.code = ip7100
			left join dicip7102 on dicip7102.code = ip7102
			left join dicip7140 on dicip7140.code = ip7140
			left join dicip7145 on dicip7145.code = ip7145
			left join dicip7160 on dicip7160.code = ip7160
			left join dicip7200 on dicip7200.code = ip7200
			left join dicip7205 on dicip7205.code = ip7205
			left join dicip7210 on dicip7210.code = ip7210
		--	left join dicip7235 on dicip7235.code = ip7235 --need to join table for 7235
		--	left join dicip7255 on dicip7255.code = ip7255 --need to join table for ip7255
			left join dicip7400 on dicip7400.code = ip7400
			left join dicip7405 on dicip7405.code = ip7405
			left join dicip7600 on dicip7600.code = ip7600
			left join dicip7620 on dicip7620.code = ip7620
		--	left join dicip7625 on dicip7625.code = ip7625
			left join dicip7640 on dicip7640.code = ip7640
			left join dicip7700 on dicip7700.code = ip7700
			left join dicip7710 on dicip7710.code = ip7710
			left join dicip7725 on dicip7725.code = ip7725
			left join dicip7735 on dicip7735.code = ip7735
		--	left join dicip7825 on dicip7825.code = ip7825
			left join dicip8005 on dicip8005.code = ippoevnt.ip8005
			left join dicip8006 on dicip8006.code = ippoevnt.ip8006
			left join dicip8007 on dicip8007.code = ippoevnt.ip8007
			left join dicip8050 on dicip8050.code = ippoevnt.ip8050
			left join dicip8071 on dicip8071.code = ippoevnt.ip8071
			left join dicip8072 on dicip8072.code = ippoevnt.ip8072
			left join dicip8073 on dicip8073.code = ippoevnt.ip8073
			left join dicip8074 on dicip8074.code = ippoevnt.ip8074
			left join dicip8076 on dicip8076.code = ippoevnt.ip8076
			left join dicip8131 on dicip8131.code = ippoevnt.ip8131
			left join dicip8132 on dicip8132.code = ippoevnt.ip8132
			left join dicip8133 on dicip8133.code = ippoevnt.ip8133
			left join dicip8236 on dicip8236.code = ippoevnt.ip8236
			left join dicip8237 on dicip8237.code = ippoevnt.ip8237
where 1=1
 AND (PT.PATID = '55832029' and STUDATE = '2018-08-13 00:00:00.000')

 SELECT * FROM SYS.TABLES T JOIN SYS.COLUMNS C ON T.OBJECT_ID = C.OBJECT_ID
 WHERE C.NAME = 'IMPUSE'


 select * from cathdata