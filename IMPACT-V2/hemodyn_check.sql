select stghemo.*,
       hemo.*
  from chop_impact_cathhemodynamics stghemo
       inner join cathdata                     on cathdata.emreventid = stghemo.surg_enc_id
       left join  cathhemodynamics hemo        on hemo.cathid = cathdata.cathid
 where loaddt > '2019-10-01'
       and 
       (hemo.SystemicArtSat is null
		and hemo.MixVenSat is null
		and hemo.SystemVentSysPres is null
		and hemo.SystemVentEndDiaPres is null
		and hemo.SystemSysBP is null
		and hemo.SystemDiaBP is null
		and hemo.SystemMeanBP is null
		and hemo.PulmArtSysPres is null
		and hemo.PulmArtMeanPres is null
		and hemo.PulmVentSysPres is null
		and hemo.PulmVascRestInd is null
		and hemo.CardInd is null
		and hemo.QpQsRatio is null
		)
	   and 	
	   (hemo.SystemicArtSat is not null
		or hemo.MixVenSat is not null
		or hemo.SystemVentSysPres is not null
		or hemo.SystemVentEndDiaPres is not null
		or hemo.SystemSysBP is not null
		or hemo.SystemDiaBP is not null
		or hemo.SystemMeanBP is not null
		or hemo.PulmArtSysPres is not null
		or hemo.PulmArtMeanPres is not null
		or hemo.PulmVentSysPres is not null
		or hemo.PulmVascRestInd is not null
		or hemo.CardInd is not null
		or hemo.QpQsRatio is not null
		)

		