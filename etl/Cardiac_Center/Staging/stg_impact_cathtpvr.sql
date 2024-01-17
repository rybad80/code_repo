select
         surg_enc_id,
         case when tpv2ind.tpvrci = 1 then 4110
              when tpv2ind.tpvrci = 2 then 4111
              when tpv2ind.tpvrci = 3 then 4112
              when tpv2ind.tpvrci = 4 then 4113
              when tpv2ind.tpvrci = 5 then 4114
         else null end as tpvrclinind,
         case when tpv2ind.tpvrhi = 1 then 4132
              when tpv2ind.tpvrhi = 2 then 4133
              when tpv2ind.tpvrhi = 3 then 4134
         else null end as tpvrhemoind,
         case when tpv2ind.i11010 = 1 then 4144
              when tpv2ind.i11010 = 2 then 4146
              when tpv2ind.i11010 = 3 then 4148
              when tpv2ind.i11010 = 4 then 4149
              when tpv2ind.i11010 = 5 then 4145
              when tpv2ind.i11010 = 6 then 4147
         else null end as tpvrrvotdysfunction,
         ip2ppt.i11015 as tpvrecho,
         cast(ip2ppt.i11016 as numeric(4, 1)) as tpvrechomeangradient,
         cast(ip2ppt.i11017 as numeric(4, 1)) as tpvrechomaxgradient,
         case when ip2ppt.i11018 = 0 then 4122
              when ip2ppt.i11018 = 1 then 4123
              when ip2ppt.i11018 = 2 then 4124
              when ip2ppt.i11018 = 3 then 4125
              when ip2ppt.i11018 = 4 then 4126
         else null end as tpvrechopvregurg,
         ip2ppt.i11019 as tpvrecholvef,
         case when ip2ppt.i11020 = 0 then 4127
              when ip2ppt.i11020 = 1 then 4128
              when ip2ppt.i11020 = 2 then 4129
              when ip2ppt.i11020 = 3 then 4130
              when ip2ppt.i11020 = 4 then 4131
         else null end as tpvrechotrs,
         ip2ppt.i11030 as tpvrmri,
         ip2ppt.i11031 as tpvrmrirvef,
         ip2ppt.i11032 as tpvrmrilvef,
         ip2ppt.i11033 as tpvrmrirvedvindex,
         ip2ppt.i11034 as tpvrmrirvesvindex,
         ip2ppt.i11035 as tpvrmrilvedvindex,
         ip2ppt.i11036 as tpvrmrilvesvindex,
         ip2ppt.i11037 as tpvrmriprfraction,
         case when ip2rvot.ip2rvot = 76 then 4150
              when ip2rvot.ip2rvot = 77 then 4154
              when ip2rvot.ip2rvot = 78 then 4155
              when ip2rvot.ip2rvot = 79 then 4156
              when ip2rvot.ip2rvot = 80 then 4151
              when ip2rvot.ip2rvot = 81 then 4152
              when ip2rvot.ip2rvot = 82 then 4153
         else null end as tpvrrvottype,
         ip2rvot.i11041 as tpvroriginalconduit,
         ip2rvot.i11045 as tpvrexistingstent,
         ip2rvot.i11050 as tpvrpriortpvr,
         cast(ip2rvot.i11055 as numeric(5, 2)) as tpvrcathpeakgradient,
         cast(ip2rvot.i11060 as numeric(5, 2)) as tpvrnarrowdia,
         ip2cart.i11065 as tpvraortoperf,
         ip2cart.i11070 as tpvrselectiveangio,
         ip2cart.i11075 as tpvrcorcompresstest,
         cast(ip2cart.i11076 as numeric(4, 1)) as tpvrmaxballoonsize,
         case when ip2cart.i11077 = 1 then 4116
              when ip2cart.i11077 = 2 then 4115
              when ip2cart.i11077 = 3 then 4117
         else null end as tpvrcorcompresspresent,
         ip2int.i11080 as tpvrpredilationperf,
         cast(ip2int.i11081 as numeric(4, 1)) as tpvrfirstballsize,
         cast(ip2int.i11082 as numeric(4, 1)) as tpvrmaxballsize,
         ip2int.i11083 as tpvrhighinflaperf,
         ip2int.i11085 as tpvrnewprestent,
         ip2int.i11086 as tpvrnewstentsnum,
         case when ip2int.i11090 = 1 then 4105
              when ip2int.i11090 = 2 then 4106
              when ip2int.i11090 = 3 then 4107
              when ip2int.i11090 = 4 then 4108
              when ip2int.i11090 = 5 then 4109
         else null end as tpvraccessvessel,
         cast(ip2int.i11095 as numeric(4, 1)) as tpvrdeliballsize,
         ip2int.i11100 as tpvrtpvdeployed,
         ip2int.i11101 as tpvrtpvpostdilation,
         cast(ip2int.i11102 as numeric(4, 1)) as tpvrfinalballsize,
         ip2int.i11103 as tpvrfinalpressure,
         cast(ip2int.i11105 as numeric(4, 1)) as tpvrpeakrvotgrad,
         case when ip2int.i11110 = 0 then 4122
              when ip2int.i11110 = 1 then 4123
              when ip2int.i11110 = 2 then 4124
              when ip2int.i11110 = 3 then 4125
              when ip2int.i11110 = 4 then 4126
         else null end as tpvrpostprocpvregurg,
         cast(ip2int.i11115 as numeric(5, 2)) as tpvrfinaldiameter,
         ip2int.i11120 as tpvrnotdeployedreason,
         null as tpvrpostecho,
         cast(null as numeric(5, 2)) as tpvrpostechomeangrad,
         cast(null as numeric(5, 2)) as tpvrpostechomaxgrad,
         null as tpvrpostechopulvalvergurg

  from {{ref('stg_impact_cathstudy')}} as study
        inner join {{source('ccis_ods', 'sensis_tpv2ind')}} as tpv2ind
           on study.refno = tpv2ind.refno
        inner join {{source('ccis_ods', 'sensis_ip2ppt')}} as ip2ppt
           on study.refno = ip2ppt.refno
        inner join {{source('ccis_ods', 'sensis_ip2rvot')}} as ip2rvot
           on study.refno = ip2rvot.refno
        inner join {{source('ccis_ods', 'sensis_ip2cart')}} as ip2cart
           on study.refno = ip2cart.refno
        inner join {{source('ccis_ods', 'sensis_ip2int')}} as ip2int
           on study.refno = ip2int.refno
