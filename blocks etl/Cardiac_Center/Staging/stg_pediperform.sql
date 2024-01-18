select
      coalesce(emrlinkid.casenumber, cases.casenumber) as casenumber,
      reop,
      3304 as asstperfused,
      3304 as studentperfused,
      3342 as atsused,
      atscollvol,
      atsretvol,
      3335 as artpumpty,
      pumpbootsz,
      3319 as artlinety,
      32 as artporesz,
      artlinesz,
      venlinesz,
      3389 as hemoconty,
      oxygenatorty,
      3348 as biocoat,
      3350 as biotype,
      3493 as venreservoirty,
      primefluidvol + 50 as primevol,
      primerbctreat,
      0 as primemedothervol,
      3303 as primemedused,
      arttemphigh,
      lwsttemp,
      lwsttempsrc,
      cpbseptemp,
      phmgmt,
      3304 as phstatwarm,
      phstatcool,
      phstatcoolthresh,
      case when cplegiavol > 0 then 3369 else 0 end as cplegiasystem,
      1 as cplegiabloodratio,
      4 as cplegiacrystratio,
      cplegiavol,
      cplegiacrysvol,
      3505 as vendrainaug,
      5443 as vendrainaugloc,
      -40 as vendrainaugmax,
      autocirc,
      autocircprimevol,
      autoharv,
      0 as autoharvprecpbvol,
      0 as autoharvcpbvol,
      0 as autoharvpostcpbvol,
      autoharvvol,
      bloodprodused,
      cpbcryovol,
      cpbffpvol,
      cpbplatvol,
      cpbrbcvol,
      cpbwholebloodvol,
      noncpbcryovol,
      noncpbffpvol,
      noncpbplatvol,
      noncpbrbcvol,
      noncpbwholebloodvol,
      primecryovol,
      primeffpvol,
      primeplatvol,
      primerbcvol,
      primewholebloodvol,
      modultrafilt,
      modultrafiltty,
      modultrafilttm,
      modultrafiltvolrem,
      coalesce(ultrafilt, 3304) as ultrafilt,
      3304 as zerobalultrafilt,
      0 as zbufvol,
      residpumpvol,
      3490 as residvolprocess,
      residprocessreturn,
      0 as irrigatesolvol,
      0 as wallwastevol,
      0 as collvol,
      ultrafiltvol,
      medvoloncpb,
      (
       coalesce(primevol, 0)
      + coalesce(cplegiacrysvol, 0)
      + 0
      + coalesce(atsretvol, 0)
      + coalesce(plasmalyte_cpb, 0)
      + coalesce(plasmalyte_muf, 0)
      + coalesce(primefluidvol + 50, 0)
      + coalesce(medvoloncpb, 0)
      + coalesce(cpbrbcvol, 0)
      + coalesce(cpbffpvol, 0)
      + coalesce(cpbplatvol, 0)
      + coalesce(cpbcryovol, 0)
      + coalesce(cpbwholebloodvol, 0)
      )
      -
      (
       coalesce(residpumpvol, 0)
      + 0
      + coalesce(atscollvol, 0)
      + coalesce(cpburinevol, 0)
      + coalesce(modultrafiltvolrem, 0)
      + coalesce(ultrafiltvol, 0)
      ) as fluidbal,
      cpburinevol,
      crysvol,
      actbase,
      actpostheparin,
      actmincpb,
      actmaxcpb,
      actpostprot,
      5669 as hepconmeasured,
      creatlst,
      hctbase,
      hctlastprecpb,
      hctfirst,
      lwsthct,
      hctlast,
      hctpostpro,
      hctfirsticu,
      lactatefirstor,
      lactatelastprecpb,
      lactatefirstoncpb,
      lactatelastoncpb,
      lactatepostpro,
      intraopdeath,
      creatfirsticu,
      creatmax48,
      cast(lactatemax24 as decimal(4, 1)) as lactatemax24,
      chesttubeoutlt24,
	current_timestamp as loaddate

 from
      {{ref('cardiac_perfusion_surgery')}} as perfusion
      inner join {{ref('surgery_encounter')}} as surgery
                on perfusion.log_key = surgery.log_key
      inner join {{ref('cardiac_perfusion_bypass')}} as bypass
                on perfusion.anes_visit_key = bypass.visit_key
      inner join {{ref('surgery_encounter_timestamps')}} as timestamps
                on surgery.log_key = timestamps.or_key
      inner join {{ref( 'stg_pediperform_flowsheet_sde' )}} as pediperform_flowsheet_sde
                on pediperform_flowsheet_sde.log_id = surgery.log_id
      inner join {{ref( 'stg_pediperform_labs_meds' )}} as pediperform_labs_meds
                on pediperform_labs_meds.log_id = surgery.log_id
      left join {{source('ccis_ods', 'centripetus_cases')}} as cases
                on cases.caselinknum = timestamps.log_id
      left join {{source('ccis_ods', 'centripetus_emrlinkid')}} as emrlinkid
                on emrlinkid.emreventid = timestamps.log_id
where
     surgery.surgery_date >= '2021-10-01'
