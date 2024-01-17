SELECT CASELINKNUM
       , ICUARRDT
       , INITIALFIO2
       , TEMPICUARR
       , TEMPSITE
       , INITPULSEOX
       , ICUPACULABS
       , PH
       , PCO2
       , PO2
       , BASEEXCESS
       , LACTATE
       , HEMATOCRIT


  FROM CDW_STG_DEV.RYBAD.S_CDW_CCAS_ANESTHESIA_POSTOPICU
 order by 1;