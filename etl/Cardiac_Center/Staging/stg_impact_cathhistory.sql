with ip2hrsk as (
select *,
       min(seqno) over (partition by refno order by seqno) as newseq
  from
      {{source('ccis_ods', 'sensis_ip2hrsk')}}
)

select
        surg_enc_id,
        ip2hrsk.ip3200 as chroniclungdisease,
        ip2hrsk.ip3205 as coagdisorder,
        case when ip2hrsk.ip3205 = 1
              then ip2hrsk.ip3210
            else null end as hypercoag,
        case when ip2hrsk.ip3205 = 1
              then ip2hrsk.ip3215
            else null end as hypocoag,
        ip2hrsk.ip3220 as diabetes,
        ip2hrsk.ip3225 as hepaticdisease,
        ip2hrsk.ip3230 as renalinsuff,
        ip2hrsk.ip3235 as seizures,
        ip2hrsk.ip3240 as sicklecell,
        ip2hrsk.ip3250 as priorstroke,
        ip2hrsk.ip3160 as arrhythmia,
--      ip2hrsk.ip3161 arrhythmiahx
        ip2hrsk.ip3170 as priorcm,
        case when ip2hrsk.ip3175 = 1 then 4097
             when ip2hrsk.ip3175 = 2 then 4098
             when ip2hrsk.ip3175 = 3 then 4099
             when ip2hrsk.ip3175 = 4 then 4100
             when ip2hrsk.ip3175 = 5 then 4101
             when ip2hrsk.ip3175 = 6 then 4102
            else null end as priorcmhx,
        ip2hrsk.ip3221 as endocarditis,
        ip2hrsk.ip3222 as hf,
        case when ip2hrsk.ip3223 = 1 then 1423
              when ip2hrsk.ip3223 = 2 then 1424
              when ip2hrsk.ip3223 = 3 then 1425
              when ip2hrsk.ip3223 = 4 then 1426
            else null end as nyha,
        ip2hrsk.ip3224 as hearttransplant,
        ip2hrsk.ip3226 as ischemichd,
        ip2hrsk.ip3227 as kawasakidisease,
        ip2hrsk.ip3231 as rheumatichd

 from
      {{ref('stg_impact_cathstudy')}} as study
      inner join ip2hrsk
          on study.refno = ip2hrsk.refno
             and ip2hrsk.newseq = ip2hrsk.seqno
