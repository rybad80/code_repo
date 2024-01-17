with xwalk as
(select 11985 cathid_tst, 13414 cathid_prd, 27791 patid_tst, 29124 patid_prd, 2061336631 surg_enc_id union all
select 11988,13424,	27742, 27742, 2061368302 union all
select 11986,13426,	27790, 29114, 2061379399 union all
select 11987,13428, 27792, 29142, 2061386058 
)





select xwalk.surg_enc_id, src.* from CathEvents src join xwalk on xwalk.cathid_tst = src.cathid
where cathid in (select cathid from cathdata where EMREVENTID in (2061336631, 2061368302, 2061379399,2061386058))
order by surg_enc_id

