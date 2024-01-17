--Combine Series Assigned Encounters with Non-Series Assigned Encounters 

select * from {{ref('stg_qnr_provider_review_series')}}
 union all
select * from {{ref('stg_qnr_provider_review_visit')}}
