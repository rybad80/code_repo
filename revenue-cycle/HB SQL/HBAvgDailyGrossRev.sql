WITH STEP1 AS (
SELECT 
last_day(MASTER_DATE.FULL_DT) AS POST_DATE,
Sum(FACT_TRANSACTION_HB.CHRG_AMT) AS AMT
FROM 
CDWPRD.ADMIN.FACT_TRANSACTION_HB 
LEFT JOIN CDWPRD.ADMIN."PROCEDURE" ON FACT_TRANSACTION_HB.PROC_KEY = "PROCEDURE".PROC_KEY 
LEFT JOIN CDWPRD.ADMIN.MASTER_DATE ON FACT_TRANSACTION_HB.POST_DT_KEY = MASTER_DATE.DT_KEY
WHERE 
MASTER_DATE.FULL_DT between add_months(date_trunc('month',current_date),-24) and add_months(last_day(current_date),-1)
AND 
"PROCEDURE".PROC_TYPE = 'Charge'
GROUP BY 1
),
STEP2 AS (
SELECT 
POST_DATE as POST_MONTH,
TRUNC(Sum(AMT) OVER(ORDER BY POST_DATE ROWS BETWEEN 2 PRECEDING AND 0 PRECEDING), 2) AS AMT
FROM STEP1
WHERE POST_DATE between add_months(date_trunc('month',current_date),-24) and add_months(last_day(current_date),-1)
),
days AS (
select distinct
MD.FULL_DT as DAYS_MONTH,
FINANCE_COMPANY_DAYS_IN_AR.DAYS_FOR_AVERAGE
from
CHOP_ANALYTICS.BLOCKS_FINANCE.FINANCE_COMPANY_DAYS_IN_AR 
join CDWPRD.ADMIN.MASTER_DATE MD on MD.DT_KEY = METRIC_DATE_KEY
where 
FULL_DT between add_months(date_trunc('month',current_date),-16) and add_months(last_day(current_date),-1)
AND 
ROLLING_MONTH_RANGE = 3
)

SELECT 
POST_MONTH,
ROUND(AMT/DAYS_FOR_AVERAGE) AS AVG_DAILY_GROSS_REV
FROM STEP2
LEFT JOIN days ON days.DAYS_MONTH = step2.POST_MONTH
WHERE POST_MONTH between add_months(date_trunc('month',current_date),-16) and add_months(last_day(current_date),-1)
;