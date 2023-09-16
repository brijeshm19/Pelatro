WITH BOOSTER_FLAG AS 
(
SELECT EVENT_IN_DATE,
CASE WHEN EVENT_IN_DATE>=date('"+context.vLoadDate+"')-7 AND EVENT_IN_DATE<date('"+context.vLoadDate+"') THEN 1 ELSE 0 END FLAG_LAST_7DAYS,
(CASE WHEN EVENT_IN_DATE>=date('"+context.vLoadDate+"')-30 AND EVENT_IN_DATE<date('"+context.vLoadDate+"') THEN 1 
WHEN EVENT_IN_DATE>=date('"+context.vLoadDate+"')-60 AND EVENT_IN_DATE<date('"+context.vLoadDate+"')-30 THEN 2 
WHEN EVENT_IN_DATE>=date('"+context.vLoadDate+"')-90 AND EVENT_IN_DATE<date('"+context.vLoadDate+"')-60 THEN 3
ELSE 0 END) AS FLAG_30_60_90,

(CASE WHEN EVENT_IN_DATE>=CAST(date('"+context.vLoadDate+"') - DAYOFMONTH(date('"+context.vLoadDate+"')-1) AS DATE) AND EVENT_IN_DATE<date('"+context.vLoadDate+"') THEN 1
WHEN EVENT_IN_DATE>=CAST(ADD_MONTHS(date('"+context.vLoadDate+"')-1,-1) - DAYOFMONTH(ADD_MONTHS(date('"+context.vLoadDate+"')-2,-1)) AS DATE)
 AND EVENT_IN_DATE<=CAST((date('"+context.vLoadDate+"') - DAYOFMONTH(date('"+context.vLoadDate+"')-1))-1 AS DATE) THEN 2
WHEN EVENT_IN_DATE>=CAST(ADD_MONTHS(date('"+context.vLoadDate+"')-1,-2) - DAYOFMONTH(ADD_MONTHS(date('"+context.vLoadDate+"')-2,-2)) AS DATE)
 AND EVENT_IN_DATE<=CAST((ADD_MONTHS(date('"+context.vLoadDate+"')-1,-1) - DAYOFMONTH(ADD_MONTHS(date('"+context.vLoadDate+"')-2,-1)))-1 AS DATE) THEN 3
ELSE 0 END) AS FLAG_M0_M1_M2
FROM(
SELECT EVENT_IN_DATE,MAX(1) AS MAX_FLAG
FROM uae_db_prod.ae_prod.event_in_booster
WHERE EVENT_IN_DATE>=date('"+context.vLoadDate+"')-92 AND EVENT_IN_DATE<date('"+context.vLoadDate+"')
GROUP BY 1
)a
ORDER BY 1 DESC
),
BOOSTER_IN_LIFE AS
(
SELECT EVENT_IN_ACCOUNT_NUMBER AS IN_ACCOUNT_NUMBER , 
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='DATA' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS TOTAL_DATA_BOOSTER_QTY,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS TOTAL_NATL_BOOSTER_QTY,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS TOTAL_INTL_BOOSTER_QTY,
MAX(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='DATA' THEN EVENT_IN_DATE    END) AS LAST_DATA_BOOSTER_DATE,
MAX(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_DATE END) AS LAST_NATL_BOOSTER_DATE,
MAX(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_DATE END) AS LAST_INTL_BOOSTER_DATE,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='DATA' AND EVENT_IN_DATE<date('"+context.vLoadDate+"')-30 THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS TOTAL_DATA_BOOSTER_BEFORE30D_QTY,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='VOICE_LCL' AND EVENT_IN_DATE<date('"+context.vLoadDate+"')-30 THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS TOTAL_NATL_BOOSTER_BEFORE30D_QTY,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND EVENT_IN_SERVICE_TYPE='VOICE_INT' AND EVENT_IN_DATE<date('"+context.vLoadDate+"')-30 THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS TOTAL_INTL_BOOSTER_BEFORE30D_QTY
FROM uae_db_prod.ae_prod.event_in_booster
WHERE EVENT_IN_EVENT_GROUP IN ('Booster','Roaming')
GROUP BY EVENT_IN_ACCOUNT_NUMBER
),
BOOSTER_CUST_LAST_90 AS(
SELECT 
EVENT_IN_ACCOUNT_NUMBER,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_LAST_7DAYS=1 AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_NATL_COUNT_LAST_7DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_LAST_7DAYS=1 AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_INTL_COUNT_LAST_7DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_LAST_7DAYS=1 AND EXCEL_SERVICE_TYPE='DATA' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_DATA_COUNT_LAST_7DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=1 AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_NATL_COUNT_M0,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=2 AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_NATL_COUNT_M1,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=3 AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_NATL_COUNT_M2,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=1 AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_INTL_COUNT_M0,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=2 AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_INTL_COUNT_M1,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=3 AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_INTL_COUNT_M2,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=1 AND EXCEL_SERVICE_TYPE='DATA' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_DATA_COUNT_M0,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=2 AND EXCEL_SERVICE_TYPE='DATA' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_DATA_COUNT_M1,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_M0_M1_M2=3 AND EXCEL_SERVICE_TYPE='DATA' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_DATA_COUNT_M2,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=1 AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_NATL_COUNT_LAST_30DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=2 AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_NATL_COUNT_LAST_31_60DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=3 AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_NATL_COUNT_LAST_61_90DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=1 AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_INTL_COUNT_LAST_30DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=2 AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_INTL_COUNT_LAST_31_60DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=3 AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_INTL_COUNT_LAST_61_90DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=1 AND EXCEL_SERVICE_TYPE='DATA' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_DATA_COUNT_LAST_30DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=2 AND EXCEL_SERVICE_TYPE='DATA' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_DATA_COUNT_LAST_31_60DAYS,
SUM(CASE WHEN EVENT_IN_EVENT_GROUP='Booster' AND FLAG_30_60_90=3 AND EXCEL_SERVICE_TYPE='DATA' THEN EVENT_IN_TOTAL_QUANTITY ELSE 0 END) AS BOOSTER_TOTAL_DATA_COUNT_LAST_61_90DAYS
FROM(
SELECT a.*,EXCEL_SERVICE_DESCRIPTION,EXCEL_SERVICE_TYPE
FROM(
SELECT a.EVENT_IN_DATE,EVENT_IN_ACCOUNT_NUMBER,EVENT_IN_EVENT_ID,EVENT_IN_EVENT_GROUP,
FLAG_LAST_7DAYS,FLAG_30_60_90,FLAG_M0_M1_M2,
SUM(EVENT_IN_TOTAL_QUANTITY) AS EVENT_IN_TOTAL_QUANTITY,SUM(EVENT_IN_TOTAL_REVENUE_NET) AS EVENT_IN_TOTAL_REVENUE_NET , 
SUM(EVENT_IN_TOTAL_REVENUE_GROSS) AS EVENT_IN_TOTAL_REVENUE_GROSS, SUM(EVENT_IN_TOTAL_DISCOUNT) AS EVENT_IN_TOTAL_DISCOUNT
FROM uae_db_prod.ae_prod.event_in_booster a
LEFT JOIN BOOSTER_FLAG b on a.EVENT_IN_DATE=b.EVENT_IN_DATE
WHERE EVENT_IN_EVENT_GROUP IN ('Booster','Roaming') AND a.EVENT_IN_DATE>=date('"+context.vLoadDate+"')-92 AND a.EVENT_IN_DATE<date('"+context.vLoadDate+"')
GROUP By 1,2,3,4,5,6,7
)a LEFT JOIN uae_db_prod.ae_stg.po_service_file b on a.EVENT_IN_EVENT_ID=b.EXCEL_SERVICE_EVENT_ID
)b GROUP BY 1
),
EVENT_ALL_PURCHASE AS (
SELECT 
*,
CASE WHEN TOTAL_NATL_BOOSTER_QTY=0 THEN 'Never' WHEN BOOSTER_TOTAL_NATL_COUNT_LAST_30DAYS>=1 AND BOOSTER_TOTAL_NATL_COUNT_LAST_31_60DAYS>=1 THEN 'Frequent'  
WHEN BOOSTER_TOTAL_NATL_COUNT_LAST_30DAYS>=1 AND TOTAL_NATL_BOOSTER_BEFORE30D_QTY<=0 THEN '1st Time' WHEN (BOOSTER_TOTAL_NATL_COUNT_LAST_30DAYS=0 OR BOOSTER_TOTAL_NATL_COUNT_LAST_30DAYS IS NULL) AND TOTAL_NATL_BOOSTER_BEFORE30D_QTY=1 THEN 'One Time' ELSE 'Occasional' END AS BOOSTER_PURCHASE_PROFILE_NATL,
CASE WHEN TOTAL_DATA_BOOSTER_QTY=0 THEN 'Never' WHEN BOOSTER_TOTAL_DATA_COUNT_LAST_30DAYS>=1 AND BOOSTER_TOTAL_DATA_COUNT_LAST_31_60DAYS>=1 THEN 'Frequent'  
WHEN BOOSTER_TOTAL_DATA_COUNT_LAST_30DAYS>=1 AND TOTAL_DATA_BOOSTER_BEFORE30D_QTY<=0 THEN '1st Time' WHEN (BOOSTER_TOTAL_DATA_COUNT_LAST_30DAYS=0 OR BOOSTER_TOTAL_DATA_COUNT_LAST_30DAYS IS NULL) AND TOTAL_DATA_BOOSTER_BEFORE30D_QTY=1 THEN 'One Time' ELSE 'Occasional' END AS BOOSTER_PURCHASE_PROFILE_DATA,
CASE WHEN TOTAL_INTL_BOOSTER_QTY=0 THEN 'Never' WHEN BOOSTER_TOTAL_INTL_COUNT_LAST_30DAYS>=1 AND BOOSTER_TOTAL_INTL_COUNT_LAST_31_60DAYS>=1 THEN 'Frequent'  
WHEN BOOSTER_TOTAL_INTL_COUNT_LAST_30DAYS>=1 AND TOTAL_INTL_BOOSTER_BEFORE30D_QTY<=0 THEN '1st Time' WHEN (BOOSTER_TOTAL_INTL_COUNT_LAST_30DAYS=0 OR BOOSTER_TOTAL_INTL_COUNT_LAST_30DAYS IS NULL) AND TOTAL_INTL_BOOSTER_BEFORE30D_QTY=1 THEN 'One Time' ELSE 'Occasional' END AS BOOSTER_PURCHASE_PROFILE_INTL
FROM BOOSTER_IN_LIFE BL
LEFT JOIN BOOSTER_CUST_LAST_90 BL90 on BL.IN_ACCOUNT_NUMBER = BL90.EVENT_IN_ACCOUNT_NUMBER
)
SELECT 
IN_ACCOUNT_NUMBER,
BOOSTER_TOTAL_NATL_COUNT_M0,
BOOSTER_TOTAL_NATL_COUNT_M1,
BOOSTER_TOTAL_NATL_COUNT_M2,
BOOSTER_TOTAL_INTL_COUNT_M0,
BOOSTER_TOTAL_INTL_COUNT_M1,
BOOSTER_TOTAL_INTL_COUNT_M2,
BOOSTER_TOTAL_DATA_COUNT_M0,
BOOSTER_TOTAL_DATA_COUNT_M1,
BOOSTER_TOTAL_DATA_COUNT_M2,
BOOSTER_PURCHASE_PROFILE_DATA,
BOOSTER_PURCHASE_PROFILE_NATL,
BOOSTER_PURCHASE_PROFILE_INTL,
0 AS BOOSTER_TOTAL_DATA_COUNT,
0 AS BOOSTER_TOTAL_NATL_COUNT,
0 AS BOOSTER_TOTAL_INTL_COUNT,
date('1900-01-01') AS BOOSTER_NATL_EXPIRATION_DATE,
date('1900-01-01') AS BOOSTER_INTL_EXPIRATION_DATE,
date('1900-01-01') AS BOOSTER_DATA_EXPIRATION_DATE,
'' AS BOOSTER_PURCHASE_PROFILE_ANY,
'' AS BOOSTER_COLUMN_01,
'' AS BOOSTER_COLUMN_02,
'' AS BOOSTER_COLUMN_03,
BOOSTER_TOTAL_NATL_COUNT_LAST_7DAYS,
BOOSTER_TOTAL_INTL_COUNT_LAST_7DAYS,
BOOSTER_TOTAL_DATA_COUNT_LAST_7DAYS,
date('1900-01-01') AS FIRST_TIME_BOOSTER_ANY_INLIFE,
date('1900-01-01') AS FIRST_TIME_BOOSTER_DATA_INLIFE,
date('1900-01-01') AS FIRST_TIME_BOOSTER_NATL_INLIFE,
date('1900-01-01') AS FIRST_TIME_BOOSTER_INTL_INLIFE,
date('1900-01-01') AS FIRST_TIME_BOOSTER_VOICE_ROAMING_INLIFE,
BOOSTER_TOTAL_NATL_COUNT_LAST_30DAYS,
BOOSTER_TOTAL_NATL_COUNT_LAST_31_60DAYS,
BOOSTER_TOTAL_NATL_COUNT_LAST_61_90DAYS,
BOOSTER_TOTAL_INTL_COUNT_LAST_30DAYS,
BOOSTER_TOTAL_INTL_COUNT_LAST_31_60DAYS,
BOOSTER_TOTAL_INTL_COUNT_LAST_61_90DAYS,
BOOSTER_TOTAL_DATA_COUNT_LAST_30DAYS,
BOOSTER_TOTAL_DATA_COUNT_LAST_31_60DAYS,
BOOSTER_TOTAL_DATA_COUNT_LAST_61_90DAYS
FROM EVENT_ALL_PURCHASE;