select 
DS_SENDER_IN_ACCOUNT_NUMBER AS IN_ACCOUNT_NUMBER,
--DS_SENDER_AH_ID,
--DS_SENDER_ACTIONID,
DS_SENDER_TRANS_DATE as DATA_SHARING_DONATION_DATE,
-- DS_SENDER_CUST_ID,
--DS_SENDER_MSISDN,
-- SPH_ID,
-- DS_SENDER_SERVICE_ID,
DS_SENDER_SEND_VALUE,
-- SubscriptionId,
StartDate,
EndDate,
-- LAST_PLAN_FLAG, 
sum(case when LAST_PLAN_FLAG = 1 then 10240 - DS_SENDER_SEND_VALUE else 0 end) as DATA_AVAILABLE_TO_SHARE
from (
select *, ROW_NUMBER() OVER(PARTITION BY DS_SENDER_IN_ACCOUNT_NUMBER ORDER BY EndDate desc) as RNK  from (
SELECT 
Ah.Id As DS_SENDER_AH_ID,
Ah.ActionId AS DS_SENDER_ACTIONID,
CAST(Ah.DateTime AS DATE) AS DS_SENDER_TRANS_DATE,
Ah.CustomerId AS DS_SENDER_CUST_ID,
C.NobillAccountNo AS DS_SENDER_IN_ACCOUNT_NUMBER,
C.MSISDN AS DS_SENDER_MSISDN,
SP.Id AS SPH_ID,
SP.ServiceId AS DS_SENDER_SERVICE_ID,
(CounterValuE/1024 /1024)  AS DS_SENDER_SEND_VALUE,
SP.SubscriptionId,
StartDate,
EndDate,
case when CAST(Ah.DateTime AS DATE) >= date(StartDate) then 1 else 0 end as LAST_PLAN_FLAG
FROM uae_db_prod.ae_stg.po_sre_actionhistory Ah
LEFT JOIN uae_db_prod.ae_stg.po_sre_customer C ON AH.CustomerId=C.Id
LEFT JOIN uae_db_prod.ae_stg.po_sre_shareplanhistory SP ON AH.Id=SP.ActionHistoryId 
LEFT JOIN uae_db_prod.ae_stg.po_sre_subscription su on su.Id=SP.SubscriptionId
WHERE ActionId=32 
and CAST(DateTime AS DATE)>='2022-01-01'  
AND AH.StatusId=2 and IsSuccess=1 
)t --where LAST_PLAN_FLAG = 1
-- and DS_SENDER_MSISDN = '971585932249'
)t where RNK = 1
GROUP BY 
DS_SENDER_IN_ACCOUNT_NUMBER,
--DS_SENDER_AH_ID,
-- DS_SENDER_ACTIONID,
DS_SENDER_TRANS_DATE,
--DS_SENDER_CUST_ID,
--DS_SENDER_MSISDN,
--SPH_ID,
--DS_SENDER_SERVICE_ID,
DS_SENDER_SEND_VALUE,
--SubscriptionId,
StartDate,
EndDate
--LAST_PLAN_FLAG
;