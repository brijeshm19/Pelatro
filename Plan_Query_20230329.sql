
WITH nobill_snapshot_daily_balance AS (
SELECT counter_id,account_num,msisdn,snapshotdate,balance,IN_RANK FROM(SELECT * ,
row_number() over (partition by account_num,countername order by snapshotdate DESC) as IN_RANK
FROM uae_db_prod.ae_stg.nobill_snapshot_daily_balance
WHERE snapshotdate>='"+context.vLoadDate+"'-90 and  countername in ('AC.AnnualPlanFunds_Counter','AC.AnnualPlanPromotions_Ctr') AND balance>0 )fr
WHERE IN_RANK=1
),
ANNUAL_RUNNING_BALANCE AS 
(
SELECT NobillAccountNo, Annual_Counter_Balance 
FROM(
SELECT NobillAccountNo,SUM(facc_postvalue) AS Annual_Counter_Balance
FROM(
SELECT 
a1.*,
CASE WHEN facc_account_number IS NULL THEN account_num ELSE facc_account_number END AS NobillAccountNo,
counter_id,
account_num,
msisdn,
snapshotdate,
balance
FROM(
SELECT 
facc_date,
facc_time_stamp,
facc_account_number,
facc_counter_type_id,
facc_counterid,
facc_valuediff,
facc_updatetype,
facc_postvalue,
facc_msisdn,
facc_seqno,
  ROW_NUMBER() OVER (PARTITION BY facc_counterid ORDER BY facc_time_stamp DESC ,facc_updatetype ASC , facc_postvalue ASC) AS COUNTER_ID_RANK ,
MAX(facc_account_number) OVER (PARTITION BY facc_counterid ) AS Annual_accountnum
from ae_prod.nobill_fact_account_counter a
WHERE facc_counter_type_id IN (220,271) AND facc_date>='"+context.vLoadDate+"'-90 AND facc_date<'"+context.vLoadDate+"'
)a1 LEFT JOIN nobill_snapshot_daily_balance b1 ON a1.facc_counterid=b1.counter_id
WHERE COUNTER_ID_RANK=1 AND (facc_account_number>1 OR account_num>1)
)c1 GROUP BY NobillAccountNo
)L1 WHERE Annual_Counter_Balance>0
)
,
package_price AS 
(
SELECT * ,(TotalCost - TotalCost*0.3) AS TotalCost30P , (TotalCost - TotalCost*0.5) AS TotalCost50P
FROM (
SELECT ProductCode, PackagePriceServices , CAST((TotalCost - DiscountValue ) AS float) AS TotalCost
FROM (
SELECT * , ROW_NUMBER() OVER (PARTITION BY ProductCode , PackagePriceServices  ORDER BY LastModifiedDate DESC) AS SERVICE_RANK
FROM uae_db_prod.ae_stg.po_get_package_price ) ser WHERE SERVICE_RANK=1
)pap
),
view_plan_services AS (
SELECT ServiceId AS PO_EXCEL_SERVICE_ID,EventId AS EVENT_IN_EVENT_ID,UPPER(dev_category_6) AS EVENT_NAME,UPPER(dev_category_3) AS EVENT_CATEGORY
FROM uae_db_prod.ae_stg.po_po_bundle bun
LEFT JOIN (SELECT * FROM (SELECT *,ROW_NUMBER()OVER(PARTITION BY dev_eventid_base ORDER BY dev_event_id) row_num FROM ae_prod.nobill_dim_event EVENT_DIM)a WHERE row_num=1)EVENT_DIM ON  bun.EventId = EVENT_DIM.dev_eventid_base
),
CUS_PRODUCT AS (
SELECT ProductCode , ProductName ,
CASE WHEN ProductName like 'Product Broadband' THEN 'MBB' WHEN  ProductName like '%Product Broadband - 12Months%' THEN 'MBB-12 Months' WHEN  ProductName like '%Tourist%' THEN 'Tourist' WHEN ProductName like '%6Months%' THEN '6 Months' 
WHEN  ProductName like '%12Month%' THEN '12 Months' WHEN  ProductName like '%24Month%' THEN '12 Months'
WHEN  ProductName like '%Product_Staff%' THEN 'Monthly' WHEN  ProductName like '%Product B v%' THEN 'Monthly' WHEN  ProductName like '%Staff%' THEN 'Staff'  ELSE ProductName END AS ProductGroup,
CASE WHEN  ProductName like '%12%Months%' THEN 'Annual' WHEN ProductName like '%6 Months%' THEN 'Annual' ELSE 'Monthly' END AS ProductCategory
FROM(
SELECT ProductCode , CASE WHEN ProductCode IN (168,193) THEN 'Monthly_Staff' WHEN Product_Name IS NULL THEN 'Null' ELSE Product_Name END AS ProductName
FROM ( Select src_product_code AS ProductCode,MAX(business_product_name) AS Product_Name FROM uae_db_prod.ae_prod.dim_product GROUP BY 1 ) A
)b
),
po_sre_customer AS
(
SELECT *, ROW_NUMBER() OVER (PARTITION BY NobillAccountNo ORDER BY Renewal_cycle DESC,LastUpdateDate DESC,subscribeDate DESC) AS IN_RANK FROM (
SELECT *,REGEXP_COUNT(NobillAccountNo, '^[0-9.-]+$') AS Flag_In_Available , CASE WHEN RenewalDay IS NULL THEN 0 ELSE RenewalDay END AS Renewal_cycle
FROM uae_db_prod.ae_stg.po_sre_customer 
)a
),
Payment_Action_history AS (
SELECT ActionHistoryId,SUM(PaidAmount) AS PaidAmount , SUM(CASE WHEN PaymentMethodId=13 THEN PaidAmount ELSE 0 END) AS Cashback_PaidAmount ,
SUM(CASE WHEN PaymentMethodId=15 THEN PaidAmount ELSE 0 END) AS Loyalty_PaidAmount ,  SUM(CASE WHEN PaymentMethodId=16 THEN PaidAmount ELSE 0 END) AS Promotion_PaidAmount
FROM (
SELECT ActionHistoryId,PaymentMethodId,PaymentMethodName,SUM(PaidAmount) AS PaidAmount
FROM
(
SELECT ph.*,PaymentMethodName FROM uae_db_prod.ae_stg.po_sre_paymenthistory ph
LEFT JOIN (SELECT cast(Id As int) AS LP_ID , Name AS PaymentMethodName FROM uae_db_prod.ae_stg.po_po_lookup WHERE LookupTypeId=5 ORDER By cast(Id As int) asc) lp on lp.LP_ID=ph.PaymentMethodId
WHERE IsSuccess=true AND CAST(DateTime AS DATE)>=ADD_MONTHS('"+context.vLoadDate+"',-13) AND CAST(DateTime AS DATE)<'"+context.vLoadDate+"' AND ActionHistoryId is not Null --('"+context.vLoadDate+"' - 365)
)a
GROUP BY 1,2,3
)a GROUP BY 1
),
PLAN_ACTION_HISTORY AS (
SELECT Fifth.* ,  
CASE WHEN Fifth.FLAG_PAYG_PLAN='PAYG' OR TotalCost IS NULL THEN 0 WHEN Fifth.ProductCode=4 THEN TotalCost30P  WHEN  ProductCategory='Annual'  THEN TotalCost50P  ELSE TotalCost END AS Actual_TotalCost ,
TotalCost
FROM (
SELECT * , NATL_SERVICE || '-' || INTL_SERVICE || '-' || DATA_SERVICE AS PLAN_DESCRIPTION ,
CASE WHEN ProductCode IN ('190','194') THEN DATA_SERVICE_ID ELSE 
NATL_SERVICE_ID || ',' || DATA_SERVICE_ID || ',' || INTL_SERVICE_ID END AS COMBO_ID
FROM (
SELECT * , 
CASE WHEN  SERVICE_CATEGORY_01 LIKE '%NATL%' THEN SERVICE_NAME_01 WHEN SERVICE_CATEGORY_02 LIKE '%NATL%' THEN SERVICE_NAME_02 WHEN SERVICE_CATEGORY_03 LIKE '%NATL%' THEN SERVICE_NAME_03 ELSE 'NO_NATL' END AS NATL_SERVICE,
CASE WHEN  SERVICE_CATEGORY_01 LIKE '%DATA%' THEN SERVICE_NAME_01 WHEN SERVICE_CATEGORY_02 LIKE '%DATA%' THEN SERVICE_NAME_02 WHEN SERVICE_CATEGORY_03 LIKE '%DATA%' THEN SERVICE_NAME_03 ELSE 'NO_DATA' END AS DATA_SERVICE,
CASE WHEN  SERVICE_CATEGORY_01 LIKE '%INTL%' THEN SERVICE_NAME_01 WHEN SERVICE_CATEGORY_02 LIKE '%INTL%' THEN SERVICE_NAME_02 WHEN SERVICE_CATEGORY_03 LIKE '%INTL%' THEN SERVICE_NAME_03 ELSE 'NO_INTL' END AS INTL_SERVICE,
CASE WHEN  SERVICE_CATEGORY_01 LIKE '%NATL%' THEN SERVICE_01 WHEN SERVICE_CATEGORY_02 LIKE '%NATL%' THEN SERVICE_02 WHEN SERVICE_CATEGORY_03 LIKE '%NATL%' THEN SERVICE_03 ELSE '0' END AS NATL_SERVICE_ID,
CASE WHEN  SERVICE_CATEGORY_01 LIKE '%DATA%' THEN SERVICE_01 WHEN SERVICE_CATEGORY_02 LIKE '%DATA%' THEN SERVICE_02 WHEN SERVICE_CATEGORY_03 LIKE '%DATA%' THEN SERVICE_03 ELSE '0' END AS DATA_SERVICE_ID,
CASE WHEN  SERVICE_CATEGORY_01 LIKE '%INTL%' THEN SERVICE_01 WHEN SERVICE_CATEGORY_02 LIKE '%INTL%' THEN SERVICE_02 WHEN SERVICE_CATEGORY_03 LIKE '%INTL%' THEN SERVICE_03 ELSE '17' END AS INTL_SERVICE_ID
FROM(
SELECT
Second.* ,
bund1.EVENT_IN_EVENT_ID AS SERVICE_EVENT_01,
bund2.EVENT_IN_EVENT_ID AS SERVICE_EVENT_02,
bund3.EVENT_IN_EVENT_ID AS SERVICE_EVENT_03, 
bund1.EVENT_NAME AS SERVICE_NAME_01 ,
bund2.EVENT_NAME AS SERVICE_NAME_02 ,
bund3.EVENT_NAME AS SERVICE_NAME_03 , 
bund1.EVENT_CATEGORY AS SERVICE_CATEGORY_01 , 
bund2.EVENT_CATEGORY AS SERVICE_CATEGORY_02 , 
bund3.EVENT_CATEGORY AS SERVICE_CATEGORY_03,
CASE WHEN FLAG_PAYG_PLAN='PLAN' AND Plan_Rank=1 AND FLAG_ACTIVE_PLAN=1 THEN 1 WHEN  FLAG_PAYG_PLAN='PAYG' AND Plan_Rank=1 AND FLAG_ACTIVE_PLAN=1 THEN 2 ELSE 0 END AS PLAN_STATUS
FROM(
SELECT 
First.* ,
CASE WHEN Subs_EndDate >='"+context.vLoadDate+"'  THEN 1 ELSE 0 END AS FLAG_ACTIVE_PLAN,
CASE WHEN Subs_StartDate<'"+context.vLoadDate+"' AND Subs_StartDate>='"+context.vLoadDate+"'-15 THEN 1 
WHEN Subs_StartDate<'"+context.vLoadDate+"'-15 AND Subs_StartDate>='"+context.vLoadDate+"'-30 THEN 2
WHEN Subs_StartDate<'"+context.vLoadDate+"'-30 AND Subs_StartDate>='"+context.vLoadDate+"'-60 THEN 3
WHEN Subs_StartDate<'"+context.vLoadDate+"'-60 AND Subs_StartDate>='"+context.vLoadDate+"'-90 THEN 4 ELSE 5 END AS PLAN_TYPE_RANK_PRIOR,
CASE WHEN ProductName='PAYGProduct' THEN 'PAYG' ELSE 'PLAN' END AS FLAG_PAYG_PLAN, 
CASE WHEN Action_Date >=ADD_MONTHS('"+context.vLoadDate+"',-6) AND Action_Date < '"+context.vLoadDate+"' THEN 1 ELSE 0 END AS FLAG_LAST_6M,
(SPLIT_PART(Action_ServiceIds, ',',1)) AS SERVICE_01,(SPLIT_PART(Action_ServiceIds, ',',2)) AS SERVICE_02,(SPLIT_PART(Action_ServiceIds, ',',3)) AS SERVICE_03,
ROW_NUMBER() OVER ( PARTITION BY NobillAccountNo ORDER BY AH_Id DESC) AS Plan_Rank 
FROM (
SELECT
ah.Id AS AH_Id, ah.ActionId As ActionId,aname.Name As ActionName , ah.DateTime AS Action_Timestamp , CAST(ah.DateTime  AS DATE) AS Action_Date,
ah.CustomerId , NobillAccountNo ,MSISDN , CAST(cust.SubscribeDate AS DATE) AS SubscribeDate, ah.ServiceIds AS Action_ServiceIds , ah.LastUpdateDate AS Action_UpdateTimestamp, ah.ProductCode As ProductCode, 
PaidAmount , Cashback_PaidAmount, Loyalty_PaidAmount , Promotion_PaidAmount ,  
CASE WHEN ProductName IS NULL THEN 'Null' ELSE ProductName END AS ProductName ,CASE WHEN ProductGroup IS NULL THEN 'Null' ELSE ProductGroup END AS ProductGroup,
CASE WHEN ProductCategory IS NULL THEN 'Null' ELSE ProductCategory END AS ProductCategory,
ah.SubscriptionId Action_SubsId, sub.StatusId AS SubsriptionStatus , CAST(StartDate AS Date) AS Subs_StartDate, CAST(EndDate AS Date) AS Subs_EndDate , 
ROW_NUMBER() OVER ( PARTITION BY NobillAccountNo  ORDER BY ah.Id DESC) AS Plan_Init_Rank,
ROW_NUMBER() OVER ( PARTITION BY NobillAccountNo ,CAST(ah.DateTime  AS DATE) ORDER BY ah.Id DESC) AS Plan_Day_Rank
FROM uae_db_prod.ae_stg.po_sre_actionhistory ah
LEFT JOIN uae_db_prod.ae_stg.po_sre_action aname on aname.ID=ah.ActionId
LEFT JOIN CUS_PRODUCT pc on pc.ProductCode=ah.ProductCode
LEFT JOIN po_sre_customer cust on cust.Id=ah.CustomerId
LEFT JOIN uae_db_prod.ae_stg.po_sre_subscription sub on sub.Id=ah.SubscriptionId
LEFT JOIN Payment_Action_history phm on phm.ActionHistoryId=ah.Id
WHERE  CAST(ah.DateTime AS DATE)>= ADD_MONTHS('"+context.vLoadDate+"',-13) AND CAST(ah.DateTime AS DATE)<'"+context.vLoadDate+"'  AND ah.ActionId in (1,3,4,13,14)  AND ah.StatusId=2  AND Flag_In_Available=1
)First WHERE Plan_Day_Rank=1
)Second  
LEFT JOIN view_plan_services bund1 on bund1.PO_EXCEL_SERVICE_ID::Varchar=Second.SERVICE_01::Varchar
LEFT JOIN view_plan_services bund2 on bund2.PO_EXCEL_SERVICE_ID::Varchar=Second.SERVICE_02::Varchar
LEFT JOIN view_plan_services bund3 on bund3.PO_EXCEL_SERVICE_ID::Varchar=Second.SERVICE_03::Varchar
)Third
)Fourth
)Fifth LEFT JOIN package_price pack on pack.PackagePriceServices=CAST(Fifth.COMBO_ID AS Varchar) AND pack.ProductCode=Fifth.ProductCode
),
MOTHLY_6M AS
(
SELECT NobillAccountNo, 1 AS CONSECUTIVE_6MONTHS_ON_MONTHLY , CONSECUTIVE_6MONTHS_ON_MONTHLY_AMOUNT_PAID FROM
(
SELECT NobillAccountNo,SUM(FLAG_MONTHLY_6M) AS FLAG_MONTHLY_6M , SUM(PAID_AMOUNT_MONTHLY_6M) AS CONSECUTIVE_6MONTHS_ON_MONTHLY_AMOUNT_PAID FROM
(
SELECT NobillAccountNo,TO_CHAR(Action_Date,'YYYYMM') AS LAST_6M_YYYYMM,MAX(1) AS FLAG_MONTHLY_6M, SUM(PaidAmount) AS PAID_AMOUNT_MONTHLY_6M FROM PLAN_ACTION_HISTORY WHERE FLAG_LAST_6M=1 AND ProductCode IN (1,2) GROUP BY 1,2
)first GROUP BY 1
)Second where FLAG_MONTHLY_6M>=6
),
DAILY_CHANGE_FLAG AS (
SELECT NobillAccountNo,FIRST_PLAN_RANK,
CASE WHEN FIRST_FLAG_PAYG_PLAN='PAYG' THEN 2 WHEN (FLAG_NATL_CHANGE=1 OR FLAG_DATA_CHANGE=1 OR FLAG_INTL_CHANGE=1) THEN 1 ELSE 0 END AS FLAG_PLAN_CHANGE
FROM (
SELECT first.NobillAccountNo AS NobillAccountNo ,
first.Plan_Rank AS FIRST_PLAN_RANK, second.Plan_Rank AS SECOND_PLAN_RANK,
first.FLAG_PAYG_PLAN AS FIRST_FLAG_PAYG_PLAN,
first.NATL_SERVICE AS FIRST_NATL_SERVICE , second.NATL_SERVICE AS SECOND_NATL_SERVICE,
first.DATA_SERVICE AS FIRST_DATA_SERVICE , second.DATA_SERVICE AS SECOND_DATA_SERVICE,
first.INTL_SERVICE AS FIRST_INTL_SERVICE , second.INTL_SERVICE AS SECOND_INTL_SERVICE,
CASE WHEN first.NATL_SERVICE<>second.NATL_SERVICE THEN 1 ELSE 0 END AS FLAG_NATL_CHANGE,
CASE WHEN first.DATA_SERVICE<>second.DATA_SERVICE THEN 1 ELSE 0 END AS FLAG_DATA_CHANGE,
CASE WHEN first.INTL_SERVICE<>second.INTL_SERVICE THEN 1 ELSE 0 END AS FLAG_INTL_CHANGE
FROM PLAN_ACTION_HISTORY first
INNER JOIN (SELECT NobillAccountNo,NATL_SERVICE,DATA_SERVICE,INTL_SERVICE,Plan_Rank FROM PLAN_ACTION_HISTORY WHERE Plan_Rank=2) second on first.NobillAccountNo=second.NobillAccountNo
WHERE first.Plan_Rank=1 AND first.Subs_StartDate = '"+context.vLoadDate+"' - 1 
)b1
),
MIN_PLAN_TABLE AS 
(
SELECT NobillAccountNo, CASE WHEN PLAN_MIN_RANK>=1 THEN PLAN_MIN_RANK WHEN PAYG_MIN_RANK>=1 THEN PAYG_MIN_RANK ELSE 0 END AS PLAN_PAYG_MIN_RANK , 
CASE WHEN PLAN_MIN_RANK>=1 THEN PLAN_MIN_RANK ELSE 0 END AS PLAN_MIN_RANK , CASE WHEN PAYG_MIN_RANK>=1 THEN PAYG_MIN_RANK ELSE 0 END AS PAYG_MIN_RANK 
FROM ( 
SELECT NobillAccountNo,MIN(CASE WHEN FLAG_PAYG_PLAN='PLAN' THEN Plan_Rank  END) AS PLAN_MIN_RANK , MIN(CASE WHEN FLAG_PAYG_PLAN='PAYG' THEN Plan_Rank END) AS PAYG_MIN_RANK
FROM PLAN_ACTION_HISTORY 
GROUP BY NobillAccountNo
)first
)
,
LAST_PLAN_TABLE AS 
(
SELECT 
PH.NobillAccountNo , ProductName AS LAST_PLAN_TYPE , PLAN_DESCRIPTION AS LAST_PLAN_DESCRIPTION,Subs_EndDate AS LAST_RENEWAL_DATE ,PLAN_PAYG_MIN_RANK
FROM PLAN_ACTION_HISTORY PH
Inner join MIN_PLAN_TABLE MP on MP.NobillAccountNo=PH.NobillAccountNo AND MP.PLAN_PAYG_MIN_RANK=PH.Plan_Rank
),
PLAN_TYPE_PRIOR AS
(
SELECT NobillAccountNo, 
MAX(CASE WHEN PLAN_TYPE_RANK_PRIOR=2 THEN ProductName END) AS PLAN_TYPE_15DAYS_PRIOR,
MAX(CASE WHEN PLAN_TYPE_RANK_PRIOR=3 THEN ProductName END) AS PLAN_TYPE_30DAYS_PRIOR,
MAX(CASE WHEN PLAN_TYPE_RANK_PRIOR=4 THEN ProductName END) AS PLAN_TYPE_60DAYS_PRIOR
FROM(
SELECT a.NobillAccountNo,a.ProductName,a.Plan_Rank,a.PLAN_TYPE_RANK_PRIOR,PLAN_RANK_MAX_PRIOR FROM PLAN_ACTION_HISTORY a
INNER JOIN (SELECT NobillAccountNo,PLAN_TYPE_RANK_PRIOR,MIN(Plan_Rank) AS PLAN_RANK_MAX_PRIOR FROM PLAN_ACTION_HISTORY PH GROUP BY 1,2)b
On a.NobillAccountNo=b.NobillAccountNo AND a.Plan_Rank=b.PLAN_RANK_MAX_PRIOR
)last GROUP BY NobillAccountNo
)
,
LAST_PLAN_DETAILS AS (
SELECT *,ADD_MONTHS(Subs_EndDate,CAST(Remaining_Renewal_Month AS INT)) AS ANNUAL_PLANS_RENEWAL_DATE,Subs_EndDate AS RENEWAL_PAYMENT_DATE
FROM
(
SELECT final_first.*,CASE WHEN Annual_Counter_Balance<=0 OR Actual_TotalCost<=0 THEN 0 ELSE ROUND(Annual_Counter_Balance/Actual_TotalCost) END AS Remaining_Renewal_Month
FROM 
(
SELECT PAH.*,PAH.ProductName AS LATEST_PLAN_TYPE , PAH.PLAN_DESCRIPTION AS LATEST_PLAN_DESCRIPTION,
CASE WHEN CONSECUTIVE_6MONTHS_ON_MONTHLY=1 THEN 1 ELSE 0 END AS CONSECUTIVE_6MONTHS_ON_MONTHLY,CONSECUTIVE_6MONTHS_ON_MONTHLY_AMOUNT_PAID ,
CASE WHEN FLAG_PLAN_CHANGE>=1 THEN FLAG_PLAN_CHANGE ELSE 0 END AS FLAG_PLAN_CHANGE ,
NATL_SERVICE AS PLAN_CHANGE_NATL,
DATA_SERVICE AS PLAN_CHANGE_DATA,
INTL_SERVICE AS PLAN_CHANGE_INTL,
CASE WHEN FIRST_SUBSCRIPTION_DATE IS NULL THEN SubscribeDate ELSE FIRST_SUBSCRIPTION_DATE END AS FIRST_SUBSCRIPTION_DATE,
CASE WHEN Annual_Counter_Balance>0 THEN Annual_Counter_Balance ELSE 0 END AS Annual_Counter_Balance,
PLAN_TYPE_15DAYS_PRIOR , PLAN_TYPE_30DAYS_PRIOR , PLAN_TYPE_60DAYS_PRIOR,
LAST_PLAN_TYPE,LAST_PLAN_DESCRIPTION ,LAST_RENEWAL_DATE, PLAN_PAYG_MIN_RANK  
FROM PLAN_ACTION_HISTORY PAH 
LEFT JOIN MOTHLY_6M M6 ON M6.NobillAccountNo=PAH.NobillAccountNo AND PAH.Plan_Rank=1
LEFT JOIN DAILY_CHANGE_FLAG DC ON DC.NobillAccountNo=PAH.NobillAccountNo AND DC.FIRST_PLAN_RANK=PAH.Plan_Rank
LEFT JOIN LAST_PLAN_TABLE LPT ON LPT.NobillAccountNo=PAH.NobillAccountNo AND PAH.Plan_Rank=1
LEFT JOIN PLAN_TYPE_PRIOR PTP ON PTP.NobillAccountNo=PAH.NobillAccountNo  AND PAH.Plan_Rank=1
LEFT JOIN ANNUAL_RUNNING_BALANCE ann ON ann.NobillAccountNo=PAH.NobillAccountNo AND PAH.Plan_Rank=1
LEFT JOIN (SELECT  CustomerId,CAST(Min(StartDate) AS DATE) AS FIRST_SUBSCRIPTION_DATE FROM uae_db_prod.ae_stg.po_sre_subscription GROUP BY CustomerId) cusu ON cusu.CustomerId=PAH.CustomerId AND PAH.Plan_Rank=1
)final_first 
WHERE Plan_Rank=1
)Un_Cust
)
SELECT IN_ACCOUNT_NUMBER,
PLAN_STATUS,
LAST_PLAN_TYPE,
LAST_PLAN_DESCRIPTION,
RENEWAL_PAYMENT_DATE,
'' AS PREVIOUS_PLAN_TYPE,
ANNUAL_PLANS_RENEWAL_DATE,
'' AS PACKAGE_CHANGE_STATUS,
'' AS PAYGPLAN,
'' AS SCHEDULED_UPDATE_PLAN_DATE,
CONSECUTIVE_6MONTHS_ON_MONTHLY,
CONSECUTIVE_6MONTHS_ON_MONTHLY_AMOUNT_PAID,
LAST_RENEWAL_DATE,
PLAN_TYPE_15DAYS_PRIOR,
PLAN_TYPE_30DAYS_PRIOR,
PLAN_TYPE_60DAYS_PRIOR,
FLAG_PLAN_CHANGE,
PLAN_CHANGE_NATL,
PLAN_CHANGE_DATA,
PLAN_CHANGE_INTL,
LATEST_PLAN_TYPE,
LATEST_PLAN_DESCRIPTION,
TENURE_MONTH
FROM(
SELECT *,CEIL(('"+context.vLoadDate+"' - FINAL_FIRST_PLAN_DATE)/30) AS TENURE_MONTH FROM (
SELECT LPD.*,psc.NobillAccountNo AS IN_ACCOUNT_NUMBER,CUST_TYPE,FULL_ACTIVATION_DATE,FIRST_PLAN_DATE , cm.MSISDN AS QLIK_MSISDN , psc.MSISDN AS LP_MSISDN ,
CASE WHEN CAST(FULL_ACTIVATION_DATE AS DATE) IS NULL THEN CAST(FIRST_PLAN_DATE AS DATE) ELSE CAST(FULL_ACTIVATION_DATE AS DATE) END AS FINAL_FIRST_PLAN_DATE
FROM po_sre_customer psc
LEFT JOIN LAST_PLAN_DETAILS LPD ON LPD.NobillAccountNo=psc.NobillAccountNo
LEFT JOIN (SELECT CustomerId,CAST(MIN(DateTime) AS DATE) AS FIRST_PLAN_DATE FROM uae_db_prod.ae_stg.po_sre_actionhistory WHERE ActionId in (1,3,4,13,14)  AND StatusId=2 AND SubscriptionId IS NOT NULL GROUP BY 1)cus_ah ON cus_ah.CustomerId=psc.Id
LEFT JOIN uae_db_prod.ae_prod.customer_master cm on cm.IN_ACCOUNT_NUMBER=psc.NobillAccountNo
WHERE Flag_In_Available=1 AND IN_RANK=1
)Load_Last
)final;
