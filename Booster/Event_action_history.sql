WITH SERVICE_MASTER AS
(SELECT Id  AS ServiceId,Name AS ServiceName,EventId,CounterId,ServiceChargingTypeId,ServiceChargingTypeName,ServiceTypeId,ServiceTypeName,ServiceCategoryId,ServiceCategoryName,ServiceCost,StatusId as ServiceStatusId,
dev_event_desc,dev_category_1,dev_category_2,dev_category_3,dev_category_4 , CreationDate AS ServiceCreationDate,
CASE WHEN dev_category_1='VAS' THEN 'VAS' WHEN dev_category_3 ilike '%Booster%' THEN 'Booster' WHEN  dev_category_3 ilike '%Roaming%' THEN 'Roaming' ELSE 'Others' END AS ServiceGroup
FROM(
SELECT CustomerServiceName,CounterId,EventId,sc.Name as ServiceCategoryName, st.Name as ServiceTypeName,sct.Name as ServiceChargingTypeName ,dev_event_desc,dev_category_1,dev_category_2,dev_category_3,dev_category_4,sa.* 
FROM uae_db_prod.ae_stg.po_po_service sa
LEFT JOIN uae_db_prod.ae_stg.po_po_servicecategory sc on sa.ServiceCategoryId=sc.Id
LEFT JOIN uae_db_prod.ae_stg.po_po_servicetype st on sa.ServiceTypeId=st.Id
LEFT JOIN uae_db_prod.ae_stg.po_po_servicechargingtype sct on sct.Id=sa.ServiceChargingTypeId
LEFT JOIN uae_db_prod.ae_stg.po_po_bundle bun on bun.ServiceId=sa.Id
LEFT JOIN (SELECT * FROM (SELECT *,ROW_NUMBER()OVER(PARTITION BY dev_eventid_base ORDER BY dev_event_id) row_num FROM ae_prod.nobill_dim_event EVENT_DIM)a WHERE row_num=1)EVENT_DIM ON  bun.EventId = EVENT_DIM.dev_eventid_base
)FINAL
),
Payment_Action_history AS (
SELECT ActionHistoryId,SUM(PaidAmount) AS PaidAmount
FROM (
SELECT ActionHistoryId,PaymentMethodId,SUM(PaidAmount) AS PaidAmount
FROM
(
SELECT ph.* FROM uae_db_prod.ae_stg.po_sre_paymenthistory ph
--LEFT JOIN uae_db_prod.ae_stg.payment_paymentmethods pm on pm.id=ph.PaymentMethodId
WHERE IsSuccess=true AND CAST(DateTime AS DATE)>=date('"+context.vLoadDate+"')-120 AND CAST(DateTime AS DATE)<date('"+context.vLoadDate+"') AND ActionHistoryId is not Null --And PaymentMethod Not Ilike '%REFUND%'
AND PaidAmount>0
)a
GROUP BY 1,2
)a GROUP BY 1
),
EVENT_ACTION_HISTORY AS (
SELECT * ,
CASE WHEN VAS_MIN_ACTIVE_RENEWAL_DATE is null then VAS_MAX_RENEWAL_DATE ELSE VAS_MIN_ACTIVE_RENEWAL_DATE END AS VAS_RENEWAL_DATE
FROM(
 SELECT  IN_ACCOUNT_NUMBER,
    CASE WHEN MAX(CASE WHEN FLAG_VAS=1 THEN EndDate END)>=date('"+context.vLoadDate+"') THEN 1 ELSE 0 END AS VAS_SERVICE_FLAG,
    MIN(CASE WHEN FLAG_VAS=1 AND FLAG_ACTIVE=1 THEN EndDate END)  AS VAS_MIN_ACTIVE_RENEWAL_DATE,
    MAX(CASE WHEN FLAG_VAS=1 THEN EndDate END)  AS VAS_MAX_RENEWAL_DATE,
    SUM(CASE WHEN FLAG_VAS=1 AND FLAG_30D=1 THEN PaidAmount ELSE 0 END) AS VAS_TOTAL_AMOUNT,
 CASE WHEN MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOIP' THEN EndDate END)>=date('"+context.vLoadDate+"') THEN 1 ELSE 0 END AS VOIP_SERVICE,
 CASE WHEN MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOIP' THEN EndDate END)>=date('"+context.vLoadDate+"') THEN 1 ELSE 0 END AS BOTIM_SERVICE_FLAG,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOIP' THEN StartDate END) AS VOIP_LAST_SUBS_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOIP' THEN EndDate END) AS VOIP_RENEWAL_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOIP' THEN EndDate END) AS BOTIM_RENEWAL_DATE,
 CASE WHEN MAX(CASE WHEN EXCEL_SERVICE_TYPE='Netflix' THEN EndDate END)>=date('"+context.vLoadDate+"') THEN 1 ELSE 0 END AS NETFLIX_SERVICE_FLAG,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='Netflix' THEN StartDate END) AS NETFLIX_LAST_SUBS_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='Netflix' THEN EndDate END) AS NETFLIX_RENEWAL_DATE,
 CASE WHEN MAX(CASE WHEN EXCEL_SERVICE_TYPE='Anghami' THEN EndDate END)>=date('"+context.vLoadDate+"') THEN 1 ELSE 0 END AS ANGHAMI_SERVICE_FLAG,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='Anghami' THEN StartDate END) AS ANGHAMI_LAST_SUBS_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='Anghami' THEN EndDate END) AS ANGHAMI_RENEWAL_DATE,
 CASE WHEN MAX(CASE WHEN EXCEL_SERVICE_TYPE='AppleWatch' THEN EndDate END)>=date('"+context.vLoadDate+"') THEN 1 ELSE 0 END AS APPLE_WATCH_SERVICE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='AppleWatch' THEN StartDate END) AS APPLE_WATCH_LAST_SUBS_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='AppleWatch' THEN EndDate END) AS APPLE_WATCH_RENEWAL_DATE,
 CASE WHEN MAX(CASE WHEN EXCEL_SERVICE_TYPE='Entertainer' THEN EndDate END)>=date('"+context.vLoadDate+"') THEN 1 ELSE 0 END AS ENTERTAINER_SERVICE_FLAG,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='Entertainer' THEN StartDate END) AS ENTERTAINER_LAST_SUBSL_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='Entertainer' THEN EndDate END) AS ENTERTAINER_RENEWAL_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='DATA_ROM' THEN 1 ELSE 0 END) AS ROAMING_DATA_FLAG,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='DATA_ROM' THEN EXCEL_SERVICE_DESCRIPTION END) AS LAST_DATA_ROAMING_BUNDLE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='DATA_ROM' THEN StartDate END) AS LAST_DATA_ROAMING_BUNDLE_PURCHASE_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='DATA_ROM' THEN EndDate END) AS ROAMING_DATA_END_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOICE_OUT_ROM' THEN 1 ELSE 0 END) AS ROAMING_VOICE_FLAG,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOICE_OUT_ROM' THEN EXCEL_SERVICE_DESCRIPTION END) AS LAST_VOICE_ROAMING_BUNDLE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOICE_OUT_ROM' THEN StartDate END) AS LAST_VOICE_ROAMING_BUNDLE_PURCHASE_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOICE_OUT_ROM' THEN EndDate END) AS ROAMING_VOICE_END_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='DATA_PASS_ROM' THEN 1 ELSE 0 END) AS ROAMING_DATA_PASS_FLAG,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='DATA_PASS_ROM' THEN EXCEL_SERVICE_DESCRIPTION END) AS LAST_DATA_ROAMING_PASS,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='DATA_PASS_ROM' THEN StartDate END) AS LAST_DATA_ROAMING_PASS_PURCHASE_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='DATA_PASS_ROM' THEN EndDate END) AS ROAMING_DATA_PASS_END_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOICE_PASS_ROM' THEN 1 ELSE 0 END) AS ROAMING_VOICE_PASS_FLAG,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOICE_PASS_ROM' THEN EXCEL_SERVICE_DESCRIPTION END) AS LAST_VOICE_ROAMING_PASS,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOICE_PASS_ROM' THEN StartDate END) AS LAST_VOICE_ROAMING_PASS_PURCHASE_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_TYPE='VOICE_PASS_ROM' THEN EndDate END) AS ROAMING_VOICE_PASS_END_DATE,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' THEN 1 ELSE 0 END) AS BOOSTER_FLAG_INLIFE,
 date('"+context.vLoadDate+"') - MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' THEN StartDate END) AS DAYS_SINCE_LAST_BOOSTER_PURCHASE,
 date('"+context.vLoadDate+"') - MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' THEN EndDate END) AS DAYS_SINCE_BOOSTER_EXPIRY,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' THEN EndDate END) - date('"+context.vLoadDate+"')  AS DAYS_TILL_BOOSTER_EXPIRY,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='DATA' THEN 1 ELSE 0 END) AS BOOSTER_DATA_FLAG_INLIFE,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='DATA' THEN EXCEL_SERVICE_DESCRIPTION END) AS BOOSTER_LAST_DATA,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='DATA' THEN StartDate END) AS BOOSTER_LAST_DATE_DATA,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='DATA' THEN EndDate END) AS BOOSTER_DATA_EXPIRATION_DATE,
 date('"+context.vLoadDate+"') - MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='DATA' THEN EndDate END)    AS DAYS_SINCE_DATA_BOOSTER_EXPIRY,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='DATA' THEN EndDate END) - date('"+context.vLoadDate+"') AS DAYS_TILL_DATA_BOOSTER_EXPIRY,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN 1 ELSE 0 END) AS BOOSTER_VOICE_LCL_FLAG_INLIFE,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EXCEL_SERVICE_DESCRIPTION END) AS BOOSTER_LAST_NATL,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN StartDate END) AS BOOSTER_LAST_DATE_NATL,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EndDate END) AS BOOSTER_NATL_EXPIRATION_DATE,
 date('"+context.vLoadDate+"') - MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EndDate END) AS DAYS_SINCE_NATL_BOOSTER_EXPIRY,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_LCL' THEN EndDate END) - date('"+context.vLoadDate+"') AS DAYS_TILL_NATL_BOOSTER_EXPIRY,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN 1 ELSE 0 END) AS BOOSTER_VOICE_INT_FLAG_INLIFE,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EXCEL_SERVICE_DESCRIPTION END) AS BOOSTER_LAST_INTL,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN StartDate END) AS BOOSTER_LAST_DATE_INTL,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EndDate END) AS BOOSTER_INTL_EXPIRATION_DATE,
 date('"+context.vLoadDate+"') - MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EndDate END)  AS DAYS_SINCE_INTL_BOOSTER_EXPIRY,
 MAX(CASE WHEN EXCEL_SERVICE_CATEGORY='Booster' AND EXCEL_SERVICE_TYPE='VOICE_INT' THEN EndDate END) - date('"+context.vLoadDate+"')  AS DAYS_TILL_INTL_BOOSTER_EXPIRY
  FROM(
  SELECT FT.* , sf.*  , ana.Name AS ActionName,sub.StatusId AS SubscriptionStatus,CAST(StartDate AS DATE) AS StartDate ,CAST(EndDate AS DATE) AS EndDate ,  
  CASE WHEN EXCEL_SERVICE_TYPE in ('VOIP','Netflix','Anghami','AppleWatch','Entertainer') THEN 1 ELSE 0 END AS FLAG_VAS,
  CASE WHEN CAST(EndDate AS DATE)>=date('"+context.vLoadDate+"') THEN 1 ELSE 0 END AS FLAG_ACTIVE
  FROM
 (
 SELECT ah.Id , ah.ActionId , ah.DateTime , ah.CustomerId, NobillAccountNo AS IN_ACCOUNT_NUMBER , ah.ServiceIds , EventId , ah.SubscriptionId , ah.LastUpdateDate,
 ah.ProductCode, ServiceGroup, dev_category_3,
 CASE WHEN ah.DateTime>=date('"+context.vLoadDate+"')-30 THEN 1 ELSE 0 END AS FLAG_30D,PaidAmount,
 ROW_NUMBER() OVER ( PARTITION BY NobillAccountNo,ServiceGroup,dev_category_3 ORDER BY ah.Id DESC ) AS GROUP_RANK
 , ROW_NUMBER() OVER ( PARTITION BY NobillAccountNo,ServiceGroup,dev_category_3 ORDER BY ah.Id asc ) AS TOTAL_COUNT
 FROM uae_db_prod.ae_stg.po_sre_actionhistory ah
 LEFT JOIN SERVICE_MASTER sm on sm.ServiceId::int=SPLIT_PART(ah.ServiceIds, ',',1)::int
 LEFT JOIN uae_db_prod.ae_stg.po_sre_customer cust on cust.Id=ah.CustomerId
 LEFT JOIN Payment_Action_history phm on phm.ActionHistoryId=ah.Id
 WHERE  CAST(DateTime AS DATE)>='2018-01-01' and CAST(DateTime AS DATE)<date('"+context.vLoadDate+"') 
 AND CHAR_LENGTH(ServiceIds)>1 AND ah.StatusId=2 AND ServiceIds not ilike '%,%,%' AND ActionId not in ('30') AND ServiceGroup IN ('VAS','Booster','Roaming') 
 ) FT
 LEFT JOIN uae_db_prod.ae_stg.po_service_file sf on sf.EXCEL_SERVICE_EVENT_ID=FT.EventId
 LEFT JOIN uae_db_prod.ae_stg.po_sre_action ana  on ana.Id=FT.ActionId
 LEFT JOIN uae_db_prod.ae_stg.po_sre_subscription sub on sub.Id=FT.SubscriptionId
 WHERE GROUP_RANK=1 AND EventId<>'109'
)SE WHERE LENGTH(IN_ACCOUNT_NUMBER)>1 AND NOT regexp_like(IN_ACCOUNT_NUMBER, '_')
GROUP BY 1
)FI
)
SELECT 
IN_ACCOUNT_NUMBER,
BOOSTER_LAST_DATA,
BOOSTER_LAST_NATL,
BOOSTER_LAST_INTL,
BOOSTER_LAST_DATE_NATL,
BOOSTER_LAST_DATE_DATA,
BOOSTER_LAST_DATE_INTL,
DAYS_SINCE_LAST_BOOSTER_PURCHASE,
DAYS_TILL_BOOSTER_EXPIRY,
DAYS_SINCE_BOOSTER_EXPIRY,
DAYS_TILL_DATA_BOOSTER_EXPIRY,
DAYS_SINCE_DATA_BOOSTER_EXPIRY,
DAYS_TILL_NATL_BOOSTER_EXPIRY,
DAYS_SINCE_NATL_BOOSTER_EXPIRY,
DAYS_TILL_INTL_BOOSTER_EXPIRY,
DAYS_SINCE_INTL_BOOSTER_EXPIRY,
VOIP_SERVICE,
NETFLIX_SERVICE_FLAG,
VAS_SERVICE_FLAG,
ANGHAMI_SERVICE_FLAG,
ENTERTAINER_SERVICE_FLAG,
BOTIM_SERVICE_FLAG,
VOIP_RENEWAL_DATE,
APPLE_WATCH_SERVICE,
APPLE_WATCH_RENEWAL_DATE,
ANGHAMI_RENEWAL_DATE,
NETFLIX_RENEWAL_DATE,
ENTERTAINER_RENEWAL_DATE,
BOTIM_RENEWAL_DATE,
VAS_RENEWAL_DATE,
VAS_TOTAL_AMOUNT,
0 AS VAS_USAGE_RIGHT_FLAG,
LAST_DATA_ROAMING_BUNDLE,
LAST_DATA_ROAMING_BUNDLE_PURCHASE_DATE,
LAST_DATA_ROAMING_PASS,
LAST_DATA_ROAMING_PASS_PURCHASE_DATE,
LAST_VOICE_ROAMING_BUNDLE,
LAST_VOICE_ROAMING_BUNDLE_PURCHASE_DATE,
LAST_VOICE_ROAMING_PASS,
LAST_VOICE_ROAMING_PASS_PURCHASE_DATE
FROM EVENT_ACTION_HISTORY;