WITH ACTIVE_PLAN AS 
(
SELECT CustomerId,MAX(Subs_EndDate) AS PLAN_END_DATE, MAX(1) AS FLAG_PLAN FROM 
(
SELECT 
ah.Id AS AH_Id , ah.DateTime AS Action_Timestamp , CAST(ah.DateTime  AS DATE) AS Action_Date,ah.CustomerId,
ah.SubscriptionId Action_SubsId, sub.StatusId AS SubsriptionStatus , CAST(StartDate AS Date) AS Subs_StartDate, CAST(EndDate AS Date) AS Subs_EndDate
FROM uae_db_prod.ae_stg.po_sre_actionhistory ah
LEFT JOIN uae_db_prod.ae_stg.po_sre_subscription sub on sub.Id=ah.SubscriptionId
WHERE CAST(DateTime AS DATE)>= (CURRENT_DATE - 60) AND CAST(DateTime AS DATE)<CURRENT_DATE AND ah.ActionId in (1,3,4,13,14)  AND ah.StatusId=2
AND CAST(EndDate AS Date)>=CURRENT_DATE
)a GROUP BY CustomerId
),
PLAN_PAUSED AS (
SELECT CustomerId ,NobillAccountNo, CAST(DateTime AS DATE) AS PAUSED_DATE 
FROM uae_db_prod.ae_stg.po_sre_actionhistory ah
LEFT JOIN uae_db_prod.ae_stg.po_sre_action aname on aname.ID=ah.ActionId
LEFT JOIN uae_db_prod.ae_stg.po_sre_customer cust on cust.Id=ah.CustomerId
WHERE  CAST(DateTime AS DATE)>= ADD_MONTHS((CURRENT_DATE - 1),-1) AND CAST(DateTime AS DATE)<CURRENT_DATE AND aname.Name Like '%Pause%' --AND ah.StatusId=2

)
SELECT IN_ACCOUNT_NUMBER,PAUSE_FLAG FROM(
SELECT NobillAccountNo AS IN_ACCOUNT_NUMBER , MAX(1) AS PAUSE_FLAG , MAX(PAUSED_DATE) AS PAUSED_DATE
FROM
(
SELECT NobillAccountNo,a.CustomerId,PAUSED_DATE ,FLAG_PLAN , PLAN_END_DATE, CASE WHEN FLAG_PLAN=1 THEN 0 ELSE 1 END AS FLAG_PAUSED 
FROM PLAN_PAUSED a
LEFT JOIN ACTIVE_PLAN b ON a.CustomerId=b.CustomerId
)a WHERE FLAG_PAUSED=1
GROUP BY NobillAccountNo
)last;