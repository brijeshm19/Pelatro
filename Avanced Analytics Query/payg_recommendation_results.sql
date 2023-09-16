SELECT
IN_ACCOUNT_NUMBER,
'payg_recommendation_results' AS MODEL_NAME,
SNAPSHOT_DATE AS LAST_MODEL_REFRESH_DATE,
CLASS AS AD_ATTRIBUTE_11,
PROBA AS AD_ATTRIBUTE_31
FROM(
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY IN_ACCOUNT_NUMBER) AS IN_RANK
FROM(
SELECT 
*,
RANK() OVER (ORDER BY SNAPSHOT_DATE DESC) AS PAYG_LAST_DATE_FLAG
FROM neocognix.payg_recommendation_results
WHERE SNAPSHOT_DATE < date('"+context.vLoadDate+"')
)PAYG_LAST_DATE
WHERE PAYG_LAST_DATE_FLAG=1
)UNI_IN WHERE IN_RANK=1;