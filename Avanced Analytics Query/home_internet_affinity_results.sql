SELECT 
IN_ACCOUNT_NUMBER,
'home_internet_affinity_results' AS MODEL_NAME,
SNAPSHOT_DATE AS LAST_MODEL_REFRESH_DATE,
MBB_affinity_probability AS SCORE,
decile AS DECILE        
FROM(                                       
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY IN_ACCOUNT_NUMBER) AS IN_RANK
FROM(
SELECT 
*,
RANK() OVER (ORDER BY SNAPSHOT_DATE DESC) AS HBB_AFFINITY_RESULTS_DATE_FLAG
FROM neocognix.home_internet_affinity_results
WHERE SNAPSHOT_DATE < date('"+context.vLoadDate+"')
)HBB_AFFINITY_RESULTS_DATE
WHERE HBB_AFFINITY_RESULTS_DATE_FLAG=1 
)UNI_IN WHERE IN_RANK=1;