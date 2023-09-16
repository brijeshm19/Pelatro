SELECT 
in_account_number AS IN_ACCOUNT_NUMBER,
'happiness_index_results' AS MODEL_NAME,
snapshot_date AS LAST_MODEL_REFRESH_DATE,
chi_score AS SCORE,
decile AS DECILE        
FROM(                                       
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY in_account_number) AS IN_RANK
FROM(
SELECT 
*,
RANK() OVER (ORDER BY snapshot_date DESC) AS HAPPY_INDEX_DATE_FLAG
FROM neocognix.happiness_index_results
WHERE snapshot_date < date('"+context.vLoadDate+"')
)HAPPY_INDEX_DATE
WHERE HAPPY_INDEX_DATE_FLAG=1 
)UNI_IN WHERE IN_RANK=1;