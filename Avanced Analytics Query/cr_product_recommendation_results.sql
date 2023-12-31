SELECT
IN_ACCOUNT_NUMBER,
'product_recommendation_results' AS MODEL_NAME,
date_dt AS LAST_MODEL_REFRESH_DATE,
Predicted_data AS AD_ATTRIBUTE_31,
Predicted_natl AS AD_ATTRIBUTE_32,
Predicted_intl AS AD_ATTRIBUTE_33,
DATA_BOOSTER_RECOMMENDED_PRICE AS AD_ATTRIBUTE_34,
NATL_BOOSTER_RECOMMENDED_PRICE AS AD_ATTRIBUTE_35,
INTL_BOOSTER_RECOMMENDED_PRICE AS AD_ATTRIBUTE_36,
DATA_BOOSTER_RECOMMENDED AS AD_ATTRIBUTE_11,
NATL_BOOSTER_RECOMMENDED AS AD_ATTRIBUTE_12,
INTL_BOOSTER_RECOMMENDED AS AD_ATTRIBUTE_13
FROM (
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY IN_ACCOUNT_NUMBER) AS IN_RANK
FROM(
SELECT 
IN_ACCOUNT_NUMBER,
date_dt ,
TRUNC(Predicted_data,2) AS Predicted_data,
Predicted_natl ,
Predicted_intl,
DATA_BOOSTER_RECOMMENDED,
NATL_BOOSTER_RECOMMENDED,
INTL_BOOSTER_RECOMMENDED,
DATA_BOOSTER_RECOMMENDED_PRICE,
NATL_BOOSTER_RECOMMENDED_PRICE,
INTL_BOOSTER_RECOMMENDED_PRICE,
RANK() OVER (ORDER BY date_dt DESC) AS BOOSTER_LAST_DATE_FLAG
FROM neocognix.cr_product_recommendation_results
WHERE date_dt < date('"+context.vLoadDate+"')
)BOOSTER_LAST_DATE 
WHERE BOOSTER_LAST_DATE_FLAG=1
)UNI_IN WHERE IN_RANK=1;