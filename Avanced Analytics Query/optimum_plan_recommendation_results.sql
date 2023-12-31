SELECT 
IN_ACCOUNT_NUMBER,
'optimum_plan_recommendation_results' AS MODEL_NAME,
date_dt AS LAST_MODEL_REFRESH_DATE,
RIGHT_SIZE_PLAN_NAME AS AD_ATTRIBUTE_11,
CASE WHEN RIGHT_SIZE_PLAN_CHANGE_VALUE_LESS_THAN_SPENT_CHANGE_FLAG=1 THEN TRUNC(ABS(RIGHT_SIZE_PLAN_PRICE_CHANGE)) || ' Less'  
WHEN RIGHT_SIZE_PLAN_CHANGE_VALUE_LESS_THAN_SPENT_CHANGE_FLAG=0 THEN TRUNC(ABS(RIGHT_SIZE_PLAN_PRICE_VS_SPENT_CHANGE),0) || ' Less'  
END AS AD_ATTRIBUTE_12,
RIGHT_SIZE_CURRENT_PLAN_PRICE AS AD_ATTRIBUTE_31,
RIGHT_SIZE_PLAN_PRICE AS AD_ATTRIBUTE_32,
RIGHT_SIZE_AVG_MONEY_SPENT_60D AS AD_ATTRIBUTE_33,
RIGHT_SIZE_PLAN_PRICE_CHANGE AS AD_ATTRIBUTE_34,
RIGHT_SIZE_PLAN_PRICE_VS_SPENT_CHANGE AS AD_ATTRIBUTE_35,
RIGHT_SIZE_PLAN_CHANGE_VALUE_LESS_THAN_SPENT_CHANGE_FLAG AS AD_ATTRIBUTE_36
FROM(
SELECT 
*,
CASE WHEN RIGHT_SIZE_PLAN_PRICE_VS_SPENT_CHANGE IS NULL OR RIGHT_SIZE_PLAN_PRICE_CHANGE IS NULL THEN 2 
WHEN RIGHT_SIZE_PLAN_PRICE_VS_SPENT_CHANGE>=RIGHT_SIZE_PLAN_PRICE_CHANGE THEN 1 ELSE 0 
END AS RIGHT_SIZE_PLAN_CHANGE_VALUE_LESS_THAN_SPENT_CHANGE_FLAG
FROM(
SELECT 
in_account_number AS IN_ACCOUNT_NUMBER,
date_dt ,
right_size_plan_name AS RIGHT_SIZE_PLAN_NAME,
current_plan_net_cost AS RIGHT_SIZE_CURRENT_PLAN_PRICE,
right_size_plan_net_cost AS RIGHT_SIZE_PLAN_PRICE, 
\"60day_avg_money_spent\" AS RIGHT_SIZE_AVG_MONEY_SPENT_60D,
right_size_plan_price_change AS RIGHT_SIZE_PLAN_PRICE_CHANGE, 
(right_size_plan_net_cost - \"60day_avg_money_spent\") AS RIGHT_SIZE_PLAN_PRICE_VS_SPENT_CHANGE
FROM(
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY in_account_number) AS IN_RANK
FROM(
SELECT 
*,
RANK() OVER (ORDER BY date_dt DESC) AS RIGHT_SIZE_LAST_DATE_FLAG
FROM neocognix.cr_optimum_plan_recommendation_results
WHERE date_dt < date('"+context.vLoadDate+"')
)RIGHT_SIZE_LAST_DATE
WHERE RIGHT_SIZE_LAST_DATE_FLAG=1 
)UNI_IN WHERE IN_RANK=1
)A
)B;