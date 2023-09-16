select 
account_number,
sum(incoming_count) as incoming_count,
sum(outgoing_count) as outgoing_count,
sum(case when incoming_count > 0 then outgoing_count/incoming_count else 0 end) as INCOMING_OUTGOING_CALLS_PERC
from(
SELECT
fus_accountnum as account_number,
sum(case when fus_call_direction_id = 1 then 1 else 0 end) as incoming_count,
sum(case when fus_call_direction_id = 2 then 1 else 0 end) as outgoing_count
FROM
ae_prod.nobill_fact_usage t1
JOIN ae_prod.nobill_dim_termzone t2 on t1.fus_termzone_id = t2.dtz_termzone_id
JOIN ae_prod.nobill_dim_origzone t3 on t1.fus_origzone_id = t3.doz_origzone_id
LEFT JOIN
(SELECT fus_terminationreason,  fus_subterminationreason_description
FROM ae_prod.nobill_ref_terminationreason WHERE fus_service_type_id = 1) ter 
ON t1.fus_terminationreason = ter.fus_terminationreason
WHERE
fus_time_stamp::date <= date('"+context.vLoadDate+"')
AND fus_time_stamp::date >= date('"+context.vLoadDate+"')-30
AND fus_service_type_id =1
group by 1)t
group by 1;	