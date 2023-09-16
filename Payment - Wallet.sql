SELECT
fad_accountnum ,
sum(case when fad_time_stamp>=( (EXTRACT(MONTH FROM sysdate-1) || '/1/' || EXTRACT(YEAR FROM sysdate-1))::date ) then 1 else 0 end) WALLET_RECHARGE_COUNT_M_0,
sum(case when fad_time_stamp>= ADD_MONTHS((EXTRACT(MONTH FROM sysdate-1) || '/1/' || EXTRACT(YEAR FROM sysdate-1))::date,-1)
and fad_time_stamp<ADD_MONTHS((EXTRACT(MONTH FROM sysdate-1) || '/1/' || EXTRACT(YEAR FROM sysdate-1))::date,0)
then 1 else 0 end) WALLET_RECHARGE_COUNT_M_1,
sum(case when fad_time_stamp>=cast(sysdate-31 as date) then 1 else 0 end)WALLET_RECHARGE_COUNT_last_30_days,
sum(case when fad_time_stamp>=cast(sysdate-61 as date) and fad_time_stamp<cast(sysdate-31 as date) then 1 else 0 end)_last_31_60_days,
sum(case when fad_time_stamp>=cast(sysdate-91 as date) and fad_time_stamp<cast(sysdate-61 as date) then 1 else 0 end)_last_61_90_days
FROM ae_prod.nobill_fact_adjustment adjustment
join ae_prod.nobill_dim_adjustment_type adj_lookup on adjustment.fad_adjustment_type_id = adj_lookup.dad_adjustment_type_id
where dad_adjustmenttypename = 'voucher' 
and fad_time_stamp >=  cast(sysdate-91 as date)
and fad_time_stamp <cast(sysdate as date)
group by fad_accountnum ; 
