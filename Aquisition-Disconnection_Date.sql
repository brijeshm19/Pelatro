select 
accountnum as IN_ACCOUNT_NUMBER,
snapshotdate DISCONNECTION_DATE
from (
select * from (
select 
*,
row_number()over(partition by accountnum order by snapshotdate desc) rnk
from (
select 
*
from (
select 
*,
row_number()over(partition by accountnum order by snapshotdate) first_appear,
row_number()over(partition by accountnum order by snapshotdate desc) last_appear
from (
select accountnum,	
COALESCE (lag(LINE_NUMBER_STATUS)over(partition by accountnum order by snapshotdate),'NA')prev_status,
LINE_NUMBER_STATUS,
COALESCE (lag(snapshotdate)over(partition by accountnum order by snapshotdate),'2017-01-01')prev_status_date,
snapshotdate,additionalinfo,add_date,remove_date,blocked,status
from 
(select x.accountnum,x.snapshotdate,
CASE 
	when status=6 or blocked=1 then 'DEACTIVE'
	when (additionalinfo='CS.ID_Expiry_1W_Blocking' and add_date is not null) or (status=3 and blocked=2) then 'SUSPENDED'
	when additionalinfo is null and  status=3 and blocked=0 then 'ACTIVE'
	else 'DEACTIVE'
END as LINE_NUMBER_STATUS,
phonenumber,additionalinfo,add_date,remove_date,blocked,status
from 
(SELECT 
  case when blocked = 0 then 'not blocked' when blocked = '1' then 'all activity blocked' when blocked = '2' then 'charged activity blocked' end as blocked_status,
  accountnum, 
  phonenumber, 
  status, 
  snapshotdate,
  subscriptiontype,
  blocked
  from ae_stg.nobill_snapshot_subscription  
  where subscriptiontype NOT IN ('VM_AppleWatch','VM_EventSims'))x
left join 
(select accountnum	,add_date	,remove_date	,additionalinfo from (select 
*,
row_number() over (partition by accountnum,cast(add_date as date) order by add_date desc,remove_date desc) rnk
from (select 
x.accountnum,
x.add_time add_date,
COALESCE (y.remove_time,sysdate)remove_date,
add_service additionalinfo,add_number,remove_number
from (select *,row_number()over(partition by accountnum,add_service order by add_time)add_number from (SELECT accountnum ,additionalinfo add_service,tstamp add_time
from ae_stg.nobill_tx_account_history
WHERE additionalinfo in ('CS.TRA_Blocking' ,'CS.FRAUD_Blocking','CS.Police_CatA_Blocked_Flag','CS.Police_CatB_Blocked_Flag','CS.ID_Expiry_1W_Blocking','CS.ID_Expiry_2W_Blocking','CS.HardSIM_Capping')
and historytypekey='accServiceAdded'
order by tstamp) a)x left join (select *,row_number()over(partition by accountnum,remove_service order by remove_time)remove_number from (SELECT accountnum ,additionalinfo remove_service,tstamp remove_time
from ae_stg.nobill_tx_account_history
WHERE additionalinfo in ('CS.TRA_Blocking' ,'CS.FRAUD_Blocking','CS.Police_CatA_Blocked_Flag','CS.Police_CatB_Blocked_Flag','CS.ID_Expiry_1W_Blocking','CS.ID_Expiry_2W_Blocking','CS.HardSIM_Capping')
and historytypekey='accServiceRemoved'
order by tstamp) a)y on (x.accountnum=y.accountnum and x.add_service=y.remove_service and x.add_number=y.remove_number))w) u) y
on (x.accountnum =y.accountnum and x.snapshotdate+1 >= y.add_date and x.snapshotdate+1 <y.remove_date)
order by x.snapshotdate
)z
)w 
) u
where LINE_NUMBER_STATUS<>prev_status
and prev_status='ACTIVE' and line_number_status in ('SUSPENDED','DEACTIVE')
order by snapshotdate desc
)ww
)x where rnk = 1
)www
;