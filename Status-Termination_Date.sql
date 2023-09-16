select IN_ACCOUNT_NUMBER,max(snapshotdate) as TERMINATION_DATE from (
select * 
from (select IN_SNAP_ACCOUNT_NUMBER as IN_ACCOUNT_NUMBER,LINE_NUMBER_STATUS as STATUS,snapshotdate from (select a.IN_SNAP_ACCOUNT_NUMBER,
CASE 
	when IN_SNAP_STATUS=6 OR IN_SNAP_BLOCKED_STATUS='all activity blocked' then 'DEACTIVE'
	when (IN_FRAUD_ADDITIONAL_INFO = 'CS.ID_Expiry_1W_Blocking' AND IN_FRAUD_STATUS='DEACTIVE' AND IN_SNAP_STATUS ='3') OR (IN_SNAP_NUMBER_STATUS='SUSPENDED') then 'SUSPENDED'
	when IN_FRAUD_ADDITIONAL_INFO is null AND IN_SNAP_NUMBER_STATUS='ACTIVE' then 'ACTIVE'
	else 'DEACTIVE'
END AS LINE_NUMBER_STATUS,snapshotdate
from ( select 
blocked_status as IN_SNAP_BLOCKED_STATUS,
phonenumber AS IN_SNAP_MSISDN,
accountnum AS IN_SNAP_ACCOUNT_NUMBER,
status AS IN_SNAP_STATUS,
CASE 
	when status=3 and blocked_status = 'charged activity blocked' then 'SUSPENDED'
	when status = 3 and blocked_status = 'all activity blocked' then 'DEACTIVE'
	when status = 3 and blocked_status =  'not blocked' then  'ACTIVE'
	when status<>3 then 'DEACTIVE'
END as IN_SNAP_NUMBER_STATUS,snapshotdate
from (
SELECT 
  case when blocked = 0 then 'not blocked' when blocked = '1' then 'all activity blocked' when blocked = '2' then 'charged activity blocked' end as blocked_status,
  accountnum, 
  phonenumber, 
  status, 
  snapshotdate,
  subscriptiontype
  from ae_stg.nobill_snapshot_subscription  
  where  snapshotdate = CURRENT_DATE()-1 and subscriptiontype NOT IN ('VM_AppleWatch','VM_EventSims'))x
  ) a left join ( 
 SELECT 
   accountnum AS IN_SNAP_ACCOUNT_NUMBER,
    additionalinfo  AS IN_FRAUD_ADDITIONAL_INFO,
 historytypekey,
    activity_date_last AS IN_FRAUD_LAST_ACTIVITY_DATE,
    SERVICE_FLAG AS IN_FRAUD_STATUS
 from ( SELECT 
accountnum, 
additionalinfo, 
historytypekey,
activity_date_last,
1 AS EXCLUDE,
CASE WHEN historytypekey = 'accServiceRemoved'  THEN 'ACTIVE' WHEN  historytypekey ='accServiceAdded' THEN 'DEACTIVE' END AS SERVICE_FLAG
FROM  (                    
SELECT *, ROW_NUMBER () OVER (PARTITION BY accountnum   ORDER BY activity_date_last  DESC)  AS  ROW_ FROM (
SELECT accountnum , additionalinfo,historytypekey, max(localtstamp)  as  activity_date_last  from ae_stg.nobill_tx_account_history
WHERE additionalinfo in ('CS.TRA_Blocking' ,'CS.FRAUD_Blocking','CS.Police_CatA_Blocked_Flag','CS.Police_CatB_Blocked_Flag','CS.ID_Expiry_1W_Blocking','CS.ID_Expiry_2W_Blocking','CS.HardSIM_Capping')
GROUP BY accountnum,additionalinfo,historytypekey
) TAB1 ) TAB2 WHERE ROW_ =1 AND historytypekey='accServiceAdded')y
)b on (a.IN_SNAP_ACCOUNT_NUMBER=b.IN_SNAP_ACCOUNT_NUMBER))z
union ALL 
select
			a.accountnum,
			nvl2(
				b.accountnum,
				'PORT_OUT',
				'TERMINATED'
			)status,
			nvl2(
				b.accountnum,
				tstamp,
				snapshotdate
			)snapshotdate
		from
			(
				SELECT
					accountnum,snapshotdate
				from
					ae_stg.nobill_snapshot_subscription
				where
					snapshotdate >= CURRENT_DATE()-91
					and subscriptiontype NOT IN ('VM_AppleWatch','VM_EventSims')
  and accountnum not in (
					SELECT
						accountnum
					from
						ae_stg.nobill_snapshot_subscription
					where
						snapshotdate = CURRENT_DATE()-1
						and subscriptiontype NOT IN ('VM_AppleWatch','VM_EventSims'))
			)a
		left join (
				select
					*
				from
					(
						select
							accountnum ,
							max(tstamp)tstamp
						from
							(
								SELECT
									*,
									CASE
										WHEN historytypekey = 'numDisassociated'
											OR historytypekey = 'numDeleted' THEN 'PORT-OUT'
											WHEN additionalinfo = 'Replace phone number' THEN 'PORT-IN'
											ELSE 'Others'
										END AS DWH_PORT_IN_OR_OUT
									FROM
										ae_stg.nobill_tx_phonenumber_history
									WHERE
										accountnum>1
										and tstamp< cast(
											sysdate as date
										)
							)b
						where
							DWH_PORT_IN_OR_OUT in ('PORT-OUT')
						group by
							accountnum
					)z
			)b on
			(
				a.accountnum = b.accountnum
			))w)ww
			where status in ('TERMINATED','PORT_OUT')
			group by IN_ACCOUNT_NUMBER