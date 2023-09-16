select 
IN_ACCOUNT_NUMBER,
CUSTOMER_STATUS,
LAST_ACTIVITY_DATE,
FLAG_PORT_OUT,
MAX_ACCOUNTS_LIMIT_FLAG,
CUSTOMER_SENTIMENT_SCORE,
CUSTOMER_MOOD_SCORE_CSAT
from (select 
CRM_UID as CUSTOMER_ID,
x.IN_ACCOUNT_NUMBER ,
x.STATUS as CUSTOMER_STATUS,
a.REGISTRATION_ID ,
a.EMAIL ,
a.MSISDN ,
a.CUST_NATIONALITY_NAME as NATIONALITY,
a.CUST_FIRST_NAME ||' '||a.CUST_LAST_NAME  as CUSTOMER_NAME,
case when CUST_LATEST_ID_TYPE='Emirates id' then 'Resident' when CUST_LATEST_ID_TYPE in ('Passport','GCC id') then 'Tourist' else 'NA' end  as ACTIVATION_ID_TYPE,
nvl2(b.IN_ACCOUNT_NUMBER,'MNP','Standard') as ACTIVATION_TYPE,
FULL_ACTIVATION_DATE as ACTIVATION_DATE,
CASE 
				when MONTH(CUST_DOB::DATE)=MONTH((sysdate-1)::DATE) and DAY(CUST_DOB::DATE)=DAY((sysdate-1)::DATE) and WEEK (CUST_DOB::DATE)=WEEK((sysdate-1)::DATE) then 'Today'
				when MONTH(CUST_DOB::DATE)=MONTH((sysdate-1)::DATE) and WEEK (CUST_DOB::DATE)=WEEK((sysdate-1)::DATE) then 'Current Week'
				when MONTH(CUST_DOB::DATE)=MONTH((sysdate-1)::DATE) and WEEK (CUST_DOB::DATE)=WEEK((sysdate-1)::DATE)+1 then 'Upcoming Week'
				when MONTH(CUST_DOB::DATE)=MONTH((sysdate-1)::DATE) and WEEK (CUST_DOB::DATE)!=WEEK((sysdate-1)::DATE) then 'Current Month'
				when MONTH(CUST_DOB::DATE)=MONTH((sysdate-1)::DATE)+1 then 'Next Month'
				else 'Later'
			END as CUSTOMER_DOB,
a.CUST_LAST_PRODUCT_NAME as USER_TYPE,
DATEDIFF(day,cast(a.FULL_ACTIVATION_DATE as date), sysdate-1)as DAYS_SINCE_REGISTRATION,
d.DAYS_SINCE_PORTOUT,
nvl2(d.DAYS_SINCE_PORTOUT,1,0) as FLAG_PORT_OUT,
CUST_DAYS_SINCE_LAST_ACTIVITY  as LAST_ACTIVITY_DATE,
case when AGE_IN_YEARS(
				sysdate-1,
				a.CUST_DOB::timestamp
			) <=24 then 1 else 0 end as YOUTH_FLAG,
case when FLAG_POD_POS_COD not in ('POS','POD','COD') then 'Others' else FLAG_POD_POS_COD end as ACTIVATION_PAYMENT_CHANNEL ,
SALES_CHANNEL as ACTIVATION_CHANNEL,
a.CUST_REGISTRATION_CHANNEL  as EMAIL_REGISTRATION_CHANNEL ,
DATEDIFF(day, cast(a.CUST_REGISTRATION_DATE as date), sysdate-1)as EMAIL_REGISTRATION_DATE,
e.MAX_ACCOUNTS_LIMIT_FLAG,
f.score as CUSTOMER_SENTIMENT_SCORE,
f.score as CUSTOMER_MOOD_SCORE_CSAT,
case when MONTH(CUST_DOB::DATE)=MONTH((sysdate-1)::DATE) and DAY(CUST_DOB::DATE)=DAY((sysdate-1)::DATE) then 1 else 0 end as NETWORK_ANNIVERSARY_DAY,
case when c.IsEmailVerified=true then 1 else 0 end as EMAIL_VERIFIED_FLAG
from (select * from (select IN_SNAP_ACCOUNT_NUMBER as IN_ACCOUNT_NUMBER,LINE_NUMBER_STATUS as STATUS from (select a.IN_SNAP_ACCOUNT_NUMBER,
CASE 
	when IN_SNAP_STATUS=6 OR IN_SNAP_BLOCKED_STATUS='all activity blocked' then 'DEACTIVE'
	when (IN_FRAUD_ADDITIONAL_INFO = 'CS.ID_Expiry_1W_Blocking' AND IN_FRAUD_STATUS='DEACTIVE' AND IN_SNAP_STATUS ='3') OR (IN_SNAP_NUMBER_STATUS='SUSPENDED') then 'SUSPENDED'
	when IN_FRAUD_ADDITIONAL_INFO is null AND IN_SNAP_NUMBER_STATUS='ACTIVE' then 'ACTIVE'
	else 'DEACTIVE'
END AS LINE_NUMBER_STATUS
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
END as IN_SNAP_NUMBER_STATUS
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
			)status
		from
			(
				SELECT
					accountnum
				from
					ae_stg.nobill_snapshot_subscription
				where
					snapshotdate >= CURRENT_DATE()-91
					and subscriptiontype NOT IN ('VM_AppleWatch','VM_EventSims')
  MINUS
					SELECT
						accountnum
					from
						ae_stg.nobill_snapshot_subscription
					where
						snapshotdate = CURRENT_DATE()-1
						and subscriptiontype NOT IN ('VM_AppleWatch','VM_EventSims')
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
			))w)x
left join ae_prod.customer_master a on (x.IN_ACCOUNT_NUMBER=a.IN_ACCOUNT_NUMBER)
left join (select 
accountnum as IN_ACCOUNT_NUMBER
from(select distinct accountnum 	from (
SELECT *, CASE WHEN historytypekey = 'numDisassociated'
OR historytypekey = 'numDeleted' THEN 'PORT-OUT'
WHEN additionalinfo = 'Replace phone number' THEN 'PORT-IN'
ELSE 'Others'
END AS DWH_PORT_IN_OR_OUT
FROM ae_stg.nobill_tx_phonenumber_history
WHERE accountnum>1
and tstamp< cast(sysdate as date))b
where DWH_PORT_IN_OR_OUT in ('PORT-IN'))a) b on (x.IN_ACCOUNT_NUMBER=b.IN_ACCOUNT_NUMBER)
left join ae_stg.vmwsc_registration c on(a.REGISTRATION_ID=c.ID)
left join (select 
accountnum as IN_ACCOUNT_NUMBER, 
DATEDIFF(day, tstamp , sysdate) as DAYS_SINCE_PORTOUT
from(select accountnum ,max(tstamp)tstamp	from (
SELECT *, CASE WHEN historytypekey = 'numDisassociated'
OR historytypekey = 'numDeleted' THEN 'PORT-OUT'
WHEN additionalinfo = 'Replace phone number' THEN 'PORT-IN'
ELSE 'Others'
END AS DWH_PORT_IN_OR_OUT
FROM ae_stg.nobill_tx_phonenumber_history
WHERE accountnum>1
and tstamp< cast(sysdate as date))b
where DWH_PORT_IN_OR_OUT in ('PORT-OUT')
group by accountnum)a)d on (x.IN_ACCOUNT_NUMBER=d.IN_ACCOUNT_NUMBER)
left join (select CUST_LATEST_ID_NUMBER,case when TOTAL_ACCOUNTS>=5 then 1 else 0 end as MAX_ACCOUNTS_LIMIT_FLAG from (
select CUST_LATEST_ID_NUMBER,count(*) as TOTAL_ACCOUNTS from ae_prod.customer_master 
where LATEST_LINE_STATUS='ACTIVE'
group by CUST_LATEST_ID_NUMBER) y)e
on (a.CUST_LATEST_ID_NUMBER=e.CUST_LATEST_ID_NUMBER)
left join (SELECT
score,
regexp_replace(extra_info , '[^0-9]', '') AS UID
FROM
api.usabilla_data a
left join 
api.usabilla_campaigns b 
on b.campaign_id =a.campaign_id
where LENGTH(regexp_replace(extra_info , '[^0-9]', ''))>1) f on (x.IN_ACCOUNT_NUMBER=f.UID)
)y